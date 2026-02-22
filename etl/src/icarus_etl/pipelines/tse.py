from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from icarus_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from icarus_etl.loader import Neo4jBatchLoader
from icarus_etl.transforms import (
    deduplicate_rows,
    format_cpf,
    normalize_name,
    strip_document,
)

# TSE 2024 masks ALL candidate CPFs as "-4". After strip_document → "4",
# format_cpf → "4" — every candidate MERGEs into one ghost node.
# We use SQ_CANDIDATO (unique sequential ID per candidate per election) instead.
_MASKED_CPF_SENTINEL = "-4"


class TSEPipeline(Pipeline):
    """Electoral data pipeline — candidates and campaign donations."""

    name = "tse"
    source_id = "tribunal_superior_eleitoral"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size)
        self.candidates: list[dict[str, Any]] = []
        self.donations: list[dict[str, Any]] = []
        self.elections: list[dict[str, Any]] = []

    def extract(self) -> None:
        tse_dir = Path(self.data_dir) / "tse"
        self._raw_candidatos = pd.read_csv(
            tse_dir / "candidatos.csv", encoding="latin-1", dtype=str,
            nrows=self.limit,
        )
        self._raw_doacoes = pd.read_csv(
            tse_dir / "doacoes.csv", encoding="latin-1", dtype=str,
            nrows=self.limit,
        )

    def transform(self) -> None:
        self._transform_candidates()
        self._transform_donations()

    def _transform_candidates(self) -> None:
        candidates: list[dict[str, Any]] = []
        elections: list[dict[str, Any]] = []

        for _, row in self._raw_candidatos.iterrows():
            sq = str(row["sq_candidato"]).strip()
            raw_cpf = str(row["cpf"]).strip()
            name = normalize_name(str(row["nome"]))
            ano = int(row["ano"])
            cargo = normalize_name(str(row["cargo"]))
            uf = str(row["uf"]).strip().upper()
            municipio = normalize_name(str(row.get("municipio", "")))
            partido = str(row.get("partido", "")).strip().upper()

            # Only store CPF if it's a real value (not the TSE "-4" mask)
            cpf = None
            if raw_cpf != _MASKED_CPF_SENTINEL:
                cpf = format_cpf(strip_document(raw_cpf))

            candidate: dict[str, Any] = {
                "sq_candidato": sq,
                "name": name,
                "partido": partido,
            }
            if cpf:
                candidate["cpf"] = cpf

            candidates.append(candidate)
            elections.append({
                "year": ano,
                "cargo": cargo,
                "uf": uf,
                "municipio": municipio,
                "candidate_sq": sq,
            })

        self.candidates = deduplicate_rows(candidates, ["sq_candidato"])
        self.elections = deduplicate_rows(
            elections, ["year", "cargo", "uf", "municipio", "candidate_sq"]
        )

    def _transform_donations(self) -> None:
        donations: list[dict[str, Any]] = []

        for _, row in self._raw_doacoes.iterrows():
            candidate_sq = str(row["sq_candidato"]).strip()
            donor_doc = strip_document(str(row["cpf_cnpj_doador"]))
            donor_name = normalize_name(str(row["nome_doador"]))
            valor = float(str(row["valor"]).replace(",", "."))
            ano = int(row["ano"])

            is_company = len(donor_doc) == 14
            donor_doc_fmt = donor_doc  # keep raw for CNPJ matching
            if not is_company:
                donor_doc_fmt = format_cpf(donor_doc)

            donations.append({
                "candidate_sq": candidate_sq,
                "donor_doc": donor_doc_fmt,
                "donor_name": donor_name,
                "donor_is_company": is_company,
                "valor": valor,
                "year": ano,
            })

        self.donations = donations

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        # Person nodes for candidates (keyed by sq_candidato)
        loader.load_nodes("Person", self.candidates, key_field="sq_candidato")

        # Election nodes
        election_nodes = deduplicate_rows(
            [
                {"year": e["year"], "cargo": e["cargo"], "uf": e["uf"], "municipio": e["municipio"]}
                for e in self.elections
            ],
            ["year", "cargo", "uf", "municipio"],
        )
        if election_nodes:
            loader.run_query(
                "UNWIND $rows AS row "
                "MERGE (e:Election {year: row.year, cargo: row.cargo, "
                "uf: row.uf, municipio: row.municipio})",
                election_nodes,
            )

        # CANDIDATO_EM relationships (via sq_candidato)
        candidato_rels = [
            {
                "source_key": e["candidate_sq"],
                "target_year": e["year"],
                "target_cargo": e["cargo"],
                "target_uf": e["uf"],
                "target_municipio": e["municipio"],
            }
            for e in self.elections
        ]
        if candidato_rels:
            loader.run_query(
                "UNWIND $rows AS row "
                "MATCH (p:Person {sq_candidato: row.source_key}) "
                "MATCH (e:Election {year: row.target_year, cargo: row.target_cargo, "
                "uf: row.target_uf, municipio: row.target_municipio}) "
                "MERGE (p)-[:CANDIDATO_EM]->(e)",
                candidato_rels,
            )

        # Donor nodes and DOOU relationships
        person_donors = [
            {"cpf": d["donor_doc"], "name": d["donor_name"]}
            for d in self.donations
            if not d["donor_is_company"]
        ]
        company_donors = [
            {"cnpj": d["donor_doc"], "name": d["donor_name"], "razao_social": d["donor_name"]}
            for d in self.donations
            if d["donor_is_company"]
        ]

        if person_donors:
            loader.load_nodes("Person", deduplicate_rows(person_donors, ["cpf"]), key_field="cpf")
        if company_donors:
            loader.load_nodes(
                "Company", deduplicate_rows(company_donors, ["cnpj"]), key_field="cnpj"
            )

        # DOOU from Person donors → candidate (via sq_candidato)
        person_donation_rels = [
            {
                "source_key": d["donor_doc"],
                "target_key": d["candidate_sq"],
                "valor": d["valor"],
                "year": d["year"],
            }
            for d in self.donations
            if not d["donor_is_company"]
        ]
        if person_donation_rels:
            loader.run_query(
                "UNWIND $rows AS row "
                "MATCH (d:Person {cpf: row.source_key}) "
                "MATCH (c:Person {sq_candidato: row.target_key}) "
                "MERGE (d)-[r:DOOU]->(c) "
                "SET r.valor = row.valor, r.year = row.year",
                person_donation_rels,
            )

        # DOOU from Company donors → candidate (via sq_candidato)
        company_donation_rels = [
            {
                "source_key": d["donor_doc"],
                "target_key": d["candidate_sq"],
                "valor": d["valor"],
                "year": d["year"],
            }
            for d in self.donations
            if d["donor_is_company"]
        ]
        if company_donation_rels:
            loader.run_query(
                "UNWIND $rows AS row "
                "MATCH (d:Company {cnpj: row.source_key}) "
                "MATCH (c:Person {sq_candidato: row.target_key}) "
                "MERGE (d)-[r:DOOU]->(c) "
                "SET r.valor = row.valor, r.year = row.year",
                company_donation_rels,
            )
