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
    format_cnpj,
    format_cpf,
    normalize_name,
    strip_document,
)


class SanctionsPipeline(Pipeline):
    """ETL pipeline for CEIS/CNEP sanctions data."""

    name = "sanctions"
    source_id = "ceis_cnep"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size)
        self._raw_ceis: pd.DataFrame = pd.DataFrame()
        self._raw_cnep: pd.DataFrame = pd.DataFrame()
        self.sanctions: list[dict[str, Any]] = []
        self.sanctioned_entities: list[dict[str, Any]] = []

    def extract(self) -> None:
        sanctions_dir = Path(self.data_dir) / "sanctions"
        self._raw_ceis = pd.read_csv(
            sanctions_dir / "ceis.csv",
            dtype=str,
            encoding="latin-1",
            keep_default_na=False,
        )
        self._raw_cnep = pd.read_csv(
            sanctions_dir / "cnep.csv",
            dtype=str,
            encoding="latin-1",
            keep_default_na=False,
        )

    def _parse_date(self, value: str) -> str:
        """Parse a date string, returning ISO format or empty string."""
        value = value.strip()
        if not value:
            return ""
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return str(pd.to_datetime(value, format=fmt).strftime("%Y-%m-%d"))
            except ValueError:
                continue
        return value

    def _process_rows(
        self, df: pd.DataFrame, sanction_type: str
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        sanctions: list[dict[str, Any]] = []
        entities: list[dict[str, Any]] = []

        for idx, row in df.iterrows():
            doc_raw = str(row["cpf_cnpj"])
            digits = strip_document(doc_raw)
            nome = normalize_name(str(row["nome"]))
            is_company = len(digits) == 14

            if is_company:
                doc_formatted = format_cnpj(doc_raw)
            elif len(digits) == 11:
                doc_formatted = format_cpf(doc_raw)
            else:
                doc_formatted = digits

            sanction_id = f"{sanction_type}_{digits}_{idx}"
            date_start = self._parse_date(str(row["data_inicio"]))
            date_end = self._parse_date(str(row["data_fim"]))

            sanctions.append({
                "sanction_id": sanction_id,
                "type": sanction_type,
                "date_start": date_start,
                "date_end": date_end,
                "reason": str(row["motivo"]).strip(),
                "source": sanction_type,
            })

            entity_label = "Company" if is_company else "Person"
            entity_key_field = "cnpj" if is_company else "cpf"

            entities.append({
                "source_key": doc_formatted,
                "target_key": sanction_id,
                "entity_label": entity_label,
                "entity_key_field": entity_key_field,
                "entity_name": nome,
                "entity_doc": doc_formatted,
            })

        return sanctions, entities

    def transform(self) -> None:
        ceis_sanctions, ceis_entities = self._process_rows(self._raw_ceis, "CEIS")
        cnep_sanctions, cnep_entities = self._process_rows(self._raw_cnep, "CNEP")

        all_sanctions = ceis_sanctions + cnep_sanctions
        all_entities = ceis_entities + cnep_entities

        self.sanctions = deduplicate_rows(all_sanctions, ["sanction_id"])
        self.sanctioned_entities = all_entities

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.sanctions:
            loader.load_nodes("Sanction", self.sanctions, key_field="sanction_id")

        for ent in self.sanctioned_entities:
            label = ent["entity_label"]
            key_field = ent["entity_key_field"]
            doc = ent["entity_doc"]
            name = ent["entity_name"]

            node_row: dict[str, Any] = {key_field: doc, "name": name}
            if label == "Company":
                node_row["razao_social"] = name
            loader.load_nodes(label, [node_row], key_field=key_field)

        if self.sanctioned_entities:
            rel_rows = [
                {"source_key": e["source_key"], "target_key": e["target_key"]}
                for e in self.sanctioned_entities
            ]

            query = (
                "UNWIND $rows AS row "
                "MATCH (s:Sanction {sanction_id: row.target_key}) "
                "OPTIONAL MATCH (c:Company {cnpj: row.source_key}) "
                "OPTIONAL MATCH (p:Person {cpf: row.source_key}) "
                "WITH s, coalesce(c, p) AS entity "
                "WHERE entity IS NOT NULL "
                "MERGE (entity)-[:SANCIONADA]->(s)"
            )
            loader.run_query(query, rel_rows)
