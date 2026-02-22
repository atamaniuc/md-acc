from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from icarus_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from icarus_etl.loader import Neo4jBatchLoader
from icarus_etl.transforms import deduplicate_rows, format_cnpj, normalize_name

logger = logging.getLogger(__name__)

# Receita Federal CSV column names (files have no headers)
EMPRESAS_COLS = [
    "cnpj_basico",
    "razao_social",
    "natureza_juridica",
    "qualificacao_responsavel",
    "capital_social",
    "porte_empresa",
    "ente_federativo",
]

SOCIOS_COLS = [
    "cnpj_basico",
    "identificador_socio",
    "nome_socio",
    "cpf_cnpj_socio",
    "qualificacao_socio",
    "data_entrada",
    "pais",
    "representante_legal",
    "nome_representante",
    "qualificacao_representante",
    "faixa_etaria",
]

ESTABELECIMENTOS_COLS = [
    "cnpj_basico",
    "cnpj_ordem",
    "cnpj_dv",
    "identificador_matriz_filial",
    "nome_fantasia",
    "situacao_cadastral",
    "data_situacao_cadastral",
    "motivo_situacao_cadastral",
    "nome_cidade_exterior",
    "pais",
    "data_inicio_atividade",
    "cnae_principal",
    "cnae_secundaria",
    "tipo_logradouro",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cep",
    "uf",
    "municipio",
    "ddd1",
    "telefone1",
    "ddd2",
    "telefone2",
    "ddd_fax",
    "fax",
    "email",
    "situacao_especial",
    "data_situacao_especial",
]

# Reference tables: 2-column CSVs (codigo, descricao)
REFERENCE_TABLES = [
    "Naturezas",
    "Qualificacoes",
    "Cnaes",
    "Municipios",
    "Paises",
    "Motivos",
]

# Base dos Dados (BigQuery) -> Receita Federal column name mapping.
# BQ renames many RF columns. These maps translate BQ names back to RF names
# so the transform step can use a single code path.
BQ_EMPRESAS_RENAME = {
    "porte": "porte_empresa",
}
BQ_EMPRESAS_DROP = {"ano", "mes", "data"}

BQ_SOCIOS_RENAME = {
    "tipo": "identificador_socio",
    "nome": "nome_socio",
    "documento": "cpf_cnpj_socio",
    "qualificacao": "qualificacao_socio",
    "data_entrada_sociedade": "data_entrada",
    "id_pais": "pais",
    "cpf_representante_legal": "representante_legal",
    "nome_representante_legal": "nome_representante",
    "qualificacao_representante_legal": "qualificacao_representante",
}
BQ_SOCIOS_DROP = {"ano", "mes", "data"}

BQ_ESTABELECIMENTOS_RENAME = {
    "id_pais": "pais",
    "cnae_fiscal_principal": "cnae_principal",
    "cnae_fiscal_secundaria": "cnae_secundaria",
    "sigla_uf": "uf",
    "id_municipio": "municipio",
    "ddd_1": "ddd1",
    "telefone_1": "telefone1",
    "ddd_2": "ddd2",
    "telefone_2": "telefone2",
}
BQ_ESTABELECIMENTOS_DROP = {"ano", "mes", "data", "cnpj", "id_municipio_rf"}


class _BQChunkAdapter:
    """Wraps a pandas TextFileReader to rename/drop columns on each chunk.

    Makes BQ-format CSVs yield chunks with RF-compatible column names,
    so the same transform methods work for both formats.
    """

    def __init__(
        self,
        reader: pd.io.parsers.readers.TextFileReader,
        rename_map: dict[str, str],
        drop_cols: set[str],
    ) -> None:
        self._reader = reader
        self._rename_map = rename_map
        self._drop_cols = drop_cols

    def __iter__(self) -> _BQChunkAdapter:
        return self

    def __next__(self) -> pd.DataFrame:
        chunk = next(self._reader)
        chunk = chunk.drop(columns=[c for c in self._drop_cols if c in chunk.columns])
        return chunk.rename(columns=self._rename_map)


def parse_capital_social(value: str) -> float:
    """Parse Receita Federal capital_social format.

    RF uses comma as decimal separator: '750000000,00' -> 750000000.00
    Simple format uses plain numbers: '7500000000' -> 7500000000.0
    """
    if not value or value.strip() == "":
        return 0.0
    cleaned = value.strip().replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


class CNPJPipeline(Pipeline):
    """ETL pipeline for Receita Federal CNPJ open data.

    Supports two data formats:
    - Real Receita Federal: headerless CSVs (`;` delimiter, latin-1) with multiple files
    - Simple CSV: header-based CSVs for testing/development
    """

    name = "cnpj"
    source_id = "receita_federal"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size)
        self._raw_empresas: pd.DataFrame = pd.DataFrame()
        self._raw_socios: pd.DataFrame = pd.DataFrame()
        self._raw_estabelecimentos: pd.DataFrame = pd.DataFrame()
        self._reference_tables: dict[str, dict[str, str]] = {}
        # basico -> (cnpj_full, cnae_principal, uf, municipio)
        self._estab_lookup: dict[str, tuple[str, str, str, str]] = {}
        self.companies: list[dict[str, Any]] = []
        self.partners: list[dict[str, Any]] = []
        self.relationships: list[dict[str, Any]] = []

    # --- Reference tables ---

    def _load_reference_tables(self) -> None:
        """Load reference lookup tables (naturezas, qualificacoes, etc.)."""
        ref_dir = Path(self.data_dir) / "cnpj" / "reference"
        if not ref_dir.exists():
            return

        for table_name in REFERENCE_TABLES:
            files = list(ref_dir.glob(f"*{table_name}*"))
            if not files:
                continue
            try:
                df = pd.read_csv(
                    files[0],
                    sep=";",
                    encoding="latin-1",
                    header=None,
                    names=["codigo", "descricao"],
                    dtype=str,
                    keep_default_na=False,
                )
                lookup = dict(zip(df["codigo"], df["descricao"], strict=False))
                self._reference_tables[table_name.lower()] = lookup
                logger.info("Loaded reference table %s: %d entries", table_name, len(lookup))
            except Exception:
                logger.warning("Could not load reference table %s", table_name)

    def _resolve_reference(self, table: str, code: str) -> str:
        """Look up a code in a reference table. Returns code if not found."""
        lookup = self._reference_tables.get(table, {})
        return lookup.get(code.strip(), code) if code else code

    # --- Reading ---

    def _read_bq_csv(
        self,
        pattern: str,
        rename_map: dict[str, str],
        drop_cols: set[str],
    ) -> pd.DataFrame:
        """Read Base dos Dados (BigQuery) exported CSVs with header row.

        BQ exports use different column names than Receita Federal raw files.
        This method reads header-based CSVs, drops BQ metadata columns, and
        renames columns to match the RF schema used by transform().
        """
        cnpj_dir = Path(self.data_dir) / "cnpj"
        files = sorted(cnpj_dir.glob(f"extracted/{pattern}"))
        if not files:
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        total_rows = 0
        for f in files:
            logger.info("Reading BQ export %s...", f.name)
            for chunk in pd.read_csv(
                f, dtype=str, keep_default_na=False, chunksize=self.chunk_size,
            ):
                chunk = chunk.drop(columns=[c for c in drop_cols if c in chunk.columns])
                chunk = chunk.rename(columns=rename_map)
                frames.append(chunk)
                total_rows += len(chunk)
                if self.limit and total_rows >= self.limit:
                    break
            if self.limit and total_rows >= self.limit:
                break

        if not frames:
            return pd.DataFrame()
        result = pd.concat(frames, ignore_index=True)
        if self.limit:
            result = result.head(self.limit)
        logger.info("Read %d rows from BQ export %s", len(result), pattern)
        return result

    def _read_rf_chunks(self, pattern: str, columns: list[str]) -> pd.DataFrame:
        """Read Receita Federal headerless CSVs with chunking for memory efficiency."""
        cnpj_dir = Path(self.data_dir) / "cnpj"
        # Search both extracted/ subdirectory and cnpj/ root
        files = sorted(cnpj_dir.glob(f"extracted/{pattern}"))
        if not files:
            files = sorted(cnpj_dir.glob(pattern))
        if not files:
            return pd.DataFrame(columns=columns)

        frames: list[pd.DataFrame] = []
        total_rows = 0
        for f in files:
            logger.info("Reading %s...", f.name)
            for chunk in pd.read_csv(
                f,
                sep=";",
                encoding="latin-1",
                header=None,
                names=columns,
                dtype=str,
                keep_default_na=False,
                chunksize=self.chunk_size,
            ):
                frames.append(chunk)
                total_rows += len(chunk)
                if self.limit and total_rows >= self.limit:
                    break
            if self.limit and total_rows >= self.limit:
                break

        if not frames:
            return pd.DataFrame(columns=columns)
        result = pd.concat(frames, ignore_index=True)
        if self.limit:
            result = result.head(self.limit)
        logger.info("Read %d rows from %s", len(result), pattern)
        return result

    def extract(self) -> None:
        """Extract data from Receita Federal open data files.

        Tries three formats in order:
        1. Real RF format: headerless `;`-delimited CSVs (production)
        2. Base dos Dados (BigQuery) exports: header-based CSVs with BQ column names
        3. Simple CSV: header-based CSVs with our own column names (dev/test)
        """
        # Load reference tables if available
        self._load_reference_tables()

        cnpj_dir = Path(self.data_dir) / "cnpj"

        # 1. Try real RF format: *EMPRE* or Empresas*
        rf_empresas = self._read_rf_chunks("*EMPRE*", EMPRESAS_COLS)
        if rf_empresas.empty:
            rf_empresas = self._read_rf_chunks("Empresas*", EMPRESAS_COLS)

        if not rf_empresas.empty:
            self._raw_empresas = rf_empresas
            self._raw_socios = self._read_rf_chunks("*SOCIO*", SOCIOS_COLS)
            if self._raw_socios.empty:
                self._raw_socios = self._read_rf_chunks("Socios*", SOCIOS_COLS)
            self._raw_estabelecimentos = self._read_rf_chunks(
                "*ESTABELE*", ESTABELECIMENTOS_COLS,
            )
            if self._raw_estabelecimentos.empty:
                self._raw_estabelecimentos = self._read_rf_chunks(
                    "Estabelecimentos*", ESTABELECIMENTOS_COLS,
                )
        else:
            # 2. Try BigQuery exports (empresas_*.csv with headers)
            bq_empresas = self._read_bq_csv(
                "empresas_*.csv", BQ_EMPRESAS_RENAME, BQ_EMPRESAS_DROP,
            )
            if not bq_empresas.empty:
                logger.info("Using Base dos Dados (BigQuery) exported data")
                self._raw_empresas = bq_empresas
                self._raw_socios = self._read_bq_csv(
                    "socios_*.csv", BQ_SOCIOS_RENAME, BQ_SOCIOS_DROP,
                )
                self._raw_estabelecimentos = self._read_bq_csv(
                    "estabelecimentos_*.csv",
                    BQ_ESTABELECIMENTOS_RENAME,
                    BQ_ESTABELECIMENTOS_DROP,
                )
            else:
                # 3. Simple CSV fallback (dev/test)
                empresas_path = cnpj_dir / "empresas.csv"
                socios_path = cnpj_dir / "socios.csv"
                estabelecimentos_path = cnpj_dir / "estabelecimentos.csv"
                if empresas_path.exists():
                    self._raw_empresas = pd.read_csv(
                        empresas_path, dtype=str, keep_default_na=False,
                    )
                if socios_path.exists():
                    self._raw_socios = pd.read_csv(
                        socios_path, dtype=str, keep_default_na=False,
                    )
                if estabelecimentos_path.exists():
                    self._raw_estabelecimentos = pd.read_csv(
                        estabelecimentos_path, dtype=str, keep_default_na=False,
                    )

        logger.info(
            "Extracted: %d empresas, %d socios, %d estabelecimentos",
            len(self._raw_empresas),
            len(self._raw_socios),
            len(self._raw_estabelecimentos),
        )

    # --- Vectorized transform helpers ---

    def _build_estab_lookup(self, df: pd.DataFrame) -> None:
        """Add estabelecimentos rows to estab_lookup (vectorized prep, zip on deduped)."""
        df = df.copy()
        df["basico"] = df["cnpj_basico"].astype(str).str.zfill(8)
        df["ordem"] = df["cnpj_ordem"].astype(str).str.zfill(4)
        df["dv"] = df["cnpj_dv"].astype(str).str.zfill(2)
        df["cnpj_raw"] = df["basico"] + df["ordem"] + df["dv"]
        # Skip already-seen basico keys, then dedup within chunk
        mask = ~df["basico"].isin(self._estab_lookup)
        df = df.loc[mask].drop_duplicates(subset="basico", keep="first")
        if df.empty:
            return
        for basico, cnpj_raw, cnae, uf, mun in zip(
            df["basico"],
            df["cnpj_raw"],
            df["cnae_principal"].astype(str),
            df["uf"].astype(str),
            df["municipio"].astype(str),
            strict=False,
        ):
            self._estab_lookup[basico] = (format_cnpj(cnpj_raw), cnae, uf, mun)

    def _transform_empresas_rf(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """Vectorized transform for RF-format empresas."""
        df = df.copy()
        df["basico"] = df["cnpj_basico"].astype(str).str.zfill(8)
        lookup = self._estab_lookup
        df["cnpj"] = df["basico"].map(
            lambda b: lookup[b][0] if b in lookup else format_cnpj(b + "000100"),
        )
        df["capital_social"] = df["capital_social"].astype(str).map(parse_capital_social)
        df["razao_social"] = df["razao_social"].astype(str).map(normalize_name)
        df["natureza_juridica"] = df["natureza_juridica"].astype(str).map(
            lambda c: self._resolve_reference("naturezas", c),
        )
        df["cnae_principal"] = df["basico"].map(
            lambda b: self._resolve_reference(
                "cnaes", lookup[b][1] if b in lookup else "",
            ),
        )
        df["uf"] = df["basico"].map(lambda b: lookup[b][2] if b in lookup else "")
        df["municipio"] = df["basico"].map(
            lambda b: self._resolve_reference(
                "municipios", lookup[b][3] if b in lookup else "",
            ),
        )
        df["porte_empresa"] = df["porte_empresa"].astype(str)
        cols = ["cnpj", "razao_social", "natureza_juridica", "cnae_principal",
                "capital_social", "uf", "municipio", "porte_empresa"]
        return df[cols].to_dict("records")  # type: ignore[return-value]

    def _transform_empresas_simple(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """Vectorized transform for simple-format empresas."""
        df = df.copy()
        df["cnpj"] = df["cnpj"].astype(str).map(format_cnpj)
        df["capital_social"] = df["capital_social"].astype(str).map(parse_capital_social)
        df["razao_social"] = df["razao_social"].astype(str).map(normalize_name)
        default = pd.Series("", index=df.index)
        df["natureza_juridica"] = df.get("natureza_juridica", default).astype(str)
        df["cnae_principal"] = df["cnae_principal"].astype(str)
        df["uf"] = df["uf"].astype(str)
        df["municipio"] = df["municipio"].astype(str)
        df["porte_empresa"] = df.get("porte_empresa", pd.Series("", index=df.index)).astype(str)
        cols = ["cnpj", "razao_social", "natureza_juridica", "cnae_principal",
                "capital_social", "uf", "municipio", "porte_empresa"]
        return df[cols].to_dict("records")  # type: ignore[return-value]

    def _transform_socios_rf(
        self, df: pd.DataFrame,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Vectorized transform for RF-format socios. Returns (partners, relationships)."""
        df = df.copy()
        lookup = self._estab_lookup
        df["basico"] = df["cnpj_basico"].astype(str).str.zfill(8)
        df["cnpj"] = df["basico"].map(
            lambda b: lookup[b][0] if b in lookup else format_cnpj(b + "000100"),
        )
        df["nome"] = df["nome_socio"].astype(str).map(normalize_name)
        df["cpf"] = df["cpf_cnpj_socio"].astype(str)
        df["tipo"] = df["identificador_socio"].astype(str)
        df["qualificacao"] = df["qualificacao_socio"].astype(str).map(
            lambda c: self._resolve_reference("qualificacoes", c),
        )
        df["data_entrada"] = df["data_entrada"].astype(str)
        partners: list[dict[str, Any]] = df[["nome", "cpf", "tipo"]].rename(
            columns={"nome": "name", "tipo": "tipo_socio"},
        ).to_dict("records")  # type: ignore[assignment]
        relationships: list[dict[str, Any]] = pd.DataFrame({
            "source_key": df["cpf"],
            "target_key": df["cnpj"],
            "tipo_socio": df["tipo"],
            "qualificacao": df["qualificacao"],
            "data_entrada": df["data_entrada"],
        }).to_dict("records")  # type: ignore[assignment]
        return partners, relationships

    def _transform_socios_simple(
        self, df: pd.DataFrame,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Vectorized transform for simple-format socios. Returns (partners, relationships)."""
        df = df.copy()
        df["cnpj"] = df["cnpj"].astype(str).map(format_cnpj)
        df["nome"] = df["nome_socio"].astype(str).map(normalize_name)
        df["cpf"] = df["cpf_socio"].astype(str)
        df["tipo"] = df["tipo_socio"].astype(str)
        df["qualificacao"] = df.get(
            "qualificacao_socio", pd.Series("", index=df.index),
        ).astype(str)
        df["data_entrada"] = df.get(
            "data_entrada", pd.Series("", index=df.index),
        ).astype(str)
        partners: list[dict[str, Any]] = df[["nome", "cpf", "tipo"]].rename(
            columns={"nome": "name", "tipo": "tipo_socio"},
        ).to_dict("records")  # type: ignore[assignment]
        relationships: list[dict[str, Any]] = pd.DataFrame({
            "source_key": df["cpf"],
            "target_key": df["cnpj"],
            "tipo_socio": df["tipo"],
            "qualificacao": df["qualificacao"],
            "data_entrada": df["data_entrada"],
        }).to_dict("records")  # type: ignore[assignment]
        return partners, relationships

    def transform(self) -> None:
        """Transform raw data into normalized company, partner, and relationship records."""
        if not self._raw_estabelecimentos.empty:
            self._build_estab_lookup(self._raw_estabelecimentos)

        is_rf = "cnpj_basico" in self._raw_empresas.columns
        if is_rf:
            companies = self._transform_empresas_rf(self._raw_empresas)
        else:
            companies = self._transform_empresas_simple(self._raw_empresas)
        self.companies = deduplicate_rows(companies, ["cnpj"])
        logger.info("Transformed %d companies", len(self.companies))

        is_rf_socios = "cpf_cnpj_socio" in self._raw_socios.columns
        if is_rf_socios:
            partners, rels = self._transform_socios_rf(self._raw_socios)
        else:
            partners, rels = self._transform_socios_simple(self._raw_socios)
        self.partners = deduplicate_rows(partners, ["cpf"])
        self.relationships = rels
        logger.info(
            "Transformed %d partners, %d relationships",
            len(self.partners),
            len(self.relationships),
        )

    # --- Streaming pipeline for large datasets ---

    def _find_rf_files(self, pattern: str) -> list[Path]:
        """Find RF-format data files, checking extracted/ then cnpj/ root."""
        cnpj_dir = Path(self.data_dir) / "cnpj"
        files = sorted(cnpj_dir.glob(f"extracted/{pattern}"))
        if not files:
            files = sorted(cnpj_dir.glob(pattern))
        return files

    def _find_bq_files(self, pattern: str) -> list[Path]:
        """Find BQ-format CSVs in extracted/ directory."""
        cnpj_dir = Path(self.data_dir) / "cnpj"
        return sorted(cnpj_dir.glob(f"extracted/{pattern}"))

    def _read_rf_file_chunks(
        self, path: Path, columns: list[str],
    ) -> pd.io.parsers.readers.TextFileReader:
        """Return a chunked reader for a single RF-format CSV."""
        return pd.read_csv(
            path,
            sep=";",
            encoding="latin-1",
            header=None,
            names=columns,
            dtype=str,
            keep_default_na=False,
            chunksize=self.chunk_size,
        )

    def _read_bq_file_chunks(
        self,
        path: Path,
        rename_map: dict[str, str],
        drop_cols: set[str],
    ) -> pd.io.parsers.readers.TextFileReader:
        """Return a chunked reader for a BQ-format CSV that renames/drops per chunk.

        Yields DataFrames with columns renamed to RF schema and BQ metadata dropped.
        """
        reader = pd.read_csv(
            path, dtype=str, keep_default_na=False, chunksize=self.chunk_size,
        )
        return _BQChunkAdapter(reader, rename_map, drop_cols)  # type: ignore[return-value]

    def run_streaming(self, start_phase: int = 1) -> None:
        """Stream-process data files chunk-by-chunk. For datasets that don't fit in memory.

        Tries RF-format files first, falls back to BQ-format CSVs.

        Phase 1: Build estab_lookup from all Estabelecimentos files.
        Phase 2: Stream Empresas -> transform -> load Company nodes.
        Phase 3: Stream Socios -> transform -> load Person nodes + SOCIO_DE relationships.
        """
        self._load_reference_tables()
        loader = Neo4jBatchLoader(self.driver, batch_size=self.chunk_size)
        total_companies = 0
        total_partners = 0
        total_rels = 0

        # Detect format: RF files first, then BQ files
        estab_files = self._find_rf_files("*ESTABELE*")
        if not estab_files:
            estab_files = self._find_rf_files("Estabelecimentos*")
        use_bq = not estab_files

        if use_bq:
            bq_estab = self._find_bq_files("estabelecimentos*.csv")
            bq_emp = self._find_bq_files("empresas*.csv")
            bq_socio = self._find_bq_files("socios*.csv")
            if not bq_estab and not bq_emp:
                logger.warning("No RF or BQ data files found")
                return
            logger.info("Using Base dos Dados (BigQuery) format for streaming")
        else:
            bq_estab = bq_emp = bq_socio = []

        # Phase 1: Build estab_lookup
        if use_bq:
            logger.info("Phase 1: Building estab_lookup from %d BQ files", len(bq_estab))
            for f in bq_estab:
                logger.info("  Reading %s...", f.name)
                for chunk in self._read_bq_file_chunks(
                    f, BQ_ESTABELECIMENTOS_RENAME, BQ_ESTABELECIMENTOS_DROP,
                ):
                    self._build_estab_lookup(chunk)
        else:
            logger.info("Phase 1: Building estab_lookup from %d RF files", len(estab_files))
            for f in estab_files:
                logger.info("  Reading %s...", f.name)
                for chunk in self._read_rf_file_chunks(f, ESTABELECIMENTOS_COLS):
                    self._build_estab_lookup(chunk)
        logger.info(
            "  estab_lookup: %d unique basico keys", len(self._estab_lookup),
        )

        # Phase 2: Stream Empresas -> load
        if start_phase > 2:
            logger.info("Skipping Phase 2 (empresas) — start_phase=%d", start_phase)
        elif use_bq:
            emp_files = bq_emp
            logger.info("Phase 2: Streaming %d BQ Empresas files", len(emp_files))
            for f in emp_files:
                logger.info("  Processing %s...", f.name)
                for chunk in self._read_bq_file_chunks(
                    f, BQ_EMPRESAS_RENAME, BQ_EMPRESAS_DROP,
                ):
                    companies = self._transform_empresas_rf(chunk)
                    if companies:
                        loader.load_nodes("Company", companies, key_field="cnpj")
                        total_companies += len(companies)
                logger.info("  Companies loaded so far: %d", total_companies)
        else:
            emp_files = self._find_rf_files("*EMPRE*")
            if not emp_files:
                emp_files = self._find_rf_files("Empresas*")
            logger.info("Phase 2: Streaming %d RF Empresas files", len(emp_files))
            for f in emp_files:
                logger.info("  Processing %s...", f.name)
                for chunk in self._read_rf_file_chunks(f, EMPRESAS_COLS):
                    companies = self._transform_empresas_rf(chunk)
                    if companies:
                        loader.load_nodes("Company", companies, key_field="cnpj")
                        total_companies += len(companies)
                logger.info("  Companies loaded so far: %d", total_companies)

        # Phase 3: Stream Socios -> load
        if use_bq:
            socio_files = bq_socio
            logger.info("Phase 3: Streaming %d BQ Socios files", len(socio_files))
        else:
            socio_files = self._find_rf_files("*SOCIO*")
            if not socio_files:
                socio_files = self._find_rf_files("Socios*")
            logger.info("Phase 3: Streaming %d RF Socios files", len(socio_files))

        for f in socio_files:
            logger.info("  Processing %s...", f.name)
            chunks = (
                self._read_bq_file_chunks(f, BQ_SOCIOS_RENAME, BQ_SOCIOS_DROP)
                if use_bq
                else self._read_rf_file_chunks(f, SOCIOS_COLS)
            )
            for chunk in chunks:
                partners, rels = self._transform_socios_rf(chunk)
                if partners:
                    loader.load_nodes("Person", partners, key_field="cpf")
                    total_partners += len(partners)
                if rels:
                    loader.load_relationships(
                        rel_type="SOCIO_DE",
                        rows=rels,
                        source_label="Person",
                        source_key="cpf",
                        target_label="Company",
                        target_key="cnpj",
                        properties=["tipo_socio", "qualificacao", "data_entrada"],
                    )
                    total_rels += len(rels)
            logger.info(
                "  Partners: %d, Relationships: %d so far",
                total_partners, total_rels,
            )

        logger.info(
            "Streaming complete: %d companies, %d partners, %d relationships",
            total_companies, total_partners, total_rels,
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.companies:
            loader.load_nodes("Company", self.companies, key_field="cnpj")

        if self.partners:
            loader.load_nodes("Person", self.partners, key_field="cpf")

        if self.relationships:
            loader.load_relationships(
                rel_type="SOCIO_DE",
                rows=self.relationships,
                source_label="Person",
                source_key="cpf",
                target_label="Company",
                target_key="cnpj",
                properties=["tipo_socio", "qualificacao", "data_entrada"],
            )
