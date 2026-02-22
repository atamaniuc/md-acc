from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from icarus_etl.pipelines.transparencia import TransparenciaPipeline, _parse_brl

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> TransparenciaPipeline:
    driver = MagicMock()
    pipeline = TransparenciaPipeline(driver, data_dir=str(FIXTURES))
    # Fixtures are at fixtures/transparencia_*.csv but pipeline expects
    # {data_dir}/transparencia/*.csv — symlink by overriding extraction
    return pipeline


def _extract_from_fixtures(pipeline: TransparenciaPipeline) -> None:
    """Extract directly from fixture files instead of subdirectory."""
    import pandas as pd

    pipeline._raw_contratos = pd.read_csv(
        FIXTURES / "transparencia_contratos.csv",
        dtype=str,
        keep_default_na=False,
    )
    pipeline._raw_servidores = pd.read_csv(
        FIXTURES / "transparencia_servidores.csv",
        dtype=str,
        keep_default_na=False,
    )
    pipeline._raw_emendas = pd.read_csv(
        FIXTURES / "transparencia_emendas.csv",
        dtype=str,
        keep_default_na=False,
    )


def test_pipeline_name_and_source_id() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "transparencia"
    assert pipeline.source_id == "portal_transparencia"


def test_transform_produces_correct_contracts() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.contracts) == 3
    contract = pipeline.contracts[0]
    assert contract["contracting_org"] == "MINISTERIO DA SAUDE"
    assert contract["object"] == "SERVICO DE LIMPEZA"
    assert contract["cnpj"] == "11.222.333/0001-81"
    assert contract["date"] == "2024-01-15"


def test_transform_filters_sigiloso_contracts() -> None:
    """Contracts with CNPJ=-11 (classified) should be filtered out."""
    import pandas as pd

    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)

    # Add a sigiloso row to the raw data
    sigiloso = pd.DataFrame([{
        "cnpj_contratada": "-11",
        "razao_social": "Sigiloso",
        "objeto": "Classificado",
        "valor": "100.000,00",
        "orgao_contratante": "Policia Federal",
        "data_inicio": "2024-01-01",
    }])
    pipeline._raw_contratos = pd.concat(
        [pipeline._raw_contratos, sigiloso], ignore_index=True,
    )

    pipeline.transform()
    cnpjs = [c["cnpj"] for c in pipeline.contracts]
    assert all(c != "-11" for c in cnpjs)
    # Original 3 contracts still present
    assert len(pipeline.contracts) == 3


def test_transform_parses_monetary_values() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert pipeline.contracts[0]["value"] == 1_500_000.00
    assert pipeline.contracts[1]["value"] == 3_200_000.50
    assert pipeline.offices[0]["salary"] == 15_500.00
    assert pipeline.offices[1]["salary"] == 22_300.50


def test_transform_deduplicates_contracts() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    # 3 rows, all unique contract_ids
    assert len(pipeline.contracts) == 3
    ids = [c["contract_id"] for c in pipeline.contracts]
    assert len(set(ids)) == 3


def test_transform_normalizes_server_names() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.offices) == 2
    assert pipeline.offices[0]["name"] == "MARIA DA SILVA SANTOS"
    assert pipeline.offices[0]["cpf"] == "123.456.789-01"


def test_transform_creates_amendment_nodes() -> None:
    """Emendas should produce Amendment nodes, not link to Contract."""
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.amendments) == 2
    amendment = pipeline.amendments[0]
    assert "amendment_id" in amendment
    assert "author_key" in amendment
    assert "object" in amendment
    assert "value" in amendment


def test_transform_amendment_ids_are_unique() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    ids = [a["amendment_id"] for a in pipeline.amendments]
    assert len(set(ids)) == len(ids)


def test_transform_skips_empty_cnpj_contracts() -> None:
    """Contracts with empty or non-digit CNPJ should be filtered out."""
    import pandas as pd

    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)

    # Add a row with empty CNPJ (only non-digit chars)
    empty_cnpj = pd.DataFrame([{
        "cnpj_contratada": "",
        "razao_social": "Fantasma Ltda",
        "objeto": "Servico Fantasma",
        "valor": "50.000,00",
        "orgao_contratante": "Orgao Inexistente",
        "data_inicio": "2024-01-15",
    }])
    pipeline._raw_contratos = pd.concat(
        [pipeline._raw_contratos, empty_cnpj], ignore_index=True,
    )

    pipeline.transform()
    # No malformed contract_ids (starting with underscore)
    assert all(not c["contract_id"].startswith("_") for c in pipeline.contracts)
    # Original 3 contracts still present
    assert len(pipeline.contracts) == 3


def test_parse_brl_handles_formats() -> None:
    assert _parse_brl("1.500.000,00") == 1_500_000.00
    assert _parse_brl("3.200.000,50") == 3_200_000.50
    assert _parse_brl("R$ 1.000,00") == 1_000.00
    assert _parse_brl("0") == 0.0
    assert _parse_brl("") == 0.0
    assert _parse_brl(None) == 0.0
