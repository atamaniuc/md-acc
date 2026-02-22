from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from icarus_etl.pipelines.tse import TSEPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> TSEPipeline:
    driver = MagicMock()
    pipeline = TSEPipeline(driver, data_dir=str(FIXTURES.parent))
    return pipeline


def _extract_from_fixtures(pipeline: TSEPipeline) -> None:
    """Extract from test fixtures instead of data_dir/tse/."""
    import pandas as pd

    pipeline._raw_candidatos = pd.read_csv(
        FIXTURES / "tse_candidatos.csv", encoding="latin-1", dtype=str
    )
    pipeline._raw_doacoes = pd.read_csv(
        FIXTURES / "tse_doacoes.csv", encoding="latin-1", dtype=str
    )


def test_pipeline_metadata() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "tse"
    assert pipeline.source_id == "tribunal_superior_eleitoral"


def test_transform_produces_candidates_keyed_by_sq() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    # 3 rows but 2 unique sq_candidato values (100001 appears twice)
    assert len(pipeline.candidates) == 2
    sqs = {c["sq_candidato"] for c in pipeline.candidates}
    assert "100001" in sqs
    assert "100002" in sqs


def test_transform_keeps_real_cpf_drops_masked() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    by_sq = {c["sq_candidato"]: c for c in pipeline.candidates}
    # 100001 has real CPF in first row (12345678901) — should keep it
    assert by_sq["100001"].get("cpf") == "123.456.789-01"
    # 100002 has real CPF
    assert by_sq["100002"].get("cpf") == "987.654.321-00"


def test_transform_normalizes_names() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    names = {c["name"] for c in pipeline.candidates}
    assert "JOAO DA SILVA" in names
    assert "MARIA JOSE SANTOS" in names


def test_transform_stores_partido() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    partidos = {c["partido"] for c in pipeline.candidates}
    assert "PL" in partidos
    assert "PT" in partidos


def test_transform_creates_elections() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.elections) == 3
    years = {e["year"] for e in pipeline.elections}
    assert years == {2022, 2024}


def test_transform_elections_use_sq_candidato() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    for e in pipeline.elections:
        assert "candidate_sq" in e
        assert e["candidate_sq"] in {"100001", "100002"}


def test_transform_parses_donation_values() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.donations) == 3
    valores = sorted(d["valor"] for d in pipeline.donations)
    assert valores[0] == 200.00
    assert valores[1] == 1500.50
    assert valores[2] == 50000.00


def test_transform_donations_link_via_sq() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    for d in pipeline.donations:
        assert "candidate_sq" in d
        assert d["candidate_sq"] in {"100001", "100002"}


def test_transform_identifies_company_donors() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    company_donations = [d for d in pipeline.donations if d["donor_is_company"]]
    person_donations = [d for d in pipeline.donations if not d["donor_is_company"]]

    assert len(company_donations) == 1
    assert company_donations[0]["donor_name"] == "EMPRESA ABC LTDA"
    assert len(person_donations) == 2


def test_load_company_donors_include_razao_social() -> None:
    """Company donor nodes must include razao_social for API compatibility."""
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()
    pipeline.load()

    # Collect all load_nodes calls from the mock driver
    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list

    # Find the MERGE (n:Company ...) call — its rows should include razao_social
    company_merge_calls = [
        call for call in run_calls
        if "MERGE (n:Company" in str(call)
    ]
    assert len(company_merge_calls) >= 1, "Expected at least one Company MERGE call"

    # Extract the rows from the first Company MERGE call
    call = company_merge_calls[0]
    call_kwargs = call[1] if call[1] else {}
    company_rows = (
        call_kwargs["rows"] if "rows" in call_kwargs else call[0][1]["rows"]
    )
    for row in company_rows:
        assert "razao_social" in row, f"Company donor row missing razao_social: {row}"
        assert row["razao_social"] == row["name"], (
            f"razao_social should match name for TSE donors: {row}"
        )
