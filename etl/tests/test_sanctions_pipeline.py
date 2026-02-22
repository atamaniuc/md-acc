from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from icarus_etl.pipelines.sanctions import SanctionsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SanctionsPipeline:
    driver = MagicMock()
    pipeline = SanctionsPipeline(driver=driver, data_dir=str(FIXTURES.parent))
    return pipeline


def _load_fixture_data(pipeline: SanctionsPipeline) -> None:
    """Load CSV fixtures directly into the pipeline's raw DataFrames."""
    pipeline._raw_ceis = pd.read_csv(
        FIXTURES / "ceis_sample.csv",
        dtype=str,
        keep_default_na=False,
    )
    pipeline._raw_cnep = pd.read_csv(
        FIXTURES / "cnep_sample.csv",
        dtype=str,
        keep_default_na=False,
    )


class TestSanctionsPipelineMetadata:
    def test_name(self) -> None:
        assert SanctionsPipeline.name == "sanctions"

    def test_source_id(self) -> None:
        assert SanctionsPipeline.source_id == "ceis_cnep"


class TestSanctionsTransform:
    def test_produces_sanctions(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        assert len(pipeline.sanctions) == 4
        types = {s["type"] for s in pipeline.sanctions}
        assert types == {"CEIS", "CNEP"}

    def test_produces_sanctioned_entities(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        assert len(pipeline.sanctioned_entities) == 4

    def test_normalizes_names(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        names = {e["entity_name"] for e in pipeline.sanctioned_entities}
        assert "ACME CONSTRUTORA LTDA" in names
        assert "JOAO DA SILVA" in names
        assert "EMPRESA FANTASMA SA" in names

    def test_formats_cnpj(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        docs = {e["entity_doc"] for e in pipeline.sanctioned_entities}
        assert "12.345.678/0001-99" in docs

    def test_formats_cpf(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        docs = {e["entity_doc"] for e in pipeline.sanctioned_entities}
        assert "529.982.247-25" in docs

    def test_identifies_company_vs_person(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        labels = [e["entity_label"] for e in pipeline.sanctioned_entities]
        assert "Company" in labels
        assert "Person" in labels

    def test_parses_date_dd_mm_yyyy(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        ceis_sanctions = [s for s in pipeline.sanctions if s["type"] == "CEIS"]
        dates_start = {s["date_start"] for s in ceis_sanctions}
        assert "2023-01-01" in dates_start
        assert "2022-03-15" in dates_start

    def test_parses_date_iso(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        cnep_sanctions = [s for s in pipeline.sanctions if s["type"] == "CNEP"]
        dates_start = {s["date_start"] for s in cnep_sanctions}
        assert "2024-06-10" in dates_start

    def test_empty_date_end(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        # Second CEIS row has empty data_fim
        ceis_sanctions = [s for s in pipeline.sanctions if s["type"] == "CEIS"]
        date_ends = [s["date_end"] for s in ceis_sanctions]
        assert "" in date_ends

    def test_sanction_fields(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        s = pipeline.sanctions[0]
        assert "sanction_id" in s
        assert "type" in s
        assert "date_start" in s
        assert "date_end" in s
        assert "reason" in s
        assert "source" in s


class TestSanctionsLoad:
    def test_company_nodes_include_razao_social(self) -> None:
        """Company nodes created by sanctions must include razao_social."""
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = pipeline.driver.session.return_value.__enter__.return_value
        run_calls = session_mock.run.call_args_list

        # Find Company MERGE calls
        company_merge_calls = [
            call for call in run_calls
            if "MERGE (n:Company" in str(call)
        ]
        assert len(company_merge_calls) >= 1, "Expected Company MERGE calls"

        # Every Company row should have razao_social
        for call in company_merge_calls:
            rows = (
                call[1]["rows"]
                if "rows" in call[1]
                else call[0][1]["rows"]
            )
            for row in rows:
                assert "razao_social" in row, (
                    f"Company node missing razao_social: {row}"
                )
                assert row["razao_social"] == row["name"]

    def test_person_nodes_no_razao_social(self) -> None:
        """Person nodes should NOT have razao_social."""
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = pipeline.driver.session.return_value.__enter__.return_value
        run_calls = session_mock.run.call_args_list

        person_merge_calls = [
            call for call in run_calls
            if "MERGE (n:Person" in str(call)
        ]
        for call in person_merge_calls:
            rows = (
                call[1]["rows"]
                if "rows" in call[1]
                else call[0][1]["rows"]
            )
            for row in rows:
                assert "razao_social" not in row, (
                    f"Person node should not have razao_social: {row}"
                )
