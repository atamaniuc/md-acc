from unittest.mock import MagicMock

from icarus_etl.loader import Neo4jBatchLoader


class TestNeo4jBatchLoader:
    def _make_loader(self, batch_size: int = 10_000) -> tuple[Neo4jBatchLoader, MagicMock]:
        driver = MagicMock()
        session = MagicMock()
        driver.session.return_value.__enter__ = MagicMock(return_value=session)
        driver.session.return_value.__exit__ = MagicMock(return_value=False)
        return Neo4jBatchLoader(driver, batch_size=batch_size), session

    def test_load_nodes(self) -> None:
        loader, session = self._make_loader()
        rows = [
            {"cnpj": "111", "razao_social": "Company A", "uf": "SP"},
            {"cnpj": "222", "razao_social": "Company B", "uf": "RJ"},
        ]
        count = loader.load_nodes("Company", rows, "cnpj")
        assert count == 2
        session.run.assert_called_once()
        query = session.run.call_args[0][0]
        assert "MERGE" in query
        assert "Company" in query
        assert "cnpj" in query

    def test_load_nodes_empty(self) -> None:
        loader, session = self._make_loader()
        count = loader.load_nodes("Company", [], "cnpj")
        assert count == 0
        session.run.assert_not_called()

    def test_load_relationships(self) -> None:
        loader, session = self._make_loader()
        rows = [
            {"source_key": "111", "target_key": "AAA", "value": 1000},
        ]
        count = loader.load_relationships(
            "VENCEU", rows,
            "Company", "cnpj",
            "Contract", "contract_id",
            properties=["value"],
        )
        assert count == 1
        session.run.assert_called_once()
        query = session.run.call_args[0][0]
        assert "MERGE" in query
        assert "VENCEU" in query

    def test_batching(self) -> None:
        loader, session = self._make_loader(batch_size=2)
        rows = [
            {"cnpj": str(i), "name": f"C{i}"} for i in range(5)
        ]
        count = loader.load_nodes("Company", rows, "cnpj")
        assert count == 5
        assert session.run.call_count == 3  # 2+2+1

    def test_load_nodes_filters_empty_keys(self) -> None:
        loader, session = self._make_loader()
        rows = [
            {"cnpj": "111", "name": "A"},
            {"cnpj": "", "name": "B"},
            {"cnpj": None, "name": "C"},
            {"name": "D"},  # missing key entirely
            {"cnpj": "222", "name": "E"},
        ]
        count = loader.load_nodes("Company", rows, "cnpj")
        assert count == 2
        call_args = session.run.call_args
        batch = (
            call_args[1]["rows"] if call_args[1]
            else call_args[0][1]["rows"]
        )
        assert all(r["cnpj"] for r in batch)

    def test_load_relationships_filters_empty_keys(self) -> None:
        loader, session = self._make_loader()
        rows = [
            {"source_key": "111", "target_key": "AAA", "value": 10},
            {"source_key": "", "target_key": "BBB", "value": 20},
            {"source_key": "222", "target_key": "", "value": 30},
            {"source_key": None, "target_key": "CCC", "value": 40},
            {"source_key": "333", "target_key": None, "value": 50},
            {"source_key": "444", "target_key": "DDD", "value": 60},
        ]
        count = loader.load_relationships(
            "REL", rows, "A", "id", "B", "id", properties=["value"],
        )
        assert count == 2
        call_args = session.run.call_args
        batch = (
            call_args[1]["rows"] if call_args[1]
            else call_args[0][1]["rows"]
        )
        assert all(r["source_key"] and r["target_key"] for r in batch)

    def test_run_query(self) -> None:
        loader, session = self._make_loader()
        rows = [{"a": 1}]
        count = loader.run_query("UNWIND $rows AS row CREATE (n:Test)", rows)
        assert count == 1
