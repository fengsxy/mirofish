"""Tests for DuckDBStore."""

import json
import os
import tempfile

import pytest
from unittest.mock import patch, MagicMock

from app.services.duckdb_store import DuckDBStore


@pytest.fixture
def store(tmp_path):
    db_path = str(tmp_path / "test.db")
    s = DuckDBStore(db_path=db_path)
    yield s
    s.close()


class TestTableInit:
    def test_tables_created(self, store):
        tables = store.query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        )
        table_names = {row["table_name"] for row in tables}
        assert {"evidence", "entities", "agents", "interactions", "reports"} <= table_names


class TestStoreEvidence:
    def test_store_and_query(self, store):
        store.store_evidence("proj123", [
            {"content": "Evidence A", "source": "file1.pdf"},
            {"content": "Evidence B", "source": "file2.pdf"},
        ])
        rows = store.query("SELECT * FROM evidence WHERE project_id = ?", ["proj123"])
        assert len(rows) == 2
        assert rows[0]["content"] == "Evidence A"
        assert rows[1]["source"] == "file2.pdf"

    def test_store_empty_list(self, store):
        store.store_evidence("proj123", [])
        rows = store.query("SELECT * FROM evidence WHERE project_id = ?", ["proj123"])
        assert len(rows) == 0


class TestStoreEntities:
    def test_store_and_query(self, store):
        entities = [
            {
                "uuid": "ent-001",
                "name": "Alice",
                "labels": ["Entity", "Person"],
                "attributes": {"age": 30},
            },
            {
                "uuid": "ent-002",
                "name": "Acme Corp",
                "labels": ["Organization"],
                "attributes": {"industry": "tech"},
            },
        ]
        store.store_entities("proj456", entities)
        rows = store.query("SELECT * FROM entities WHERE project_id = ?", ["proj456"])
        assert len(rows) == 2
        # First entity: type should be "Person" (skip "Entity")
        person = [r for r in rows if r["name"] == "Alice"][0]
        assert person["type"] == "Person"
        # Second entity: type should be "Organization"
        org = [r for r in rows if r["name"] == "Acme Corp"][0]
        assert org["type"] == "Organization"


class TestStoreAgents:
    def test_store_and_query(self, store):
        agents = [
            {
                "user_id": "u1",
                "name": "Agent Smith",
                "persona": "A mysterious agent",
                "role": "antagonist",
            }
        ]
        store.store_agents("proj789", agents)
        rows = store.query("SELECT * FROM agents WHERE project_id = ?", ["proj789"])
        assert len(rows) == 1
        assert rows[0]["name"] == "Agent Smith"
        assert rows[0]["persona"] == "A mysterious agent"
        assert rows[0]["user_id"] == "u1"
        attrs = json.loads(rows[0]["attributes"])
        assert attrs["role"] == "antagonist"


class TestStoreInteractions:
    def test_parse_actions_jsonl(self, store, tmp_path):
        twitter_dir = tmp_path / "twitter"
        twitter_dir.mkdir()
        actions = [
            {
                "agent_name": "Alice",
                "action_type": "CREATE_POST",
                "action_args": {"content": "Hello world"},
            },
            {
                "agent_name": "Bob",
                "action_type": "CREATE_COMMENT",
                "action_args": {"content": "Nice post!", "post_author_name": "Alice"},
            },
            {
                "agent_name": "Charlie",
                "action_type": "LIKE_POST",
                "action_args": {"post_author_name": "Alice"},
            },
        ]
        with open(twitter_dir / "actions.jsonl", "w") as f:
            for a in actions:
                f.write(json.dumps(a) + "\n")

        store.store_interactions("projABC", str(tmp_path))
        rows = store.query("SELECT * FROM interactions WHERE project_id = ?", ["projABC"])
        assert len(rows) == 3

        # CREATE_POST has no target
        create_post = [r for r in rows if r["action_type"] == "CREATE_POST"][0]
        assert create_post["target_agent"] == ""
        assert create_post["content"] == "Hello world"

        # CREATE_COMMENT target = post_author_name
        comment = [r for r in rows if r["action_type"] == "CREATE_COMMENT"][0]
        assert comment["target_agent"] == "Alice"

        # LIKE_POST target = post_author_name
        like = [r for r in rows if r["action_type"] == "LIKE_POST"][0]
        assert like["target_agent"] == "Alice"

    def test_no_actions_dir(self, store, tmp_path):
        store.store_interactions("projXYZ", str(tmp_path))
        rows = store.query("SELECT * FROM interactions WHERE project_id = ?", ["projXYZ"])
        assert len(rows) == 0


class TestStoreReport:
    def test_store_and_query(self, store):
        store.store_report("projREP", "This is the final report.")
        rows = store.query("SELECT * FROM reports WHERE project_id = ?", ["projREP"])
        assert len(rows) == 1
        assert rows[0]["report_text"] == "This is the final report."
        assert rows[0]["id"].startswith("rp_projREP_")


class TestExportToPuppyGraph:
    def test_export_calls_client(self, store):
        mock_instance = MagicMock()
        mock_instance.base_url = "http://localhost:8000"

        # Insert some data
        store.store_evidence("proj_export", [{"content": "ev1", "source": "s1"}])
        store.store_report("proj_export", "report text")

        with patch("app.services.puppygraph_client.PuppyGraphClient", return_value=mock_instance) as MockClient:
            with patch.dict("sys.modules", {}):
                # Patch at the source module so the local import picks it up
                with patch("app.services.duckdb_store.PuppyGraphClient", MockClient, create=True):
                    store.export_to_puppygraph("proj_export")

        assert mock_instance.upload_jsonl.called
        mock_instance.build_graph.assert_called_once_with("bibibo_proj_export")

    def test_export_skips_when_no_url(self, store):
        mock_instance = MagicMock()
        mock_instance.base_url = None

        store.store_evidence("proj_skip", [{"content": "ev1", "source": "s1"}])

        with patch("app.services.puppygraph_client.PuppyGraphClient", return_value=mock_instance) as MockClient:
            with patch("app.services.duckdb_store.PuppyGraphClient", MockClient, create=True):
                store.export_to_puppygraph("proj_skip")

        mock_instance.upload_jsonl.assert_not_called()
