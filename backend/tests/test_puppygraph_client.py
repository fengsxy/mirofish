"""Tests for PuppyGraphClient."""

from unittest.mock import patch, MagicMock
import pytest

from app.services.puppygraph_client import PuppyGraphClient


class TestPuppyGraphClient:

    def test_noop_when_no_base_url(self):
        """PuppyGraphClient(base_url=None) returns None without errors."""
        client = PuppyGraphClient(base_url=None)
        # Force base_url to None in case Config has a value
        client.base_url = None
        assert client.upload_jsonl("db", "data") is None
        assert client.build_graph("db") is None
        assert client.query("db", "SELECT 1") is None

    @patch("app.services.puppygraph_client.requests.post")
    def test_upload_jsonl_calls_api(self, mock_post):
        mock_post.return_value = MagicMock(
            status_code=200, json=lambda: {"ok": True}
        )
        mock_post.return_value.raise_for_status = MagicMock()
        client = PuppyGraphClient(base_url="http://localhost:8000")
        result = client.upload_jsonl("mydb", '{"a":1}')
        assert result == {"ok": True}
        mock_post.assert_called_once_with(
            "http://localhost:8000/api/upload-jsonl",
            json={"db": "mydb", "data": '{"a":1}'},
            timeout=30,
        )

    @patch("app.services.puppygraph_client.requests.post")
    def test_build_graph_calls_api(self, mock_post):
        mock_post.return_value = MagicMock(
            status_code=200, json=lambda: {"status": "built"}
        )
        mock_post.return_value.raise_for_status = MagicMock()
        client = PuppyGraphClient(base_url="http://localhost:8000")
        result = client.build_graph("mydb")
        assert result == {"status": "built"}
        mock_post.assert_called_once_with(
            "http://localhost:8000/api/build-graph",
            json={"db": "mydb"},
            timeout=60,
        )

    @patch("app.services.puppygraph_client.requests.post")
    def test_query_calls_api(self, mock_post):
        mock_post.return_value = MagicMock(
            status_code=200, json=lambda: {"rows": []}
        )
        mock_post.return_value.raise_for_status = MagicMock()
        client = PuppyGraphClient(base_url="http://localhost:8000")
        result = client.query("mydb", "SELECT * FROM t")
        assert result == {"rows": []}
        mock_post.assert_called_once_with(
            "http://localhost:8000/api/query",
            json={"db": "mydb", "sql": "SELECT * FROM t"},
            timeout=30,
        )

    @patch("app.services.puppygraph_client.requests.post")
    def test_upload_handles_request_error(self, mock_post):
        mock_post.side_effect = Exception("connection refused")
        client = PuppyGraphClient(base_url="http://localhost:8000")
        result = client.upload_jsonl("mydb", "data")
        assert result is None
