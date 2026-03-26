"""
Tests for research_service module:
QueryExpander, WebSearcher, EvidenceCollector, ResearchService
"""

import pytest
from unittest.mock import patch, MagicMock

from app.services.research_service import (
    QueryExpander,
    WebSearcher,
    EvidenceCollector,
    ResearchService,
)


# ── QueryExpander Tests ──────────────────────────────────────────────


class TestQueryExpander:
    def setup_method(self):
        self.expander = QueryExpander()

    def test_rule_expand_strips_punctuation(self):
        result = self.expander._rule_expand("PuppyGraph未来发展？")
        assert "PuppyGraph未来发展？" in result
        assert "PuppyGraph未来发展" in result

    def test_rule_expand_no_punctuation(self):
        result = self.expander._rule_expand("PuppyGraph未来发展")
        assert result == ["PuppyGraph未来发展"]

    def test_rule_expand_multiple_punctuation(self):
        result = self.expander._rule_expand("AI会取代人类吗？！")
        assert "AI会取代人类吗？！" in result
        assert "AI会取代人类吗" in result

    def test_llm_expand_fallback_when_no_api_key(self):
        with patch("app.services.research_service.Config") as mock_config:
            mock_config.LLM_API_KEY = None
            result = self.expander._llm_expand("test question")
            assert result == []


# ── WebSearcher Tests ────────────────────────────────────────────────


class TestWebSearcher:
    def setup_method(self):
        self.searcher = WebSearcher()

    def test_search_ddg_returns_structured_results(self):
        mock_results = [
            {"title": "Title A", "href": "https://a.com", "body": "Snippet A"},
            {"title": "Title B", "href": "https://b.com", "body": "Snippet B"},
        ]
        with patch("app.services.research_service.DDGS") as MockDDGS:
            instance = MagicMock()
            instance.text.return_value = mock_results
            MockDDGS.return_value = instance

            results = self.searcher._search_ddg("test query", max_results=5)

        assert len(results) == 2
        assert results[0]["title"] == "Title A"
        assert results[0]["url"] == "https://a.com"
        assert results[0]["snippet"] == "Snippet A"
        assert results[0]["source"] == "duckduckgo"
        assert results[0]["query"] == "test query"

    def test_search_ddg_handles_exception(self):
        with patch("app.services.research_service.DDGS") as MockDDGS:
            instance = MagicMock()
            instance.text.side_effect = Exception("network error")
            MockDDGS.return_value = instance

            results = self.searcher._search_ddg("test query", max_results=5)

        assert results == []

    def test_search_uses_ddg_by_default(self):
        with patch("app.services.research_service.Config") as mock_config:
            mock_config.YOU_API_KEY = None
            with patch.object(self.searcher, "_search_ddg", return_value=[]) as mock_ddg:
                self.searcher.search("test query")
                mock_ddg.assert_called_once_with("test query", 5)


# ── EvidenceCollector Tests ──────────────────────────────────────────


class TestEvidenceCollector:
    def setup_method(self):
        self.collector = EvidenceCollector()

    def test_deduplicates_by_url(self):
        raw = [
            {"title": "A", "url": "https://a.com", "snippet": "s1"},
            {"title": "B", "url": "https://b.com", "snippet": "s2"},
            {"title": "A dup", "url": "https://a.com", "snippet": "s3"},
        ]
        result = self.collector.collect(raw)
        assert len(result) == 2
        urls = [r["url"] for r in result]
        assert urls == ["https://a.com", "https://b.com"]

    def test_assigns_sequential_ids(self):
        raw = [
            {"title": "A", "url": "https://a.com", "snippet": "s1"},
            {"title": "B", "url": "https://b.com", "snippet": "s2"},
        ]
        result = self.collector.collect(raw)
        assert result[0]["id"] == 0
        assert result[1]["id"] == 1

    def test_empty_input(self):
        result = self.collector.collect([])
        assert result == []


# ── ResearchService Tests ────────────────────────────────────────────


class TestResearchService:
    def test_confirm_builds_text_with_selected_evidence(self):
        evidence = [
            {"id": 0, "title": "Title A", "url": "https://a.com", "snippet": "Snippet A"},
            {"id": 1, "title": "Title B", "url": "https://b.com", "snippet": "Snippet B"},
            {"id": 2, "title": "Title C", "url": "https://c.com", "snippet": "Snippet C"},
        ]
        result = ResearchService._build_confirmed_text(evidence, [0, 2])
        text = result["text"]
        assert "Title A" in text
        assert "Title C" in text
        assert "Title B" not in text
        assert result["metadata"]["source_count"] == 2

    def test_confirm_includes_extra_text(self):
        evidence = [
            {"id": 0, "title": "Title A", "url": "https://a.com", "snippet": "Snippet A"},
        ]
        result = ResearchService._build_confirmed_text(evidence, [0], extra_text="Extra info")
        assert "Extra info" in result["text"]
        assert result["metadata"]["has_extra_text"] is True

    def test_confirm_no_extra_text(self):
        evidence = [
            {"id": 0, "title": "Title A", "url": "https://a.com", "snippet": "Snippet A"},
        ]
        result = ResearchService._build_confirmed_text(evidence, [0], extra_text="")
        assert "补充信息" not in result["text"]
        assert result["metadata"]["has_extra_text"] is False
