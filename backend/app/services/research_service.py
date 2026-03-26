"""
Auto-Research Service
Provides QueryExpander, WebSearcher, EvidenceCollector, and ResearchService
for automated web research workflows.
"""

import re
import json
import threading
from typing import Dict, List, Optional

import requests
from ddgs import DDGS
from openai import OpenAI

from ..config import Config
from ..models.task import TaskManager, TaskStatus
from ..utils.logger import get_logger

logger = get_logger("research_service")


class QueryExpander:
    """Expands a user question into multiple search queries via rules and LLM."""

    # Trailing punctuation to strip
    _TRAILING_PUNCT = re.compile(r'[？?。.！!~]+$')

    def expand(self, question: str) -> List[str]:
        """Return deduplicated list of search queries (rule + LLM)."""
        queries = self._rule_expand(question)
        llm_queries = self._llm_expand(question)
        seen = set()
        result = []
        for q in queries + llm_queries:
            if q not in seen:
                seen.add(q)
                result.append(q)
        return result

    def _rule_expand(self, question: str) -> List[str]:
        """Return original question + stripped-punctuation version (if different)."""
        clean = self._TRAILING_PUNCT.sub('', question)
        if clean == question:
            return [question]
        return [question, clean]

    def _llm_expand(self, question: str) -> List[str]:
        """Call LLM to generate 3-5 search queries. Falls back to [] on error."""
        if not Config.LLM_API_KEY:
            return []
        try:
            client = OpenAI(
                api_key=Config.LLM_API_KEY,
                base_url=Config.LLM_BASE_URL,
            )
            response = client.chat.completions.create(
                model=Config.LLM_MODEL_NAME,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a search query generator. Given a user question, "
                            "generate 3 to 5 diverse search queries that would help "
                            "research the topic. Return ONLY a JSON array of strings, "
                            "no other text."
                        ),
                    },
                    {"role": "user", "content": question},
                ],
                temperature=0.7,
            )
            content = response.choices[0].message.content.strip()
            # Try to extract JSON array from the response
            # Handle cases where LLM wraps in markdown code block
            if content.startswith("```"):
                content = re.sub(r'^```(?:json)?\s*', '', content)
                content = re.sub(r'\s*```$', '', content)
            queries = json.loads(content)
            if isinstance(queries, list):
                return [str(q) for q in queries]
            return []
        except Exception as e:
            logger.warning(f"LLM query expansion failed: {e}")
            return []


class WebSearcher:
    """Searches the web using You.com or DuckDuckGo."""

    def search(self, query: str, max_results: int = 5) -> List[Dict]:
        """Search using You.com if API key set, else DuckDuckGo."""
        if Config.YOU_API_KEY:
            return self._search_you(query, max_results)
        return self._search_ddg(query, max_results)

    def _search_ddg(self, query: str, max_results: int) -> List[Dict]:
        """Search via DuckDuckGo. Returns list of result dicts."""
        try:
            results = DDGS().text(query, max_results=max_results)
            return [
                {
                    "title": r.get("title", ""),
                    "url": r.get("href", ""),
                    "snippet": r.get("body", ""),
                    "source": "duckduckgo",
                    "query": query,
                }
                for r in results
            ]
        except Exception as e:
            logger.warning(f"DuckDuckGo search failed: {e}")
            return []

    def _search_you(self, query: str, max_results: int) -> List[Dict]:
        """Search via You.com API. Returns list of result dicts."""
        try:
            resp = requests.get(
                "https://api.ydc-index.io/search",
                headers={"X-API-Key": Config.YOU_API_KEY},
                params={"query": query, "num_web_results": max_results},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            results = []
            for hit in data.get("hits", [])[:max_results]:
                results.append(
                    {
                        "title": hit.get("title", ""),
                        "url": hit.get("url", ""),
                        "snippet": hit.get("description", hit.get("snippet", "")),
                        "source": "you.com",
                        "query": query,
                    }
                )
            return results
        except Exception as e:
            logger.warning(f"You.com search failed: {e}")
            return []


class EvidenceCollector:
    """Deduplicates search results and assigns sequential IDs."""

    def collect(self, raw_results: List[Dict]) -> List[Dict]:
        """Dedup by URL, assign sequential id field (0, 1, 2...), return list."""
        seen_urls = set()
        deduped = []
        for item in raw_results:
            url = item.get("url", "")
            if url in seen_urls:
                continue
            seen_urls.add(url)
            deduped.append(item)
        # Assign sequential IDs
        for idx, item in enumerate(deduped):
            item["id"] = idx
        return deduped


class ResearchService:
    """Orchestrates the full research pipeline."""

    def __init__(self):
        self.task_manager = TaskManager()
        self.query_expander = QueryExpander()
        self.web_searcher = WebSearcher()
        self.evidence_collector = EvidenceCollector()
        self._evidence_store: Dict[str, list] = {}
        self._queries_store: Dict[str, list] = {}
        self._lock = threading.Lock()

    def start_research(self, question: str, project_id: str) -> str:
        """Create a research task and spawn a daemon thread to execute it."""
        task_id = self.task_manager.create_task(
            task_type="research",
            metadata={"question": question, "project_id": project_id},
        )
        thread = threading.Thread(
            target=self._research_worker,
            args=(task_id, question),
            daemon=True,
        )
        thread.start()
        return task_id

    def _research_worker(self, task_id: str, question: str):
        """Execute research pipeline in background thread."""
        try:
            # Phase 1: Expanding (0-10%)
            self.task_manager.update_task(
                task_id,
                status=TaskStatus.PROCESSING,
                progress=0,
                message="Expanding queries...",
            )
            queries = self.query_expander.expand(question)
            self.task_manager.update_task(task_id, progress=10, message="Queries expanded")

            # Phase 2: Searching (10-80%)
            all_results = []
            for i, query in enumerate(queries):
                progress = 10 + int((i + 1) / len(queries) * 70) if queries else 80
                self.task_manager.update_task(
                    task_id,
                    progress=progress,
                    message=f"Searching ({i + 1}/{len(queries)}): {query[:50]}...",
                )
                results = self.web_searcher.search(query)
                all_results.extend(results)

            # Phase 3: Organizing (80-100%)
            self.task_manager.update_task(
                task_id, progress=80, message="Organizing evidence..."
            )
            evidence = self.evidence_collector.collect(all_results)

            with self._lock:
                self._evidence_store[task_id] = evidence
                self._queries_store[task_id] = queries

            self.task_manager.complete_task(
                task_id,
                result={
                    "total_count": len(evidence),
                    "queries_used": queries,
                },
            )
        except Exception as e:
            logger.error(f"Research worker failed: {e}")
            self.task_manager.fail_task(task_id, str(e))

    def get_results(self, task_id: str) -> Optional[Dict]:
        """Return evidence results if task is completed."""
        task = self.task_manager.get_task(task_id)
        if not task or task.status != TaskStatus.COMPLETED:
            return None
        with self._lock:
            evidence = self._evidence_store.get(task_id, [])
            queries = self._queries_store.get(task_id, [])
        return {
            "evidence": evidence,
            "total_count": len(evidence),
            "queries_used": queries,
        }

    def get_status(self, task_id: str) -> Optional[Dict]:
        """Return current status of a research task."""
        task = self.task_manager.get_task(task_id)
        if not task:
            return None
        return {
            "status": task.status.value,
            "progress": task.progress,
            "phase": task.message,
            "message": task.message,
        }

    def confirm_and_build_text(
        self, task_id: str, selected_ids: List[int], extra_text: str = ""
    ) -> Optional[Dict]:
        """Build confirmed text from selected evidence items."""
        with self._lock:
            evidence = self._evidence_store.get(task_id, [])
        result = self._build_confirmed_text(evidence, selected_ids, extra_text)

        # DuckDB hook: store selected evidence
        try:
            from .duckdb_store import DuckDBStore
            task = self.task_manager.get_task(task_id)
            project_id = task.metadata.get("project_id", "") if task else ""
            if project_id:
                selected = [e for e in evidence if e["id"] in selected_ids]
                store = DuckDBStore()
                store.store_evidence(project_id, selected)
                store.close()
        except Exception as e:
            logger.warning(f"DuckDB evidence write failed: {e}")

        return result

    @staticmethod
    def _build_confirmed_text(
        evidence: List[Dict], selected_ids: List[int], extra_text: str = ""
    ) -> Dict:
        """Filter evidence by selected_ids and concatenate into text."""
        selected = [e for e in evidence if e.get("id") in selected_ids]
        parts = []
        sources = []
        for e in selected:
            parts.append(f"来源: {e.get('title', '')} ({e.get('url', '')})\n{e.get('snippet', '')}")
            sources.append({"title": e.get("title", ""), "url": e.get("url", "")})

        text = "\n\n".join(parts)

        if extra_text and extra_text.strip():
            text += f"\n\n补充信息:\n{extra_text}"

        return {
            "text": text,
            "metadata": {
                "source_count": len(selected),
                "has_extra_text": bool(extra_text and extra_text.strip()),
                "sources": sources,
            },
        }
