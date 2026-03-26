"""
DuckDB 本地存储
将图谱数据、Agent 信息、交互记录和报告持久化到 DuckDB
"""

import json
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import duckdb

from ..utils.logger import get_logger

logger = get_logger('mirofish.duckdb')

DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/bibibo_graph.db')


class DuckDBStore:
    """DuckDB local storage for graph data, agents, interactions, and reports."""

    _TARGET_AGENT_KEYS: Dict[str, str] = {
        "CREATE_COMMENT": "post_author_name",
        "LIKE_POST": "post_author_name",
        "DISLIKE_POST": "post_author_name",
        "LIKE_COMMENT": "comment_author_name",
        "DISLIKE_COMMENT": "comment_author_name",
        "REPOST": "original_author_name",
        "QUOTE_POST": "original_author_name",
        "FOLLOW": "target_user_name",
        "MUTE": "target_user_name",
    }

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or DEFAULT_DB_PATH
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_tables()

    def _init_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS evidence (
                id VARCHAR PRIMARY KEY,
                project_id VARCHAR,
                content TEXT,
                source VARCHAR,
                created_at TIMESTAMP DEFAULT current_timestamp
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS entities (
                id VARCHAR PRIMARY KEY,
                project_id VARCHAR,
                name VARCHAR,
                type VARCHAR,
                attributes TEXT,
                created_at TIMESTAMP DEFAULT current_timestamp
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS agents (
                id VARCHAR PRIMARY KEY,
                project_id VARCHAR,
                user_id VARCHAR,
                name VARCHAR,
                persona TEXT,
                attributes TEXT,
                created_at TIMESTAMP DEFAULT current_timestamp
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS interactions (
                id VARCHAR PRIMARY KEY,
                project_id VARCHAR,
                agent_name VARCHAR,
                action_type VARCHAR,
                target_agent VARCHAR,
                content TEXT,
                created_at TIMESTAMP DEFAULT current_timestamp
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                id VARCHAR PRIMARY KEY,
                project_id VARCHAR,
                report_text TEXT,
                created_at TIMESTAMP DEFAULT current_timestamp
            )
        """)

    def query(self, sql: str, params: Optional[list] = None) -> List[Dict[str, Any]]:
        if params:
            result = self.conn.execute(sql, params)
        else:
            result = self.conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    # ---- Evidence ----

    def store_evidence(self, project_id: str, evidence_list: List[Dict[str, Any]]):
        if not evidence_list:
            return
        prefix = project_id[:8]
        for idx, ev in enumerate(evidence_list):
            ev_id = f"ev_{prefix}_{idx}"
            self.conn.execute(
                "INSERT INTO evidence (id, project_id, content, source) VALUES (?, ?, ?, ?)",
                [ev_id, project_id, ev.get("content", ""), ev.get("source", "")],
            )

    # ---- Entities ----

    def store_entities(self, project_id: str, entities: List[Any]):
        if not entities:
            return
        for ent in entities:
            if hasattr(ent, "to_dict"):
                d = ent.to_dict()
            else:
                d = ent

            # Extract type from labels, skipping generic ones
            labels = d.get("labels", [])
            entity_type = ""
            for label in labels:
                if label not in ("Entity", "Node"):
                    entity_type = label
                    break

            ent_id = d.get("uuid", uuid.uuid4().hex)
            attributes = d.get("attributes", {})
            if isinstance(attributes, dict):
                attributes = json.dumps(attributes, ensure_ascii=False)

            self.conn.execute(
                "INSERT INTO entities (id, project_id, name, type, attributes) VALUES (?, ?, ?, ?, ?)",
                [ent_id, project_id, d.get("name", ""), entity_type, attributes],
            )

    # ---- Agents ----

    def store_agents(self, project_id: str, profiles: List[Any]):
        if not profiles:
            return
        prefix = project_id[:8]
        for profile in profiles:
            if hasattr(profile, "to_reddit_format"):
                d = profile.to_reddit_format()
            elif isinstance(profile, dict):
                d = profile
            else:
                continue

            user_id = d.get("user_id", uuid.uuid4().hex)
            ag_id = f"ag_{prefix}_{user_id}"
            attributes = {k: v for k, v in d.items() if k not in ("user_id", "name", "persona")}
            self.conn.execute(
                "INSERT INTO agents (id, project_id, user_id, name, persona, attributes) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ag_id,
                    project_id,
                    str(user_id),
                    d.get("name", ""),
                    d.get("persona", ""),
                    json.dumps(attributes, ensure_ascii=False),
                ],
            )

    # ---- Interactions ----

    def store_interactions(self, project_id: str, sim_dir: str):
        action_files = []
        for platform in ("twitter", "reddit"):
            path = os.path.join(sim_dir, platform, "actions.jsonl")
            if os.path.isfile(path):
                action_files.append(path)

        for path in action_files:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        action = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    action_type = action.get("action_type", "")
                    if action_type in ("DO_NOTHING", "simulation_end"):
                        continue

                    args = action.get("action_args", {}) or {}
                    target_key = self._TARGET_AGENT_KEYS.get(action_type)
                    target_agent = args.get(target_key, "") if target_key else ""

                    content = args.get("content") or args.get("post_content") or args.get("original_content") or ""

                    ix_id = f"ix_{uuid.uuid4().hex[:12]}"
                    self.conn.execute(
                        "INSERT INTO interactions (id, project_id, agent_name, action_type, target_agent, content) VALUES (?, ?, ?, ?, ?, ?)",
                        [
                            ix_id,
                            project_id,
                            action.get("agent_name", ""),
                            action_type,
                            target_agent,
                            content,
                        ],
                    )

    # ---- Reports ----

    def store_report(self, project_id: str, report_text: str):
        prefix = project_id[:8]
        rp_id = f"rp_{prefix}_{uuid.uuid4().hex[:8]}"
        self.conn.execute(
            "INSERT INTO reports (id, project_id, report_text) VALUES (?, ?, ?)",
            [rp_id, project_id, report_text],
        )

    # ---- Export to PuppyGraph ----

    def export_to_puppygraph(self, project_id: str):
        from .puppygraph_client import PuppyGraphClient

        client = PuppyGraphClient()
        if not client.base_url:
            return

        db_name = f"bibibo_{project_id[:16]}"
        tables = ["evidence", "entities", "agents", "interactions", "reports"]

        for table in tables:
            rows = self.query(f"SELECT * FROM {table} WHERE project_id = ?", [project_id])
            if not rows:
                continue
            lines = []
            for row in rows:
                clean = {}
                for k, v in row.items():
                    if isinstance(v, datetime):
                        clean[k] = v.isoformat()
                    else:
                        clean[k] = v
                lines.append(json.dumps(clean, ensure_ascii=False))
            jsonl_data = "\n".join(lines)
            client.upload_jsonl(db_name, jsonl_data)

        client.build_graph(db_name)
