"""
PuppyGraph Data API 客户端
通过 REST API 与共享 PuppyGraph 服务交互
"""

import requests
from typing import Optional

from ..config import Config
from ..utils.logger import get_logger

logger = get_logger('mirofish.puppygraph')


class PuppyGraphClient:
    """PuppyGraph Data API 客户端。base_url 为空时所有方法为 no-op。"""

    def __init__(self, base_url: Optional[str] = None):
        self.base_url = base_url or Config.PUPPYGRAPH_API_URL

    def upload_jsonl(self, db: str, data: str) -> Optional[dict]:
        if not self.base_url:
            return None
        try:
            resp = requests.post(
                f"{self.base_url}/api/upload-jsonl",
                json={"db": db, "data": data},
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"PuppyGraph upload failed [{db}]: {e}")
            return None

    def build_graph(self, db: str) -> Optional[dict]:
        if not self.base_url:
            return None
        try:
            resp = requests.post(
                f"{self.base_url}/api/build-graph",
                json={"db": db},
                timeout=60,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"PuppyGraph build-graph failed [{db}]: {e}")
            return None

    def query(self, db: str, sql: str) -> Optional[dict]:
        if not self.base_url:
            return None
        try:
            resp = requests.post(
                f"{self.base_url}/api/query",
                json={"db": db, "sql": sql},
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"PuppyGraph query failed [{db}]: {e}")
            return None
