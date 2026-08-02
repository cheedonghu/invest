from __future__ import annotations

import logging
from typing import Any

from backend.app.services.stock.marked_stock_service import MarkedStockService


logger = logging.getLogger(__name__)


class FetchMarkedStockTool:
    """获取用户已标记（感兴趣）的股票列表。

    数据统一走 MarkedStockService -> StockInfoRepository -> DatabaseSessionFactory，
    连接参数由 backend/app/config/config.toml 提供，不在此处直连数据库。
    """

    name = "fetch_marked_stock"
    description = "获取用户已标记（感兴趣）的股票，返回股票代码、名称和标记原因"

    def __init__(self, service: MarkedStockService | None = None) -> None:
        self._service = service

    @property
    def service(self) -> MarkedStockService:
        # 延迟创建，避免导入本模块时就建立数据库连接池
        if self._service is None:
            self._service = MarkedStockService()
        return self._service

    def fetch(self) -> list[dict[str, Any]]:
        return self.service.list_marked_stocks()["items"]

    def run(self, query: str = "") -> str:
        """供 Agent 调用的入口，返回给模型阅读的文本。query 暂未使用。"""
        try:
            items = self.fetch()
        except Exception as exc:
            logger.warning("fetch marked stocks failed: %s", exc)
            return f"获取标记股票失败：{exc}"

        if not items:
            return "当前没有已标记的股票。"

        lines = [f"共 {len(items)} 只已标记股票："]
        for item in items:
            lines.append(f"- {item['symbol']} {item['name']}：{item['mark_reason']}")
        return "\n".join(lines)
