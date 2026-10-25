from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from fastapi import HTTPException

from backend.app.repositories.stock_info_repository import StockInfoRepository
from backend.app.services.stock.stock_sync_coordinator import StockSyncCoordinator, get_default_coordinator


class MarkedStockService:
    MAX_NOTE_LENGTH = 100_000

    def __init__(
        self,
        repository: StockInfoRepository | None = None,
        coordinator: StockSyncCoordinator | None = None,
    ) -> None:
        self.repository = repository or StockInfoRepository()
        self.coordinator = coordinator or get_default_coordinator()

    def list_marked_stocks(self) -> dict[str, Any]:
        self._ensure_database_available()
        items = [self._serialize_row(row) for row in self.repository.fetch_marked()]
        return {"count": len(items), "items": items}

    def upsert_marked_stock(self, symbol: str, mark_reason: str) -> dict[str, Any]:
        self._ensure_database_available()
        security_code = self.normalize_symbol(symbol)
        normalized_reason = self.normalize_reason(mark_reason)

        self.repository.upsert_mark_reason(security_code, normalized_reason)

        # 名称是备份流程的副产品，本层不接受也不写任何调用方传入的名称。
        # 标记后触发一次惰性同步，名称会随利润表一并落库。
        self.coordinator.maybe_sync(security_code)

        row = self.repository.fetch_by_code(security_code)
        if row is None:
            raise HTTPException(status_code=500, detail="marked stock was not persisted")
        return self._serialize_row(row)

    def delete_marked_stock(self, symbol: str) -> dict[str, str]:
        self._ensure_database_available()
        security_code = self.normalize_symbol(symbol)
        # 幂等清除备注，不删除共享的 stock_info 记录，也不触发行情同步。
        self.repository.clear_mark_reason(security_code)
        return {"symbol": security_code}

    def _ensure_database_available(self) -> None:
        if not self.repository.is_available:
            raise HTTPException(status_code=503, detail="database settings are not configured")

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        cleaned = re.sub(r"[^0-9]", "", symbol or "")
        if len(cleaned) != 6:
            raise HTTPException(status_code=400, detail="symbol must be a 6-digit A-share stock code")
        return cleaned

    @classmethod
    def normalize_reason(cls, reason: str) -> str:
        if not reason or not reason.strip():
            raise HTTPException(status_code=400, detail="备注不能为空")
        if len(reason) > cls.MAX_NOTE_LENGTH:
            raise HTTPException(status_code=400, detail="备注最多支持 100000 个字符")
        # 缩进代码块和行尾双空格都是 Markdown 语法，保存原文。
        return reason

    @classmethod
    def _serialize_row(cls, row: dict[str, Any]) -> dict[str, Any]:
        security_code = str(row.get("security_code") or "")
        name = str(row.get("security_name_abbr") or "").strip()
        return {
            "symbol": security_code,
            # 名称缺失时临时回退为代号供前端展示，不写回数据库，
            # 待备份流程补齐后自然恢复。
            "name": name or security_code,
            "mark_reason": str(row.get("mark_reason") or ""),
            "created_at": cls._format_datetime(row.get("created_at")),
            "updated_at": cls._format_datetime(row.get("updated_at")),
        }

    @staticmethod
    def _format_datetime(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, date):
            return value.strftime("%Y-%m-%d")
        text = str(value).strip()
        return text or None
