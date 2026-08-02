from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Any

from fastapi import HTTPException

from backend.app.providers.interfaces.stock_provider import StockNameProvider
from backend.app.providers.name.eastmoney_individual_info_name_provider import EastmoneyIndividualInfoNameProvider
from backend.app.providers.name.xueqiu_individual_info_name_provider import XueqiuIndividualInfoNameProvider
from backend.app.repositories.stock_marked_repository import StockMarkedRepository


logger = logging.getLogger(__name__)


class MarkedStockService:
    def __init__(self, repository: StockMarkedRepository | None = None) -> None:
        self.repository = repository or StockMarkedRepository()
        self.name_providers: list[StockNameProvider] = [
            XueqiuIndividualInfoNameProvider(),
            EastmoneyIndividualInfoNameProvider(),
        ]

    def list_marked_stocks(self) -> dict[str, Any]:
        self._ensure_database_available()
        items = [self._serialize_row(row) for row in self.repository.fetch_all()]
        return {
            "count": len(items),
            "items": items,
        }

    def upsert_marked_stock(self, symbol: str, mark_reason: str, name: str | None = None) -> dict[str, Any]:
        self._ensure_database_available()
        normalized_symbol = self.normalize_symbol(symbol)
        normalized_reason = self.normalize_reason(mark_reason)
        resolved_name = self._resolve_stock_name(normalized_symbol, name=name)

        self.repository.upsert(
            symbol=normalized_symbol,
            name=resolved_name,
            mark_reason=normalized_reason,
        )
        row = self.repository.fetch_by_symbol(normalized_symbol)
        if row is None:
            raise HTTPException(status_code=500, detail="marked stock was not persisted")
        return self._serialize_row(row)

    def _resolve_stock_name(self, symbol: str, name: str | None = None) -> str:
        normalized_name = (name or "").strip()
        if normalized_name:
            return normalized_name

        existing = self.repository.fetch_by_symbol(symbol)
        if existing and str(existing.get("name") or "").strip():
            return str(existing["name"]).strip()

        for provider in self.name_providers:
            try:
                return provider.fetch(symbol)
            except Exception as exc:
                logger.warning(
                    "Marked stock name provider %s failed for symbol=%s: %s",
                    getattr(provider, "source_name", provider.__class__.__name__),
                    symbol,
                    exc,
                )
        return symbol

    def _ensure_database_available(self) -> None:
        if not self.repository.is_available:
            raise HTTPException(status_code=503, detail="database settings are not configured")

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        cleaned = re.sub(r"[^0-9]", "", symbol or "")
        if len(cleaned) != 6:
            raise HTTPException(status_code=400, detail="symbol must be a 6-digit A-share stock code")
        return cleaned

    @staticmethod
    def normalize_reason(reason: str) -> str:
        cleaned = (reason or "").strip()
        if not cleaned:
            raise HTTPException(status_code=400, detail="mark_reason must not be empty")
        return cleaned

    @classmethod
    def _serialize_row(cls, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "symbol": str(row.get("symbol") or ""),
            "name": str(row.get("name") or ""),
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
