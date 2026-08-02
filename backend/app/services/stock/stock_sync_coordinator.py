from __future__ import annotations

import logging
import threading
from datetime import date
from typing import Any

from fastapi import HTTPException

from backend.app.providers.profit_sheet.eastmoney_report_period_provider import (
    EastmoneyReportPeriodProvider,
)
from backend.app.repositories.stock_profit_sheet_repository import StockProfitSheetRepository
from backend.app.repositories.stock_value_daily_repository import StockValueDailyRepository
from backend.app.services.stock.stock_backup_service import StockBackupService
from backend.app.services.stock.trade_calendar import TradeCalendar


logger = logging.getLogger(__name__)


class StockSyncCoordinator:
    """新鲜度门控 + in-flight 去重 + 后台线程调度。

    请求线程内不做任何 I/O：先占 in-flight 槽位（纯内存操作），
    再把水位线查询与门控判定全部交给后台线程，避免拖慢接口响应。
    """

    def __init__(
        self,
        backup_service: StockBackupService | None = None,
        calendar: TradeCalendar | None = None,
        value_repository: StockValueDailyRepository | None = None,
        profit_repository: StockProfitSheetRepository | None = None,
        period_provider: EastmoneyReportPeriodProvider | None = None,
    ) -> None:
        self.backup_service = backup_service or StockBackupService()
        self.calendar = calendar or TradeCalendar()
        self.value_repository = value_repository or StockValueDailyRepository()
        self.profit_repository = profit_repository or StockProfitSheetRepository()
        self.period_provider = period_provider or EastmoneyReportPeriodProvider()
        self._in_flight: set[str] = set()
        self._lock = threading.Lock()

    # ---- in-flight 去重 ----

    def _acquire(self, security_code: str) -> bool:
        with self._lock:
            if security_code in self._in_flight:
                return False
            self._in_flight.add(security_code)
            return True

    def _release(self, security_code: str) -> None:
        with self._lock:
            self._in_flight.discard(security_code)

    # ---- 惰性触发 ----

    def maybe_sync(self, security_code: str) -> None:
        if not self._acquire(security_code):
            logger.info("Sync already in flight for %s, skip", security_code)
            return

        worker = threading.Thread(
            target=self._run_and_release,
            args=(security_code,),
            name=f"stock-sync-{security_code}",
            daemon=True,
        )
        try:
            worker.start()
        except Exception:
            self._release(security_code)
            raise

    def _run_and_release(self, security_code: str) -> None:
        try:
            self.run_sync(security_code)
        except Exception:
            logger.exception("Background sync failed for %s", security_code)
        finally:
            self._release(security_code)

    def run_sync(self, security_code: str) -> None:
        """门控 + 拉取。同步执行，不涉及 in-flight 槽位管理。"""
        today = date.today()
        watermark = self.value_repository.latest_trade_date(security_code)
        if not self.calendar.is_stale(watermark, today):
            logger.info("Valuation is fresh for %s (watermark=%s), skip sync", security_code, watermark)
            return

        self.backup_service.sync_valuation(security_code)
        self._sync_profit_if_new_report(security_code)

    def _sync_profit_if_new_report(self, security_code: str) -> None:
        try:
            upstream = self.period_provider.latest_report_period(security_code)
        except Exception as exc:
            # 上游出问题时再猛拉是火上浇油，跳过本次，下个交易日重试
            logger.warning("Report period probe failed for %s, skip profit sync: %s", security_code, exc)
            return

        if upstream is None:
            logger.warning("Report period probe returned nothing for %s, skip profit sync", security_code)
            return

        current = self.profit_repository.latest_report_date(security_code)
        if current is not None and upstream <= current:
            logger.info(
                "Profit sheet is fresh for %s (db=%s upstream=%s), skip pull",
                security_code, current, upstream,
            )
            return

        self.backup_service.sync_profit(security_code)

    # ---- 手工运维：跳过门控，保留并发保护 ----

    def force_sync_valuation(self, security_code: str) -> dict[str, Any]:
        return self._force(security_code, self.backup_service.sync_valuation)

    def force_sync_profit(self, security_code: str) -> dict[str, Any]:
        return self._force(security_code, self.backup_service.sync_profit)

    def _force(self, security_code: str, action) -> dict[str, Any]:
        if not self._acquire(security_code):
            raise HTTPException(
                status_code=409,
                detail=f"sync already in progress for {security_code}, retry later",
            )
        try:
            return action(security_code)
        finally:
            self._release(security_code)
