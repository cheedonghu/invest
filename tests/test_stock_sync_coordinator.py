from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from fastapi import HTTPException

from backend.app.services.stock.stock_sync_coordinator import StockSyncCoordinator


class FakeBackupService:
    def __init__(self) -> None:
        self.valuation_calls: list[str] = []
        self.profit_calls: list[str] = []

    def sync_valuation(self, security_code: str) -> dict:
        self.valuation_calls.append(security_code)
        return {"security_code": security_code, "affected_rows": 1}

    def sync_profit(self, security_code: str) -> dict:
        self.profit_calls.append(security_code)
        return {"security_code": security_code, "affected_rows": 1}


class FakeCalendar:
    def __init__(self, stale: bool) -> None:
        self.stale = stale

    def is_stale(self, watermark, today) -> bool:
        return self.stale


class FakeValueRepo:
    def latest_trade_date(self, security_code: str):
        return date(2026, 3, 20)


class FakeProfitRepo:
    def __init__(self, watermark) -> None:
        self.watermark = watermark

    def latest_report_date(self, security_code: str):
        return self.watermark


class FakePeriodProvider:
    def __init__(self, period, error: Exception | None = None) -> None:
        self.period = period
        self.error = error
        self.calls = 0

    def latest_report_period(self, security_code: str):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.period


def build(stale=True, db_watermark=date(2026, 3, 31), upstream=date(2026, 3, 31), probe_error=None):
    backup = FakeBackupService()
    provider = FakePeriodProvider(upstream, probe_error)
    coordinator = StockSyncCoordinator(
        backup_service=backup,
        calendar=FakeCalendar(stale),
        value_repository=FakeValueRepo(),
        profit_repository=FakeProfitRepo(db_watermark),
        period_provider=provider,
    )
    return coordinator, backup, provider


class GatingTestCase(unittest.TestCase):
    def test_fresh_watermark_skips_everything(self) -> None:
        coordinator, backup, provider = build(stale=False)
        coordinator.run_sync("600519")
        self.assertEqual(backup.valuation_calls, [])
        self.assertEqual(provider.calls, 0, "日线新鲜时不应探测报告期")

    def test_stale_watermark_syncs_valuation(self) -> None:
        coordinator, backup, _ = build(stale=True)
        coordinator.run_sync("600519")
        self.assertEqual(backup.valuation_calls, ["600519"])

    def test_same_report_period_does_not_pull_profit(self) -> None:
        """上游报告期与库中水位线相同时绝不能拉全量——本门控的核心价值。"""
        coordinator, backup, provider = build(
            db_watermark=date(2026, 3, 31), upstream=date(2026, 3, 31)
        )
        coordinator.run_sync("600519")
        self.assertEqual(provider.calls, 1)
        self.assertEqual(backup.profit_calls, [], "报告期无变化不应拉取利润表")

    def test_newer_report_period_pulls_profit(self) -> None:
        coordinator, backup, _ = build(
            db_watermark=date(2026, 3, 31), upstream=date(2026, 6, 30)
        )
        coordinator.run_sync("600519")
        self.assertEqual(backup.profit_calls, ["600519"])

    def test_null_watermark_pulls_profit(self) -> None:
        """新股票利润表水位线为 NULL，必须拉取，否则名称永远写不进去。"""
        coordinator, backup, _ = build(db_watermark=None, upstream=date(2026, 3, 31))
        coordinator.run_sync("600519")
        self.assertEqual(backup.profit_calls, ["600519"])

    def test_probe_failure_skips_profit_but_keeps_valuation(self) -> None:
        coordinator, backup, _ = build(probe_error=RuntimeError("upstream down"))
        coordinator.run_sync("600519")
        self.assertEqual(backup.valuation_calls, ["600519"])
        self.assertEqual(backup.profit_calls, [], "探测失败时不应盲拉")


class InFlightTestCase(unittest.TestCase):
    def test_force_sync_releases_slot(self) -> None:
        coordinator, backup, _ = build()
        coordinator.force_sync_valuation("600519")
        coordinator.force_sync_valuation("600519")
        self.assertEqual(backup.valuation_calls, ["600519", "600519"])

    def test_force_sync_conflicts_with_held_slot(self) -> None:
        coordinator, _, _ = build()
        self.assertTrue(coordinator._acquire("600519"))
        with self.assertRaises(HTTPException) as ctx:
            coordinator.force_sync_valuation("600519")
        self.assertEqual(ctx.exception.status_code, 409)
        coordinator._release("600519")

    def test_force_sync_skips_gating(self) -> None:
        """手工接口即使数据新鲜也必须真实拉取。"""
        coordinator, backup, _ = build(stale=False)
        coordinator.force_sync_profit("600519")
        self.assertEqual(backup.profit_calls, ["600519"])

    def test_slot_released_after_failure(self) -> None:
        coordinator, backup, _ = build()

        def boom(security_code: str) -> dict:
            raise RuntimeError("sync exploded")

        backup.sync_valuation = boom
        with self.assertRaises(RuntimeError):
            coordinator.force_sync_valuation("600519")
        self.assertTrue(coordinator._acquire("600519"), "异常后槽位必须已释放")
        coordinator._release("600519")


if __name__ == "__main__":
    unittest.main()
