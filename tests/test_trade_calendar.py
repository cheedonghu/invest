from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.app.services.stock.trade_calendar import TradeCalendar, latest_workday


class LatestWorkdayTestCase(unittest.TestCase):
    def test_weekday_returns_itself(self) -> None:
        # 2026-07-27 周一 ~ 2026-07-31 周五
        for day in range(27, 32):
            with self.subTest(day=day):
                self.assertEqual(latest_workday(date(2026, 7, day)), date(2026, 7, day))

    def test_saturday_falls_back_to_friday(self) -> None:
        self.assertEqual(latest_workday(date(2026, 8, 1)), date(2026, 7, 31))

    def test_sunday_falls_back_to_friday(self) -> None:
        self.assertEqual(latest_workday(date(2026, 8, 2)), date(2026, 7, 31))


class IsStaleTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.calls = 0

        def loader() -> list[date]:
            self.calls += 1
            # 2026-07-27 ~ 07-31 为交易日，08-01/02 为周末
            return [date(2026, 7, d) for d in (27, 28, 29, 30, 31)]

        self.calendar = TradeCalendar(loader=loader)

    def test_no_watermark_is_stale(self) -> None:
        self.assertTrue(self.calendar.is_stale(None, date(2026, 8, 2)))

    def test_watermark_at_latest_workday_is_fresh(self) -> None:
        self.assertFalse(self.calendar.is_stale(date(2026, 7, 31), date(2026, 8, 2)))

    def test_first_gate_pass_does_not_load_calendar(self) -> None:
        """一级放行时绝不能触碰日历——这是两级门控的核心价值。"""
        self.calendar.is_stale(date(2026, 7, 31), date(2026, 8, 2))
        self.assertEqual(self.calls, 0)

    def test_stale_watermark_triggers_second_gate(self) -> None:
        self.assertTrue(self.calendar.is_stale(date(2026, 3, 20), date(2026, 8, 2)))
        self.assertEqual(self.calls, 1)

    def test_holiday_window_is_not_stale(self) -> None:
        """8/3 周一若为假日（不在交易日历中），水位线 7/31 仍应判为新鲜。"""
        self.assertFalse(self.calendar.is_stale(date(2026, 7, 31), date(2026, 8, 3)))

    def test_calendar_failure_degrades_to_first_gate(self) -> None:
        def boom() -> list[date]:
            raise RuntimeError("calendar source down")

        calendar = TradeCalendar(loader=boom)
        # 一级已判陈旧，日历不可用时保持陈旧结论（宁可多拉一次）
        self.assertTrue(calendar.is_stale(date(2026, 3, 20), date(2026, 8, 2)))

    def test_calendar_loaded_once_per_day(self) -> None:
        self.calendar.is_stale(date(2026, 3, 20), date(2026, 8, 2))
        self.calendar.is_stale(date(2026, 3, 21), date(2026, 8, 2))
        self.assertEqual(self.calls, 1)


if __name__ == "__main__":
    unittest.main()
