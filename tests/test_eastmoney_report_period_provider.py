from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.app.providers.profit_sheet.eastmoney_report_period_provider import (
    EastmoneyReportPeriodProvider,
)


class SymbolConversionTestCase(unittest.TestCase):
    def test_shanghai_prefix(self) -> None:
        self.assertEqual(EastmoneyReportPeriodProvider.to_eastmoney_symbol("600519"), "SH600519")

    def test_shenzhen_prefix(self) -> None:
        self.assertEqual(EastmoneyReportPeriodProvider.to_eastmoney_symbol("300308"), "SZ300308")


class ParseTestCase(unittest.TestCase):
    def test_picks_max_report_date(self) -> None:
        payload = {"data": [
            {"REPORT_DATE": "2025-12-31 00:00:00"},
            {"REPORT_DATE": "2026-03-31 00:00:00"},
            {"REPORT_DATE": "2025-09-30 00:00:00"},
        ]}
        self.assertEqual(
            EastmoneyReportPeriodProvider.parse_latest(payload), date(2026, 3, 31)
        )

    def test_empty_payload_returns_none(self) -> None:
        self.assertIsNone(EastmoneyReportPeriodProvider.parse_latest({"data": []}))

    def test_missing_data_key_returns_none(self) -> None:
        self.assertIsNone(EastmoneyReportPeriodProvider.parse_latest({}))

    def test_unparsable_dates_return_none(self) -> None:
        payload = {"data": [{"REPORT_DATE": "not-a-date"}]}
        self.assertIsNone(EastmoneyReportPeriodProvider.parse_latest(payload))


class LiveFetchTestCase(unittest.TestCase):
    """真实网络调用。上游不可用时跳过而非失败。"""

    def test_fetch_returns_a_date(self) -> None:
        provider = EastmoneyReportPeriodProvider()
        try:
            result = provider.latest_report_period("600519")
        except Exception as exc:
            self.skipTest(f"upstream unavailable: {exc}")
        self.assertIsInstance(result, date)
        self.assertGreater(result, date(2020, 1, 1))


if __name__ == "__main__":
    unittest.main()
