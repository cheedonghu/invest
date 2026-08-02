from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.app.repositories.stock_profit_sheet_repository import StockProfitSheetRepository

TEST_CODE = "000003"


def _row(report_date: date, netprofit: float) -> dict:
    return {
        "security_code": TEST_CODE,
        "security_name_abbr": "测试股",
        "report_date": report_date,
        "report_type": "年报",
        "report_date_name": "2025年报",
        "total_operate_income": 1000.0,
        "netprofit": netprofit,
        "basic_eps": 1.0,
        "total_operate_cost": 800.0,
        "opinion_type": "标准无保留意见",
    }


class StockProfitSheetRepositoryTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = StockProfitSheetRepository()
        if not self.repository.is_available:
            self.skipTest("database settings are not configured")
        self._cleanup()

    def tearDown(self) -> None:
        self._cleanup()

    def _cleanup(self) -> None:
        with self.repository.connect() as connection:
            cursor = connection.cursor()
            cursor.execute("DELETE FROM stock_profit_sheet WHERE security_code = %s", [TEST_CODE])
            connection.commit()

    def _upsert(self, rows: list[dict]) -> int:
        with self.repository.connect() as connection:
            cursor = connection.cursor()
            affected = self.repository.upsert_rows(cursor, TEST_CODE, rows)
            connection.commit()
        return affected

    def test_latest_report_date_none_when_empty(self) -> None:
        self.assertIsNone(self.repository.latest_report_date(TEST_CODE))

    def test_latest_report_date(self) -> None:
        self._upsert([_row(date(2025, 12, 31), 100.0), _row(date(2026, 3, 31), 30.0)])
        self.assertEqual(self.repository.latest_report_date(TEST_CODE), date(2026, 3, 31))

    def test_upsert_is_idempotent(self) -> None:
        rows = [_row(date(2026, 3, 31), 30.0)]
        self._upsert(rows)
        self.assertEqual(self._upsert(rows), 0, "重复写入相同数据不应产生变更")

    def test_upsert_updates_changed_value(self) -> None:
        self._upsert([_row(date(2026, 3, 31), 30.0)])
        self._upsert([_row(date(2026, 3, 31), 45.0)])
        df = self.repository.fetch_by_security_code(TEST_CODE)
        self.assertEqual(len(df), 1)
        self.assertAlmostEqual(float(df.iloc[0]["netprofit"]), 45.0, places=2)


if __name__ == "__main__":
    unittest.main()
