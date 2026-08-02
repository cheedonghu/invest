from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.app.repositories.stock_value_daily_repository import StockValueDailyRepository

TEST_CODE = "000002"


def _row(trade_date: date, close_price: float) -> dict:
    return {
        "security_code": TEST_CODE,
        "trade_date": trade_date,
        "close_price": close_price,
        "pct_change": 1.5,
        "total_market_value": 1000.0,
        "float_market_value": 900.0,
        "total_shares": 100,
        "float_shares": 90,
        "pe_ttm": 10.0,
        "pe_static": 11.0,
        "pb": 1.2,
        "peg": 0.8,
        "pcf": 5.0,
        "ps": 2.0,
    }


class StockValueDailyUpsertTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = StockValueDailyRepository()
        if not self.repository.is_available:
            self.skipTest("database settings are not configured")
        self._cleanup()

    def tearDown(self) -> None:
        self._cleanup()

    def _cleanup(self) -> None:
        with self.repository.connect() as connection:
            cursor = connection.cursor()
            cursor.execute("DELETE FROM stock_value_daily WHERE security_code = %s", [TEST_CODE])
            connection.commit()

    def _upsert(self, rows: list[dict]) -> int:
        with self.repository.connect() as connection:
            cursor = connection.cursor()
            affected = self.repository.upsert_rows(cursor, TEST_CODE, rows)
            connection.commit()
        return affected

    def test_upsert_is_idempotent(self) -> None:
        rows = [_row(date(2026, 7, 30), 10.0), _row(date(2026, 7, 31), 11.0)]
        self._upsert(rows)
        self.assertEqual(self._upsert(rows), 0, "重复写入相同数据不应产生变更")

    def test_upsert_updates_changed_value(self) -> None:
        self._upsert([_row(date(2026, 7, 31), 10.0)])
        self._upsert([_row(date(2026, 7, 31), 12.5)])
        df = self.repository.fetch_by_security_code(TEST_CODE)
        self.assertEqual(len(df), 1, "同一交易日不应产生重复行")
        self.assertAlmostEqual(float(df.iloc[0]["close_price"]), 12.5, places=4)

    def test_upsert_appends_new_dates_only(self) -> None:
        self._upsert([_row(date(2026, 7, 30), 10.0)])
        self._upsert([_row(date(2026, 7, 30), 10.0), _row(date(2026, 7, 31), 11.0)])
        self.assertEqual(len(self.repository.fetch_by_security_code(TEST_CODE)), 2)

    def test_latest_trade_date(self) -> None:
        self.assertIsNone(self.repository.latest_trade_date(TEST_CODE))
        self._upsert([_row(date(2026, 7, 30), 10.0), _row(date(2026, 7, 31), 11.0)])
        self.assertEqual(self.repository.latest_trade_date(TEST_CODE), date(2026, 7, 31))


if __name__ == "__main__":
    unittest.main()
