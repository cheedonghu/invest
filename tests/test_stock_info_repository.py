from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.app.repositories.stock_info_repository import StockInfoRepository

TEST_CODE = "000001"


class StockInfoRepositoryTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = StockInfoRepository()
        if not self.repository.is_available:
            self.skipTest("database settings are not configured")
        self._cleanup()

    def tearDown(self) -> None:
        self._cleanup()

    def _cleanup(self) -> None:
        with self.repository.connect() as connection:
            cursor = connection.cursor()
            cursor.execute("DELETE FROM stock_info WHERE security_code = %s", [TEST_CODE])
            connection.commit()

    def test_fetch_missing_returns_none(self) -> None:
        self.assertIsNone(self.repository.fetch_by_code(TEST_CODE))

    def test_upsert_name_then_fetch(self) -> None:
        self.repository.upsert_name(TEST_CODE, "平安银行")
        row = self.repository.fetch_by_code(TEST_CODE)
        self.assertIsNotNone(row)
        self.assertEqual(row["security_name_abbr"], "平安银行")
        self.assertIsNone(row["mark_reason"])

    def test_upsert_mark_reason_keeps_existing_name(self) -> None:
        self.repository.upsert_name(TEST_CODE, "平安银行")
        self.repository.upsert_mark_reason(TEST_CODE, "银行股观察")
        row = self.repository.fetch_by_code(TEST_CODE)
        self.assertEqual(row["security_name_abbr"], "平安银行")
        self.assertEqual(row["mark_reason"], "银行股观察")

    def test_upsert_name_keeps_existing_mark_reason(self) -> None:
        self.repository.upsert_mark_reason(TEST_CODE, "银行股观察")
        self.repository.upsert_name(TEST_CODE, "平安银行")
        row = self.repository.fetch_by_code(TEST_CODE)
        self.assertEqual(row["mark_reason"], "银行股观察")
        self.assertEqual(row["security_name_abbr"], "平安银行")

    def test_mark_without_prior_data_has_null_name(self) -> None:
        """标记一只从未拉过数据的股票，名称应为 NULL 等待回填。"""
        self.repository.upsert_mark_reason(TEST_CODE, "先标记再拉数据")
        row = self.repository.fetch_by_code(TEST_CODE)
        self.assertIsNone(row["security_name_abbr"])

    def test_fetch_marked_only_returns_marked(self) -> None:
        self.repository.upsert_name(TEST_CODE, "平安银行")
        codes = [item["security_code"] for item in self.repository.fetch_marked()]
        self.assertNotIn(TEST_CODE, codes)

        self.repository.upsert_mark_reason(TEST_CODE, "银行股观察")
        codes = [item["security_code"] for item in self.repository.fetch_marked()]
        self.assertIn(TEST_CODE, codes)


if __name__ == "__main__":
    unittest.main()
