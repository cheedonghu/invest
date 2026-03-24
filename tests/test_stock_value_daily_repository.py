from __future__ import annotations

import unittest
import sys
import akshare as ak
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.app.repositories.stock_value_daily_repository import StockValueDailyRepository


class StockValueDailyRepositoryTestCase(unittest.TestCase):
    def test_fetch_one_sample(self) -> None:
        repository = StockValueDailyRepository()
        self.assertTrue(repository.is_available, "Database settings are not configured")

        sample = repository.fetch_one_sample()
        self.assertIsNotNone(sample, "stock_value_daily has no rows")
        self.assertIn("symbol", sample)
        self.assertIn("trade_date", sample)

        print("database sample row:", sample)

    def test_profit_sheet_to_csv(self) -> None:
        stock_profit_sheet_by_report_em_df = ak.stock_profit_sheet_by_report_em(symbol="SH600519")
        
        output_dir = Path(__file__).parent / "output"
        output_dir.mkdir(exist_ok=True)
        
        csv_path = output_dir / "profit_sheet_SH600519.csv"
        stock_profit_sheet_by_report_em_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        self.assertTrue(csv_path.exists())
        print(f"Data saved to: {csv_path}")
        print(stock_profit_sheet_by_report_em_df)



if __name__ == "__main__":
    unittest.main()
