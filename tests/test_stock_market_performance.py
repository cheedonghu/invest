from __future__ import annotations

import unittest

import pandas as pd

from backend.app.services.stock.stock_service import StockService


class MarketPerformanceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.service = StockService.__new__(StockService)
        self.valuation = pd.DataFrame({
            "date": ["2024-12-31", "2025-03-28", "2025-04-01", "2025-05-06"],
            "market_cap": [100e8, 120e8, 130e8, 140e8],
        })
        self.profit = pd.DataFrame({
            "REPORT_DATE": ["2023-03-31", "2023-12-31", "2024-03-31", "2024-12-31", "2025-03-31"],
            "TOTAL_OPERATE_INCOME": [20e8, 100e8, 30e8, 150e8, 45e8],
            "NETPROFIT": [2e8, 10e8, 3e8, 15e8, -1e8],
            "PARENT_NETPROFIT": [1e8, 8e8, 2e8, 12e8, -2e8],
        })

    def normalize(self, profit=None, valuation=None):
        return self.service._normalize_market_performance(
            self.valuation if valuation is None else valuation,
            self.profit if profit is None else profit,
            "600036", 10,
        )

    def test_ttm_uses_previous_annual_and_same_quarter(self):
        latest = self.normalize()["latest"]
        self.assertEqual(latest["revenue"], 165)
        self.assertEqual(latest["net_profit"], 11)

    def test_quarter_market_cap_never_uses_future_trade(self):
        result = self.normalize()
        self.assertEqual(result["latest"]["market_cap"], 120)
        self.assertEqual(result["latest"]["market_cap_date"], "2025-03-28")
        self.assertEqual(result["latest"]["report_date"], "2025-03-31")
        self.assertEqual(result["latest_market_cap"], {"date": "2025-05-06", "value": 140})

    def test_cache_and_live_profit_use_same_basis(self):
        cached = self.profit.drop(columns="PARENT_NETPROFIT").rename(columns=str.lower)
        self.assertEqual(self.normalize()["series"], self.normalize(cached)["series"])

    def test_bank_revenue_falls_back_per_row(self):
        profit = self.profit.assign(OPERATE_INCOME=self.profit["TOTAL_OPERATE_INCOME"])
        profit.loc[profit.index[-1], "TOTAL_OPERATE_INCOME"] = None
        self.assertEqual(self.normalize(profit)["latest"]["revenue"], 165)

    def test_missing_history_is_not_annualized(self):
        profit = self.profit.loc[self.profit["REPORT_DATE"] != "2024-03-31"]
        self.assertEqual(self.normalize(profit)["latest"]["report_date"], "2024-12-31")

    def test_negative_ttm_profit_is_preserved(self):
        profit = self.profit.copy()
        profit.loc[profit.index[-1], "NETPROFIT"] = -30e8
        self.assertEqual(self.normalize(profit)["latest"]["net_profit"], -18)

    def test_integer_dates_and_duplicate_market_quotes(self):
        valuation = pd.DataFrame({"date": [20250328, 20250328], "market_cap": [100e8, 120e8]})
        self.assertEqual(self.normalize(valuation=valuation)["latest"]["market_cap"], 120)

    def test_nonfinite_and_zero_market_quotes_are_ignored(self):
        valuation = pd.concat([self.valuation, pd.DataFrame({
            "date": ["2025-05-07", "2025-05-08"], "market_cap": [float("inf"), 0],
        })])
        self.assertEqual(self.normalize(valuation=valuation)["latest_market_cap"], {"date": "2025-05-06", "value": 140})


if __name__ == "__main__":
    unittest.main()
