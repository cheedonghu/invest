from __future__ import annotations

import logging

import akshare as ak
import pandas as pd

from backend.app.providers.interfaces.stock_provider import MarketPerformanceProvider


logger = logging.getLogger(__name__)


class EastmoneyMarketPerformanceProvider(MarketPerformanceProvider):
    source_name = "eastmoney"

    @staticmethod
    def _to_eastmoney_symbol(symbol: str) -> str:
        return f"SH{symbol}" if symbol.startswith(("5", "6", "9")) else f"SZ{symbol}"

    def fetch(self, symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        logger.info("Trying market performance provider with symbol=%s", symbol)
        valuation_df = ak.stock_value_em(symbol=symbol)
        profit_df = ak.stock_profit_sheet_by_report_em(symbol=self._to_eastmoney_symbol(symbol))
        if not isinstance(valuation_df, pd.DataFrame) or valuation_df.empty:
            raise RuntimeError("stock_value_em returned empty dataframe")
        if not isinstance(profit_df, pd.DataFrame) or profit_df.empty:
            raise RuntimeError("stock_profit_sheet_by_report_em returned empty dataframe")
        return valuation_df, profit_df
