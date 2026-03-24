from __future__ import annotations

import logging

import akshare as ak
import pandas as pd

from backend.app.providers.interfaces.stock_provider import BalanceSheetProvider


logger = logging.getLogger(__name__)


class SinaBalanceSheetProvider(BalanceSheetProvider):
    source_name = "sina"

    def fetch(self, symbol: str) -> pd.DataFrame:
        stock_symbol = f"sh{symbol}" if symbol.startswith(("5", "6", "9")) else f"sz{symbol}"
        logger.info("Trying balance sheet provider stock_financial_report_sina with stock=%s", stock_symbol)
        df = ak.stock_financial_report_sina(stock=stock_symbol, symbol="资产负债表")
        if not isinstance(df, pd.DataFrame) or df.empty:
            raise RuntimeError(f"stock_financial_report_sina returned empty dataframe for {stock_symbol}")
        return df
