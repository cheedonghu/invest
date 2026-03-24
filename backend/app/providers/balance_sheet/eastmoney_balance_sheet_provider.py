from __future__ import annotations

import logging

import akshare as ak
import pandas as pd

from backend.app.providers.interfaces.stock_provider import BalanceSheetProvider


logger = logging.getLogger(__name__)


class EastmoneyBalanceSheetProvider(BalanceSheetProvider):
    source_name = "eastmoney"

    def fetch(self, symbol: str) -> pd.DataFrame:
        stock_symbol = f"SH{symbol}" if symbol.startswith(("5", "6", "9")) else f"SZ{symbol}"
        logger.info("Trying balance sheet provider stock_balance_sheet_by_report_em with symbol=%s", stock_symbol)
        df = ak.stock_balance_sheet_by_report_em(symbol=stock_symbol)
        if not isinstance(df, pd.DataFrame) or df.empty:
            raise RuntimeError(f"stock_balance_sheet_by_report_em returned empty dataframe for {stock_symbol}")
        return df
