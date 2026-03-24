from __future__ import annotations

import logging

import akshare as ak
import pandas as pd

from backend.app.providers.interfaces.stock_provider import ValuationMetricsProvider


logger = logging.getLogger(__name__)


class EastmoneyValuationProvider(ValuationMetricsProvider):
    source_name = "eastmoney"

    def fetch(self, symbol: str) -> pd.DataFrame:
        logger.info("Trying valuation provider stock_value_em with symbol=%s", symbol)
        df = ak.stock_value_em(symbol=symbol)
        if not isinstance(df, pd.DataFrame) or df.empty:
            raise RuntimeError("stock_value_em returned empty dataframe")
        return df
