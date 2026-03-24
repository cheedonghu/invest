from __future__ import annotations

import logging

import pandas as pd

from backend.app.providers.interfaces.stock_provider import ValuationMetricsProvider
from backend.app.repositories.stock_value_daily_repository import StockValueDailyRepository


logger = logging.getLogger(__name__)


class DatabaseValuationProvider(ValuationMetricsProvider):
    source_name = "stock_value_daily"

    def __init__(self, repository: StockValueDailyRepository | None = None) -> None:
        self.repository = repository or StockValueDailyRepository()

    def fetch(self, symbol: str) -> pd.DataFrame:
        logger.info("Trying valuation provider stock_value_daily for symbol=%s", symbol)
        if not self.repository.is_available:
            raise RuntimeError("stock_value_daily repository is not configured")

        valuation_df = self.repository.fetch_by_symbol(symbol)
        if not isinstance(valuation_df, pd.DataFrame) or valuation_df.empty:
            raise RuntimeError(f"stock_value_daily did not contain symbol {symbol}")
        return valuation_df
