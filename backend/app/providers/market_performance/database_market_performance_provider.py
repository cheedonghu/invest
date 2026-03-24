from __future__ import annotations

import logging

import akshare as ak
import pandas as pd

from backend.app.providers.interfaces.stock_provider import MarketPerformanceProvider
from backend.app.repositories.stock_profit_sheet_repository import StockProfitSheetRepository
from backend.app.repositories.stock_value_daily_repository import StockValueDailyRepository


logger = logging.getLogger(__name__)


class DatabaseMarketPerformanceProvider(MarketPerformanceProvider):
    source_name = "stock_value_daily"

    def __init__(
        self,
        repository: StockValueDailyRepository | None = None,
        profit_repository: StockProfitSheetRepository | None = None,
    ) -> None:
        self.repository = repository or StockValueDailyRepository()
        self.profit_repository = profit_repository or StockProfitSheetRepository()

    @staticmethod
    def _to_eastmoney_symbol(symbol: str) -> str:
        return f"SH{symbol}" if symbol.startswith(("5", "6", "9")) else f"SZ{symbol}"

    def fetch(self, symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        logger.info("Trying market performance provider stock_value_daily for symbol=%s", symbol)
        if not self.repository.is_available:
            raise RuntimeError("stock_value_daily repository is not configured")

        valuation_df = self.repository.fetch_by_symbol(symbol)
        if not isinstance(valuation_df, pd.DataFrame) or valuation_df.empty:
            raise RuntimeError(f"stock_value_daily did not contain symbol {symbol}")

        valuation_df = valuation_df.rename(
            columns={
                "trade_date": "date",
                "close_price": "close_price",
                "pct_change": "pct_change",
                "total_market_value": "market_cap",
                "float_market_value": "float_market_value",
                "total_shares": "total_shares",
                "float_shares": "float_shares",
                "pe_ttm": "pe_ttm",
                "pe_static": "pe_static",
                "pb": "pb",
                "peg": "peg",
                "pcf": "pcf",
                "ps": "ps",
            }
        )

        profit_df = self.profit_repository.fetch_by_symbol(symbol)
        if isinstance(profit_df, pd.DataFrame) and not profit_df.empty:
            logger.info("Using profit table stock_profit_sheet for symbol=%s", symbol)
            return valuation_df, profit_df

        logger.info("stock_profit_sheet did not contain symbol=%s, falling back to akshare profit sheet", symbol)
        profit_df = ak.stock_profit_sheet_by_report_em(symbol=self._to_eastmoney_symbol(symbol))
        if not isinstance(profit_df, pd.DataFrame) or profit_df.empty:
            raise RuntimeError("stock_profit_sheet_by_report_em returned empty dataframe")
        return valuation_df, profit_df
