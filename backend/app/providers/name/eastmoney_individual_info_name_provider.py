from __future__ import annotations

import logging

import akshare as ak
import pandas as pd

from backend.app.providers.interfaces.stock_provider import StockNameProvider


logger = logging.getLogger(__name__)


class EastmoneyIndividualInfoNameProvider(StockNameProvider):
    source_name = "eastmoney_individual_info"

    def fetch(self, symbol: str) -> str:
        logger.info("Trying stock name provider stock_individual_info_em with symbol=%s", symbol)
        df = ak.stock_individual_info_em(symbol=symbol)
        if not isinstance(df, pd.DataFrame) or df.empty:
            raise RuntimeError("stock_individual_info_em returned empty dataframe")

        item_col = self._find_column(df, ["item", "\u9879\u76ee"])
        value_col = self._find_column(df, ["value", "\u503c"])
        matched = df.loc[df[item_col].astype(str).str.strip() == "\u80a1\u7968\u7b80\u79f0", value_col]
        if matched.empty:
            raise RuntimeError("stock_individual_info_em did not contain stock short name")

        name = str(matched.iloc[0]).strip()
        if not name:
            raise RuntimeError("stock_individual_info_em returned blank stock short name")
        return name

    @staticmethod
    def _find_column(df: pd.DataFrame, candidates: list[str]) -> str:
        normalized = {str(column).strip().lower(): column for column in df.columns}
        for candidate in candidates:
            key = candidate.strip().lower()
            if key in normalized:
                return normalized[key]
        raise RuntimeError(f"unable to recognize columns: {list(df.columns)!r}")
