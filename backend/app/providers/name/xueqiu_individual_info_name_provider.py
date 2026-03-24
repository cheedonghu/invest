from __future__ import annotations

import logging

import akshare as ak
import pandas as pd

from backend.app.providers.interfaces.stock_provider import StockNameProvider


logger = logging.getLogger(__name__)


class XueqiuIndividualInfoNameProvider(StockNameProvider):
    source_name = "xueqiu_individual_basic_info"

    def fetch(self, symbol: str) -> str:
        xq_symbol = self._to_xq_symbol(symbol)
        logger.info("Trying stock name provider stock_individual_basic_info_xq with symbol=%s", xq_symbol)
        df = ak.stock_individual_basic_info_xq(symbol=xq_symbol)
        if not isinstance(df, pd.DataFrame) or df.empty:
            raise RuntimeError("stock_individual_basic_info_xq returned empty dataframe")

        item_col = self._find_column(df, ["item", "\u9879\u76ee", "\u5b57\u6bb5", "name"])
        value_col = self._find_column(df, ["value", "\u503c", "\u5185\u5bb9", "\u6570\u636e"])
        item_series = df[item_col].astype(str).str.strip()

        for field_name in ["org_short_name_cn", "org_name_cn", "org_short_name_en", "org_name_en"]:
            matched = df.loc[item_series == field_name, value_col]
            if matched.empty:
                continue
            name = str(matched.iloc[0]).strip()
            if name and name.lower() != "none":
                return name

        raise RuntimeError(
            f"stock_individual_basic_info_xq did not contain a usable stock name; columns={list(df.columns)!r}"
        )

    @staticmethod
    def _to_xq_symbol(symbol: str) -> str:
        prefix = "SH" if symbol.startswith(("6", "9")) else "SZ"
        return f"{prefix}{symbol}"

    @staticmethod
    def _find_column(df: pd.DataFrame, candidates: list[str]) -> str:
        normalized = {str(column).strip().lower(): column for column in df.columns}
        for candidate in candidates:
            key = candidate.strip().lower()
            if key in normalized:
                return normalized[key]
        raise RuntimeError(f"unable to recognize columns: {list(df.columns)!r}")
