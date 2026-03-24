from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd
from fastapi import HTTPException

from backend.app.providers.balance_sheet.eastmoney_balance_sheet_provider import EastmoneyBalanceSheetProvider
from backend.app.providers.balance_sheet.sina_balance_sheet_provider import SinaBalanceSheetProvider
from backend.app.providers.interfaces.stock_provider import (
    BalanceSheetProvider,
    MarketPerformanceProvider,
    StockNameProvider,
    ValuationMetricsProvider,
)
from backend.app.providers.market_performance.database_market_performance_provider import DatabaseMarketPerformanceProvider
from backend.app.providers.market_performance.eastmoney_market_performance_provider import EastmoneyMarketPerformanceProvider
from backend.app.providers.name.eastmoney_individual_info_name_provider import EastmoneyIndividualInfoNameProvider
from backend.app.providers.name.xueqiu_individual_info_name_provider import XueqiuIndividualInfoNameProvider
from backend.app.providers.valuation.database_valuation_provider import DatabaseValuationProvider
from backend.app.providers.valuation.eastmoney_valuation_provider import EastmoneyValuationProvider
from backend.app.repositories.stock_profit_sheet_repository import StockProfitSheetRepository
from backend.app.repositories.stock_value_daily_repository import StockValueDailyRepository
from backend.app.services.balance_sheet_aggregation_service import BalanceSheetAggregationService
from backend.app.services.stock_backup_service import StockBackupService


logger = logging.getLogger(__name__)


class StockService:
    def __init__(self) -> None:
        self.stock_value_daily_repository = StockValueDailyRepository()
        self.stock_profit_sheet_repository = StockProfitSheetRepository()
        self.balance_sheet_providers: list[BalanceSheetProvider] = [SinaBalanceSheetProvider(), EastmoneyBalanceSheetProvider()]
        self.valuation_providers: list[ValuationMetricsProvider] = [
            DatabaseValuationProvider(self.stock_value_daily_repository),
            EastmoneyValuationProvider(),
        ]
        self.market_performance_providers: list[MarketPerformanceProvider] = [
            DatabaseMarketPerformanceProvider(self.stock_value_daily_repository, self.stock_profit_sheet_repository),
            EastmoneyMarketPerformanceProvider(),
        ]
        self.name_providers: list[StockNameProvider] = [
            XueqiuIndividualInfoNameProvider(),
            EastmoneyIndividualInfoNameProvider(),
        ]
        self.balance_sheet_aggregation_service = BalanceSheetAggregationService()
        self.stock_backup_service = StockBackupService()

    def get_stock_name(self, symbol: str) -> dict[str, Any]:
        normalized_symbol = self.normalize_symbol(symbol)
        self.stock_backup_service.trigger_profit_sheet_sync_async(normalized_symbol)
        name, source = self._fetch_name_with_failover(normalized_symbol)
        return {"symbol": normalized_symbol, "name": name, "source": source}

    def get_stock_valuation_metrics(self, symbol: str, years: int = 10) -> dict[str, Any]:
        normalized_symbol = self.normalize_symbol(symbol)
        if years <= 0:
            raise HTTPException(status_code=400, detail="years must be a positive integer")

        valuation_df, provider_name = self._fetch_with_failover_and_source(
            normalized_symbol,
            self.valuation_providers,
            "valuation metrics",
        )
        metrics = self._normalize_valuation_metrics(valuation_df, normalized_symbol, years=years)
        latest_date = max(metric["latest"]["date"] for metric in metrics.values() if metric["series"])

        return {
            "symbol": normalized_symbol,
            "years": years,
            "latest_date": latest_date,
            "provider_summary": {
                "sources": [provider.source_name for provider in self.valuation_providers],
                "used_source": provider_name,
                "latest_available_date": latest_date,
            },
            "metrics": metrics,
        }

    def get_stock_market_performance(self, symbol: str, years: int = 10) -> dict[str, Any]:
        normalized_symbol = self.normalize_symbol(symbol)
        if years <= 0:
            raise HTTPException(status_code=400, detail="years must be a positive integer")

        (valuation_df, performance_df), provider_name = self._fetch_tuple_with_failover(
            normalized_symbol,
            self.market_performance_providers,
            "market performance",
        )
        market_performance = self._normalize_market_performance(valuation_df, performance_df, normalized_symbol, years=years)
        latest_available_date = self._extract_latest_available_date(valuation_df)

        return {
            "symbol": normalized_symbol,
            "years": years,
            "provider_summary": {
                "sources": [provider.source_name for provider in self.market_performance_providers],
                "used_source": provider_name,
                "latest_available_date": latest_available_date,
            },
            "market_performance": market_performance,
        }

    def get_stock_balance_sheet(self, symbol: str, years: int = 3) -> dict[str, Any]:
        normalized_symbol = self.normalize_symbol(symbol)
        raw_df = self._fetch_with_failover(normalized_symbol, self.balance_sheet_providers, "balance sheet")
        periods = self.balance_sheet_aggregation_service.aggregate(raw_df, normalized_symbol, years=years)
        return {
            "symbol": normalized_symbol,
            "metric": "\u8d44\u4ea7\u8d1f\u503a\u8868",
            "years": years,
            "periods": periods,
        }

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        cleaned = re.sub(r"[^0-9]", "", symbol or "")
        if len(cleaned) != 6:
            raise HTTPException(status_code=400, detail="symbol must be a 6-digit A-share stock code")
        return cleaned

    def _fetch_name_with_failover(self, symbol: str) -> tuple[str, str]:
        errors: list[str] = []
        for provider in self.name_providers:
            try:
                return provider.fetch(symbol), provider.source_name
            except Exception as exc:
                logger.warning(
                    "Stock name provider %s failed for symbol=%s: %s",
                    getattr(provider, "source_name", provider.__class__.__name__),
                    symbol,
                    exc,
                )
                errors.append(f"{getattr(provider, 'source_name', provider.__class__.__name__)} failed: {exc}")
        raise HTTPException(status_code=502, detail="stock name fetch failed. " + " | ".join(errors))

    def _fetch_with_failover(self, symbol: str, providers: list[Any], metric_name: str) -> pd.DataFrame:
        errors: list[str] = []
        for provider in providers:
            try:
                return provider.fetch(symbol)
            except Exception as exc:
                logger.exception(
                    "Provider %s failed for %s symbol=%s",
                    getattr(provider, "source_name", provider.__class__.__name__),
                    metric_name,
                    symbol,
                )
                errors.append(f"{getattr(provider, 'source_name', provider.__class__.__name__)} failed: {exc}")
        raise HTTPException(status_code=502, detail=f"{metric_name} fetch failed. " + " | ".join(errors))


    def _fetch_with_failover_and_source(self, symbol: str, providers: list[Any], metric_name: str) -> tuple[pd.DataFrame, str]:
        errors: list[str] = []
        for provider in providers:
            try:
                return provider.fetch(symbol), provider.source_name
            except Exception as exc:
                logger.exception(
                    "Provider %s failed for %s symbol=%s",
                    getattr(provider, "source_name", provider.__class__.__name__),
                    metric_name,
                    symbol,
                )
                errors.append(f"{getattr(provider, 'source_name', provider.__class__.__name__)} failed: {exc}")
        raise HTTPException(status_code=502, detail=f"{metric_name} fetch failed. " + " | ".join(errors))

    def _fetch_tuple_with_failover(
        self,
        symbol: str,
        providers: list[Any],
        metric_name: str,
    ) -> tuple[tuple[pd.DataFrame, pd.DataFrame], str]:
        errors: list[str] = []
        for provider in providers:
            try:
                return provider.fetch(symbol), provider.source_name
            except Exception as exc:
                logger.exception(
                    "Provider %s failed for %s symbol=%s",
                    getattr(provider, "source_name", provider.__class__.__name__),
                    metric_name,
                    symbol,
                )
                errors.append(f"{getattr(provider, 'source_name', provider.__class__.__name__)} failed: {exc}")
        raise HTTPException(status_code=502, detail=f"{metric_name} fetch failed. " + " | ".join(errors))

    def _normalize_valuation_metrics(self, raw_df: pd.DataFrame, symbol: str, years: int) -> dict[str, dict[str, Any]]:
        date_col = self._find_column(raw_df, ["\u6570\u636e\u65e5\u671f", "date", "trade_date"])
        filtered_df = self._filter_recent_years(raw_df, date_col, years=years, symbol=symbol, label="valuation metrics")

        metric_specs = {
            "pe": {"label": "PE(TTM)", "candidates": ["PE(TTM)", "PE_TTM", "pe_ttm"]},
            "pb": {"label": "\u5e02\u51c0\u7387", "candidates": ["\u5e02\u51c0\u7387", "PB_MRQ", "pb"]},
            "ps": {"label": "\u5e02\u9500\u7387", "candidates": ["\u5e02\u9500\u7387", "PS_TTM", "ps_ttm", "ps"]},
        }

        metrics: dict[str, dict[str, Any]] = {}
        for metric_key, spec in metric_specs.items():
            value_col = self._find_column(filtered_df, spec["candidates"])
            cleaned = self._normalize_series_frame(filtered_df, date_col, value_col)
            if cleaned.empty:
                raise HTTPException(status_code=404, detail=f"{spec['label']} data is empty for symbol {symbol}")

            values = cleaned["value"]
            mean = float(values.mean())
            std = float(values.std(ddof=0)) if len(values) > 1 else 0.0
            series = [
                {"date": row.date.strftime("%Y-%m-%d"), "value": round(float(row.value), 4)}
                for row in cleaned.itertuples(index=False)
            ]

            metrics[metric_key] = {
                "label": spec["label"],
                "latest": series[-1],
                "stats": {
                    "mean": round(mean, 4),
                    "std": round(std, 4),
                    "upper": round(mean + std, 4),
                    "lower": round(mean - std, 4),
                },
                "series": series,
            }

        return metrics

    def _normalize_market_performance(self, valuation_df: pd.DataFrame, performance_df: pd.DataFrame, symbol: str, years: int) -> dict[str, Any]:
        valuation_date_col = self._find_column(valuation_df, ["\u6570\u636e\u65e5\u671f", "date", "trade_date"])
        market_cap_col = self._find_column(valuation_df, ["\u603b\u5e02\u503c", "TOTAL_MARKET_CAP", "market_cap", "total_market_value"])
        valuation_cleaned = self._normalize_series_frame(valuation_df, valuation_date_col, market_cap_col)
        valuation_cleaned.rename(columns={"value": "market_cap"}, inplace=True)
        valuation_cleaned["market_cap"] = valuation_cleaned["market_cap"] / 1e8

        performance_date_col = self._find_column(performance_df, ["REPORT_DATE", "\u62a5\u544a\u65e5", "\u6570\u636e\u65e5\u671f"])
        revenue_col = self._find_column(performance_df, ["\u8425\u4e1a\u603b\u6536\u5165", "\u8425\u4e1a\u6536\u5165", "TOTAL_OPERATE_INCOME", "OPERATE_INCOME"])
        profit_col = self._find_column(performance_df, ["\u5f52\u6bcd\u51c0\u5229\u6da6", "\u51c0\u5229\u6da6", "\u5f52\u5c5e\u4e8e\u6bcd\u516c\u53f8\u80a1\u4e1c\u7684\u51c0\u5229\u6da6", "PARENT_NETPROFIT", "NETPROFIT"])
        opinion_col = self._find_optional_column(performance_df, ["OPINION_TYPE", "opinion_type", "\u5ba1\u8ba1\u610f\u89c1\u7c7b\u578b", "\u5ba1\u8ba1\u610f\u89c1"])

        selected_columns = [performance_date_col, revenue_col, profit_col]
        if opinion_col is not None:
            selected_columns.append(opinion_col)

        performance_cleaned = (
            performance_df[selected_columns]
            .rename(
                columns={
                    performance_date_col: "date",
                    revenue_col: "revenue",
                    profit_col: "net_profit",
                    **({opinion_col: "audit_opinion"} if opinion_col is not None else {}),
                }
            )
            .assign(
                date=lambda df: self._normalize_datetime_key(df["date"]),
                revenue=lambda df: pd.to_numeric(df["revenue"], errors="coerce") / 1e8,
                net_profit=lambda df: pd.to_numeric(df["net_profit"], errors="coerce") / 1e8,
            )
            .dropna(subset=["date", "revenue", "net_profit"])
            .sort_values("date")
            .drop_duplicates(subset=["date"], keep="last")
            .reset_index(drop=True)
        )
        if "audit_opinion" not in performance_cleaned.columns:
            performance_cleaned["audit_opinion"] = None
        else:
            performance_cleaned["audit_opinion"] = performance_cleaned["audit_opinion"].where(
                performance_cleaned["audit_opinion"].notna(),
                None,
            )
        performance_cleaned["revenue"] = self._calculate_ttm_from_cumulative(
            performance_cleaned["date"],
            performance_cleaned["revenue"],
        )
        performance_cleaned["net_profit"] = self._calculate_ttm_from_cumulative(
            performance_cleaned["date"],
            performance_cleaned["net_profit"],
        )
        performance_cleaned = performance_cleaned.dropna(subset=["revenue", "net_profit"]).reset_index(drop=True)
        if performance_cleaned.empty:
            raise HTTPException(status_code=404, detail=f"market performance data is empty for symbol {symbol}")

        latest_financial_date = performance_cleaned.iloc[-1]["date"]
        cutoff_date = latest_financial_date - pd.DateOffset(years=years)
        performance_cleaned = performance_cleaned.loc[performance_cleaned["date"] >= cutoff_date].reset_index(drop=True)
        if performance_cleaned.empty:
            raise HTTPException(
                status_code=404,
                detail=f"market performance financial data is empty within the last {years} years for symbol {symbol}",
            )

        merged = pd.merge_asof(
            performance_cleaned.sort_values("date"),
            valuation_cleaned.sort_values("date"),
            on="date",
            direction="backward",
        )
        merged = merged.dropna(subset=["market_cap"]).reset_index(drop=True)
        if merged.empty:
            raise HTTPException(status_code=404, detail=f"market performance merged data is empty for symbol {symbol}")

        series = [
            {
                "date": self._format_quarter_label(row.date),
                "market_cap": round(float(row.market_cap), 2),
                "revenue": round(float(row.revenue), 2),
                "net_profit": round(float(row.net_profit), 2),
                "audit_opinion": self._normalize_audit_opinion(row.audit_opinion),
                "highlight_revenue": self._should_highlight_revenue(row.date, row.audit_opinion),
            }
            for row in merged.itertuples(index=False)
        ]

        return {"label": "\u5e02\u503c\u4e0e\u4e1a\u7ee9\u5b63\u5ea6\u8d8b\u52bf", "latest": series[-1], "series": series}

    def _normalize_stock_value_daily_rows(self, raw_df: pd.DataFrame, symbol: str) -> list[dict[str, Any]]:
        date_col = self._find_column(raw_df, ["\u6570\u636e\u65e5\u671f", "date", "trade_date"])
        close_col = self._find_column(raw_df, ["\u5f53\u65e5\u6536\u76d8\u4ef7", "close_price"])
        pct_change_col = self._find_column(raw_df, ["\u5f53\u65e5\u6da8\u8dcc\u5e45", "pct_change"])
        total_market_value_col = self._find_column(raw_df, ["\u603b\u5e02\u503c", "total_market_value"])
        float_market_value_col = self._find_column(raw_df, ["\u6d41\u901a\u5e02\u503c", "float_market_value"])
        total_shares_col = self._find_column(raw_df, ["\u603b\u80a1\u672c", "total_shares"])
        float_shares_col = self._find_column(raw_df, ["\u6d41\u901a\u80a1\u672c", "float_shares"])
        pe_ttm_col = self._find_column(raw_df, ["PE(TTM)", "pe_ttm"])
        pe_static_col = self._find_column(raw_df, ["PE(\u9759)", "pe_static"])
        pb_col = self._find_column(raw_df, ["\u5e02\u51c0\u7387", "pb"])
        peg_col = self._find_column(raw_df, ["PEG\u503c", "peg"])
        pcf_col = self._find_column(raw_df, ["\u5e02\u73b0\u7387", "pcf"])
        ps_col = self._find_column(raw_df, ["\u5e02\u9500\u7387", "ps"])

        cleaned = raw_df[
            [
                date_col,
                close_col,
                pct_change_col,
                total_market_value_col,
                float_market_value_col,
                total_shares_col,
                float_shares_col,
                pe_ttm_col,
                pe_static_col,
                pb_col,
                peg_col,
                pcf_col,
                ps_col,
            ]
        ].copy()
        cleaned.columns = [
            "trade_date",
            "close_price",
            "pct_change",
            "total_market_value",
            "float_market_value",
            "total_shares",
            "float_shares",
            "pe_ttm",
            "pe_static",
            "pb",
            "peg",
            "pcf",
            "ps",
        ]
        cleaned["trade_date"] = self._normalize_datetime_key(cleaned["trade_date"]).dt.date
        for column in cleaned.columns:
            if column == "trade_date":
                continue
            cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")
        cleaned = cleaned.dropna(subset=["trade_date"]).sort_values("trade_date").reset_index(drop=True)

        rows: list[dict[str, Any]] = []
        for row in cleaned.itertuples(index=False):
            rows.append(
                {
                    "symbol": symbol,
                    "trade_date": row.trade_date,
                    "close_price": None if pd.isna(row.close_price) else float(row.close_price),
                    "pct_change": None if pd.isna(row.pct_change) else float(row.pct_change),
                    "total_market_value": None if pd.isna(row.total_market_value) else float(row.total_market_value),
                    "float_market_value": None if pd.isna(row.float_market_value) else float(row.float_market_value),
                    "total_shares": None if pd.isna(row.total_shares) else int(float(row.total_shares)),
                    "float_shares": None if pd.isna(row.float_shares) else int(float(row.float_shares)),
                    "pe_ttm": None if pd.isna(row.pe_ttm) else float(row.pe_ttm),
                    "pe_static": None if pd.isna(row.pe_static) else float(row.pe_static),
                    "pb": None if pd.isna(row.pb) else float(row.pb),
                    "peg": None if pd.isna(row.peg) else float(row.peg),
                    "pcf": None if pd.isna(row.pcf) else float(row.pcf),
                    "ps": None if pd.isna(row.ps) else float(row.ps),
                }
            )
        return rows

    def _extract_latest_available_date(self, raw_df: pd.DataFrame) -> str | None:
        date_col = self._find_column(raw_df, ["\u6570\u636e\u65e5\u671f", "date", "trade_date"])
        series = self._normalize_datetime_key(raw_df[date_col]).dropna()
        if series.empty:
            return None
        return series.max().strftime("%Y-%m-%d")

    @staticmethod
    def _filter_recent_years(raw_df: pd.DataFrame, date_col: str, years: int, symbol: str, label: str) -> pd.DataFrame:
        cleaned_df = raw_df.copy()
        cleaned_df[date_col] = StockService._normalize_datetime_key(cleaned_df[date_col])
        cleaned_df = cleaned_df.dropna(subset=[date_col]).sort_values(date_col).reset_index(drop=True)
        if cleaned_df.empty:
            raise HTTPException(status_code=404, detail=f"{label} dates are empty for symbol {symbol}")
        latest_date = cleaned_df.iloc[-1][date_col]
        cutoff_date = latest_date - pd.DateOffset(years=years)
        cleaned_df = cleaned_df.loc[cleaned_df[date_col] >= cutoff_date].reset_index(drop=True)
        if cleaned_df.empty:
            raise HTTPException(status_code=404, detail=f"{label} are empty within the last {years} years for symbol {symbol}")
        return cleaned_df

    @staticmethod
    def _normalize_series_frame(raw_df: pd.DataFrame, date_col: str, value_col: str) -> pd.DataFrame:
        return (
            raw_df[[date_col, value_col]]
            .rename(columns={date_col: "date", value_col: "value"})
            .assign(
                date=lambda df: StockService._normalize_datetime_key(df["date"]),
                value=lambda df: pd.to_numeric(df["value"], errors="coerce"),
            )
            .dropna(subset=["date", "value"])
            .sort_values("date")
            .reset_index(drop=True)
        )

    @staticmethod
    def _normalize_datetime_key(values: pd.Series) -> pd.Series:
        return pd.to_datetime(values, errors="coerce").astype("datetime64[ns]")

    @staticmethod
    def _calculate_ttm_from_cumulative(dates: pd.Series, values: pd.Series) -> pd.Series:
        frame = pd.DataFrame({"date": pd.to_datetime(dates), "value": pd.to_numeric(values, errors="coerce")}).dropna()
        if frame.empty:
            return pd.Series(index=dates.index, dtype="float64")

        frame = frame.sort_values("date").copy()
        frame["year"] = frame["date"].dt.year
        frame["quarter"] = frame["date"].dt.quarter
        lookup = {(int(row.year), int(row.quarter)): float(row.value) for row in frame.itertuples(index=False)}

        ttm_map: dict[pd.Timestamp, float] = {}
        for row in frame.itertuples(index=False):
            year = int(row.year)
            quarter = int(row.quarter)
            current_value = float(row.value)
            if quarter == 4:
                ttm_map[row.date] = current_value
                continue

            prev_q4 = lookup.get((year - 1, 4))
            prev_same_quarter = lookup.get((year - 1, quarter))
            if prev_q4 is None or prev_same_quarter is None:
                continue
            ttm_map[row.date] = current_value + prev_q4 - prev_same_quarter

        return pd.Series([ttm_map.get(pd.Timestamp(date)) for date in dates], index=dates.index, dtype="float64")

    @staticmethod
    def _format_quarter_label(value: pd.Timestamp) -> str:
        quarter = ((value.month - 1) // 3) + 1
        return f"{value.year}-Q{quarter}"

    @staticmethod
    def _find_optional_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
        normalized = {str(column).strip().lower(): column for column in df.columns}
        for candidate in candidates:
            key = candidate.strip().lower()
            if key in normalized:
                return normalized[key]
        return None

    @staticmethod
    def _normalize_audit_opinion(value: Any) -> str | None:
        if value is None or pd.isna(value):
            return None
        text = str(value).strip()
        return text or None

    @classmethod
    def _should_highlight_revenue(cls, report_date: pd.Timestamp, opinion: Any) -> bool:
        normalized_opinion = cls._normalize_audit_opinion(opinion)
        return (
            report_date.quarter == 4
            and normalized_opinion is not None
            and normalized_opinion != "\u6807\u51c6\u65e0\u4fdd\u7559\u610f\u89c1"
        )

    @staticmethod
    def _find_column(df: pd.DataFrame, candidates: list[str]) -> str:
        normalized = {str(column).strip().lower(): column for column in df.columns}
        for candidate in candidates:
            key = candidate.strip().lower()
            if key in normalized:
                return normalized[key]
        raise HTTPException(status_code=500, detail=f"unable to recognize dataframe columns: {list(df.columns)!r}")
