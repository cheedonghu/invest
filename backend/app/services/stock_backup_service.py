from __future__ import annotations

import logging
import os
import re
import threading
from time import perf_counter
from typing import Any

import pandas as pd
from fastapi import HTTPException

from backend.app.providers.market_performance.eastmoney_market_performance_provider import EastmoneyMarketPerformanceProvider
from backend.app.repositories.db_session import DatabaseSessionFactory
from backend.app.repositories.stock_profit_sheet_repository import StockProfitSheetRepository
from backend.app.repositories.stock_value_daily_repository import StockValueDailyRepository


logger = logging.getLogger(__name__)


class StockBackupService:
    def __init__(self) -> None:
        self.session_factory = DatabaseSessionFactory()
        self.stock_value_daily_repository = StockValueDailyRepository(session_factory=self.session_factory)
        self.stock_profit_sheet_repository = StockProfitSheetRepository(session_factory=self.session_factory)
        self.provider = EastmoneyMarketPerformanceProvider()

    def backup_stock_value(self, symbol: str) -> dict[str, Any]:
        normalized_symbol = self.normalize_symbol(symbol)
        try:
            valuation_df, profit_df = self.provider.fetch(normalized_symbol)
            valuation_rows = self._normalize_stock_value_daily_rows(valuation_df, normalized_symbol)
            profit_rows = self._normalize_stock_profit_sheet_rows(profit_df, normalized_symbol)
            backup_summary = self._replace_backup_rows(
                symbol=normalized_symbol,
                valuation_rows=valuation_rows,
                profit_rows=profit_rows,
            )
        except Exception as exc:
            logger.exception("stock backup failed for symbol=%s", normalized_symbol)
            raise HTTPException(status_code=503, detail=f"stock backup failed: {exc}") from exc

        latest_available_date = self._extract_latest_available_date(valuation_df)
        latest_report_date = self._extract_latest_available_date(profit_df)
        return {
            "symbol": normalized_symbol,
            "source": self.provider.source_name,
            "tables": {
                "valuation": self.stock_value_daily_repository.table_name,
                "profit": self.stock_profit_sheet_repository.table_name,
            },
            "affected_rows": {
                "valuation": backup_summary["valuation_inserted"],
                "profit": backup_summary["profit_inserted"],
            },
            "deleted_rows": {
                "valuation": backup_summary["valuation_deleted"],
                "profit": backup_summary["profit_deleted"],
            },
            "records": {
                "valuation": len(valuation_rows),
                "profit": len(profit_rows),
            },
            "latest_available_date": latest_available_date,
            "latest_report_date": latest_report_date,
        }

    def trigger_profit_sheet_sync_async(self, symbol: str) -> None:
        normalized_symbol = self.normalize_symbol(symbol)
        worker = threading.Thread(
            target=self._sync_profit_sheet_if_missing,
            args=(normalized_symbol,),
            name=f"stock-profit-sync-{normalized_symbol}",
            daemon=True,
        )
        worker.start()

    def _sync_profit_sheet_if_missing(self, symbol: str) -> None:
        try:
            if not self.stock_profit_sheet_repository.is_available:
                logger.info("Skip async stock backup for %s because database is not configured", symbol)
                return

            if self.stock_profit_sheet_repository.exists_by_symbol(symbol):
                logger.info("Skip async stock backup for %s because stock_profit_sheet already has data", symbol)
                return

            logger.info("Trigger async stock backup for %s because stock_profit_sheet has no data", symbol)
            self.backup_stock_value(symbol)
        except Exception:
            logger.exception("Async stock backup failed for %s", symbol)

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        cleaned = re.sub(r"[^0-9]", "", symbol or "")
        if len(cleaned) != 6:
            raise HTTPException(status_code=400, detail="symbol must be a 6-digit A-share stock code")
        return cleaned

    def _replace_backup_rows(self, symbol: str, valuation_rows: list[dict], profit_rows: list[dict]) -> dict[str, int]:
        with self.session_factory.connect("stock_backup") as connection:
            cursor = connection.cursor()
            try:
                valuation_deleted = self.stock_value_daily_repository.delete_by_symbol(cursor, symbol)
                logger.info("[%s] cleared existing rows for %s: deleted=%s pid=%s service_id=%s", self.stock_value_daily_repository.table_name, symbol, valuation_deleted, os.getpid(), id(self))
                valuation_inserted = self.stock_value_daily_repository.insert_rows(cursor, symbol, valuation_rows)

                profit_deleted = self.stock_profit_sheet_repository.delete_by_symbol(cursor, symbol)
                logger.info("[%s] cleared existing rows for %s: deleted=%s pid=%s service_id=%s", self.stock_profit_sheet_repository.table_name, symbol, profit_deleted, os.getpid(), id(self))
                profit_inserted = self.stock_profit_sheet_repository.insert_rows(cursor, symbol, profit_rows)

                logger.info("About to commit backup transaction for %s pid=%s service_id=%s", symbol, os.getpid(), id(self))
                commit_started_at = perf_counter()
                connection.commit()
                commit_elapsed = perf_counter() - commit_started_at
                logger.info(
                    "Backup transaction committed for %s: valuation[deleted=%s inserted=%s] profit[deleted=%s inserted=%s] commit_elapsed=%.3fs pid=%s service_id=%s",
                    symbol,
                    valuation_deleted,
                    valuation_inserted,
                    profit_deleted,
                    profit_inserted,
                    commit_elapsed,
                    os.getpid(),
                    id(self),
                )
                return {
                    "valuation_deleted": int(valuation_deleted),
                    "valuation_inserted": int(valuation_inserted),
                    "profit_deleted": int(profit_deleted),
                    "profit_inserted": int(profit_inserted),
                }
            except Exception:
                connection.rollback()
                logger.exception("Backup transaction rolled back for %s", symbol)
                raise

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

        cleaned = raw_df[[date_col, close_col, pct_change_col, total_market_value_col, float_market_value_col, total_shares_col, float_shares_col, pe_ttm_col, pe_static_col, pb_col, peg_col, pcf_col, ps_col]].copy()
        cleaned.columns = ["trade_date", "close_price", "pct_change", "total_market_value", "float_market_value", "total_shares", "float_shares", "pe_ttm", "pe_static", "pb", "peg", "pcf", "ps"]
        cleaned["trade_date"] = self._normalize_datetime_key(cleaned["trade_date"]).dt.date
        for column in cleaned.columns:
            if column != "trade_date":
                cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")
        cleaned = cleaned.dropna(subset=["trade_date"]).sort_values("trade_date").reset_index(drop=True)

        rows: list[dict[str, Any]] = []
        for row in cleaned.itertuples(index=False):
            rows.append({
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
            })
        return rows

    def _normalize_stock_profit_sheet_rows(self, raw_df: pd.DataFrame, symbol: str) -> list[dict[str, Any]]:
        cleaned = pd.DataFrame(index=raw_df.index)
        cleaned["security_code"] = raw_df[self._find_column(raw_df, ["SECURITY_CODE", "security_code"])]
        cleaned["security_name_abbr"] = raw_df[self._find_column(raw_df, ["SECURITY_NAME_ABBR", "security_name_abbr"])]
        cleaned["report_date"] = raw_df[self._find_column(raw_df, ["REPORT_DATE", "report_date"])]
        cleaned["report_type"] = raw_df[self._find_column(raw_df, ["REPORT_TYPE", "report_type"])]
        cleaned["report_date_name"] = raw_df[self._find_optional_column(raw_df, ["REPORT_DATE_NAME", "report_date_name"])] if self._find_optional_column(raw_df, ["REPORT_DATE_NAME", "report_date_name"]) is not None else None
        cleaned["netprofit"] = raw_df[self._find_column(raw_df, ["NETPROFIT", "netprofit"])]
        cleaned["basic_eps"] = raw_df[self._find_optional_column(raw_df, ["BASIC_EPS", "basic_eps"])] if self._find_optional_column(raw_df, ["BASIC_EPS", "basic_eps"]) is not None else None
        cleaned["opinion_type"] = raw_df[self._find_optional_column(raw_df, ["OPINION_TYPE", "opinion_type"])] if self._find_optional_column(raw_df, ["OPINION_TYPE", "opinion_type"]) is not None else None

        total_income_column = self._find_optional_column(
            raw_df,
            ["TOTAL_OPERATE_INCOME", "OPERATE_INCOME_BALANCE", "OPERATE_INCOME", "total_operate_income"],
        )
        operate_income_column = self._find_optional_column(raw_df, ["OPERATE_INCOME", "operate_income"])
        total_cost_column = self._find_optional_column(
            raw_df,
            ["TOTAL_OPERATE_COST", "OPERATE_EXPENSE_BALANCE", "OPERATE_EXPENSE", "total_operate_cost"],
        )

        cleaned["total_operate_income"] = raw_df[total_income_column] if total_income_column is not None else None
        cleaned["total_operate_cost"] = raw_df[total_cost_column] if total_cost_column is not None else None

        cleaned["report_date"] = self._normalize_datetime_key(cleaned["report_date"]).dt.date
        for column in ["total_operate_income", "netprofit", "basic_eps", "total_operate_cost"]:
            cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

        if operate_income_column is not None:
            operate_income_series = pd.to_numeric(raw_df[operate_income_column], errors="coerce")
            total_income_missing = cleaned["total_operate_income"].isna() | cleaned["total_operate_income"].eq(0)
            operate_income_available = operate_income_series.notna() & operate_income_series.ne(0)
            cleaned.loc[total_income_missing & operate_income_available, "total_operate_income"] = operate_income_series.loc[
                total_income_missing & operate_income_available
            ]

        cleaned["security_code"] = cleaned["security_code"].astype(str).str.extract(r"(\d+)", expand=False).fillna(symbol).str.zfill(6)
        cleaned = cleaned.dropna(subset=["report_date"]).sort_values("report_date").drop_duplicates(subset=["security_code", "report_date"], keep="last").reset_index(drop=True)

        rows: list[dict[str, Any]] = []
        for row in cleaned.itertuples(index=False):
            rows.append(
                {
                    "security_code": str(row.security_code),
                    "security_name_abbr": None if pd.isna(row.security_name_abbr) else str(row.security_name_abbr),
                    "report_date": row.report_date,
                    "report_type": None if pd.isna(row.report_type) else str(row.report_type),
                    "report_date_name": None if pd.isna(row.report_date_name) else str(row.report_date_name),
                    "total_operate_income": None if pd.isna(row.total_operate_income) else float(row.total_operate_income),
                    "netprofit": None if pd.isna(row.netprofit) else float(row.netprofit),
                    "basic_eps": None if pd.isna(row.basic_eps) else float(row.basic_eps),
                    "total_operate_cost": None if pd.isna(row.total_operate_cost) else float(row.total_operate_cost),
                    "opinion_type": None if pd.isna(row.opinion_type) else str(row.opinion_type),
                }
            )
        return rows

    def _extract_latest_available_date(self, raw_df: pd.DataFrame) -> str | None:
        date_col = self._find_column(raw_df, ["数据日期", "date", "trade_date", "REPORT_DATE", "report_date"])
        series = self._normalize_datetime_key(raw_df[date_col]).dropna()
        if series.empty:
            return None
        return series.max().strftime("%Y-%m-%d")

    @staticmethod
    def _normalize_datetime_key(values: pd.Series) -> pd.Series:
        return pd.to_datetime(values, errors="coerce").astype("datetime64[ns]")

    @staticmethod
    def _find_optional_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
        normalized = {str(column).strip().lower(): column for column in df.columns}
        for candidate in candidates:
            key = candidate.strip().lower()
            if key in normalized:
                return normalized[key]
        return None

    @staticmethod
    def _find_column(df: pd.DataFrame, candidates: list[str]) -> str:
        normalized = {str(column).strip().lower(): column for column in df.columns}
        for candidate in candidates:
            key = candidate.strip().lower()
            if key in normalized:
                return normalized[key]
        raise HTTPException(status_code=500, detail=f"unable to recognize dataframe columns: {list(df.columns)!r}")
