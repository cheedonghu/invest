from __future__ import annotations

from datetime import date
import logging
from time import perf_counter
from typing import Any

import pandas as pd

from backend.app.config.database import DatabaseSettings
from backend.app.repositories.db_session import DatabaseSessionFactory


logger = logging.getLogger(__name__)


class StockValueDailyRepository:
    table_name = "stock_value_daily"
    batch_size = 500

    def __init__(self, settings: DatabaseSettings | None = None, session_factory: DatabaseSessionFactory | None = None) -> None:
        self.session_factory = session_factory or DatabaseSessionFactory(settings)
        self.settings = self.session_factory.settings

    @property
    def is_available(self) -> bool:
        return self.session_factory.is_available

    def connect(self):
        return self.session_factory.connect(self.table_name)

    def fetch_by_symbol(self, symbol: str) -> pd.DataFrame:
        query = f"""
            SELECT
                trade_date,
                close_price,
                pct_change,
                total_market_value,
                float_market_value,
                total_shares,
                float_shares,
                pe_ttm,
                pe_static,
                pb,
                peg,
                pcf,
                ps
            FROM {self.table_name}
            WHERE symbol = %s
            ORDER BY trade_date ASC
        """
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query, [symbol])
            rows = cursor.fetchall() or []
        return pd.DataFrame(rows)

    def fetch_one_sample(self) -> dict[str, Any] | None:
        query = f"""
            SELECT
                symbol,
                trade_date,
                close_price,
                pct_change,
                total_market_value,
                float_market_value,
                total_shares,
                float_shares,
                pe_ttm,
                pe_static,
                pb,
                peg,
                pcf,
                ps
            FROM {self.table_name}
            ORDER BY trade_date DESC, symbol ASC
            LIMIT 1
        """
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
        return row

    def latest_trade_date(self, symbol: str) -> date | None:
        query = f"SELECT MAX(trade_date) AS latest_trade_date FROM {self.table_name} WHERE symbol = %s"
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query, [symbol])
            row = cursor.fetchone() or {}
        return row.get("latest_trade_date")

    def delete_by_symbol(self, cursor: Any, symbol: str) -> int:
        query = f"DELETE FROM {self.table_name} WHERE symbol = %s"
        return int(cursor.execute(query, [symbol]))

    def insert_rows(self, cursor: Any, symbol: str, rows: list[dict]) -> int:
        query = f"""
            INSERT INTO {self.table_name} (
                symbol,
                trade_date,
                close_price,
                pct_change,
                total_market_value,
                float_market_value,
                total_shares,
                float_shares,
                pe_ttm,
                pe_static,
                pb,
                peg,
                pcf,
                ps,
                created_at,
                updated_at
            ) VALUES (
                %(symbol)s,
                %(trade_date)s,
                %(close_price)s,
                %(pct_change)s,
                %(total_market_value)s,
                %(float_market_value)s,
                %(total_shares)s,
                %(float_shares)s,
                %(pe_ttm)s,
                %(pe_static)s,
                %(pb)s,
                %(peg)s,
                %(pcf)s,
                %(ps)s,
                CURRENT_DATE,
                CURRENT_DATE
            )
        """
        return self._insert_in_batches(cursor, symbol, query, rows)

    def _insert_in_batches(self, cursor: Any, symbol: str, query: str, rows: list[dict]) -> int:
        if not rows:
            logger.info("[%s] no rows to insert for %s", self.table_name, symbol)
            return 0

        total_rows = len(rows)
        batch_count = (total_rows + self.batch_size - 1) // self.batch_size
        total_affected = 0
        for start in range(0, total_rows, self.batch_size):
            batch = rows[start : start + self.batch_size]
            batch_index = (start // self.batch_size) + 1
            affected = int(cursor.executemany(query, batch))
            total_affected += affected
            logger.info(
                "[%s] batch executed for %s: batch %s/%s, rows %s-%s/%s, affected=%s",
                self.table_name,
                symbol,
                batch_index,
                batch_count,
                start + 1,
                start + len(batch),
                total_rows,
                affected,
            )
        return total_affected
