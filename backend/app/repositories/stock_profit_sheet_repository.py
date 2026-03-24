from __future__ import annotations

from typing import Any
import logging

import pandas as pd

from backend.app.config.database import DatabaseSettings
from backend.app.repositories.db_session import DatabaseSessionFactory


logger = logging.getLogger(__name__)


class StockProfitSheetRepository:
    table_name = "stock_profit_sheet"
    batch_size = 100

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
                security_code,
                security_name_abbr,
                report_date,
                report_type,
                report_date_name,
                total_operate_income,
                netprofit,
                basic_eps,
                total_operate_cost,
                opinion_type
            FROM {self.table_name}
            WHERE security_code = %s
            ORDER BY report_date ASC
        """
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query, [symbol])
            rows = cursor.fetchall() or []
        return pd.DataFrame(rows)

    def exists_by_symbol(self, symbol: str) -> bool:
        query = f"""
            SELECT 1
            FROM {self.table_name}
            WHERE security_code = %s
            LIMIT 1
        """
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query, [symbol])
            row = cursor.fetchone()
        return row is not None

    def delete_by_symbol(self, cursor: Any, symbol: str) -> int:
        query = f"DELETE FROM {self.table_name} WHERE security_code = %s"
        return int(cursor.execute(query, [symbol]))

    def insert_rows(self, cursor: Any, symbol: str, rows: list[dict]) -> int:
        query = f"""
            INSERT INTO {self.table_name} (
                security_code,
                security_name_abbr,
                report_date,
                report_type,
                report_date_name,
                total_operate_income,
                netprofit,
                basic_eps,
                total_operate_cost,
                opinion_type
            ) VALUES (
                %(security_code)s,
                %(security_name_abbr)s,
                %(report_date)s,
                %(report_type)s,
                %(report_date_name)s,
                %(total_operate_income)s,
                %(netprofit)s,
                %(basic_eps)s,
                %(total_operate_cost)s,
                %(opinion_type)s
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
