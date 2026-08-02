from __future__ import annotations

from datetime import date
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

    def fetch_by_security_code(self, security_code: str) -> pd.DataFrame:
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
            cursor.execute(query, [security_code])
            rows = cursor.fetchall() or []
        return pd.DataFrame(rows)

    def latest_report_date(self, security_code: str) -> date | None:
        query = f"SELECT MAX(report_date) AS latest_report_date FROM {self.table_name} WHERE security_code = %s"
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query, [security_code])
            row = cursor.fetchone() or {}
        return row.get("latest_report_date")

    def upsert_rows(self, cursor: Any, security_code: str, rows: list[dict]) -> int:
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
            ON DUPLICATE KEY UPDATE
                security_name_abbr = VALUES(security_name_abbr),
                report_type = VALUES(report_type),
                report_date_name = VALUES(report_date_name),
                total_operate_income = VALUES(total_operate_income),
                netprofit = VALUES(netprofit),
                basic_eps = VALUES(basic_eps),
                total_operate_cost = VALUES(total_operate_cost),
                opinion_type = VALUES(opinion_type)
        """
        return self._upsert_in_batches(cursor, security_code, query, rows)

    def _upsert_in_batches(self, cursor: Any, security_code: str, query: str, rows: list[dict]) -> int:
        if not rows:
            logger.info("[%s] no rows to insert for %s", self.table_name, security_code)
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
                security_code,
                batch_index,
                batch_count,
                start + 1,
                start + len(batch),
                total_rows,
                affected,
            )
        return total_affected
