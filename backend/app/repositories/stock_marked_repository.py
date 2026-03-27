from __future__ import annotations

from typing import Any

from backend.app.config.database import DatabaseSettings
from backend.app.repositories.db_session import DatabaseSessionFactory


class StockMarkedRepository:
    table_name = "stock_marked"

    def __init__(self, settings: DatabaseSettings | None = None, session_factory: DatabaseSessionFactory | None = None) -> None:
        self.session_factory = session_factory or DatabaseSessionFactory(settings)
        self.settings = self.session_factory.settings

    @property
    def is_available(self) -> bool:
        return self.session_factory.is_available

    def connect(self):
        return self.session_factory.connect(self.table_name)

    def fetch_all(self) -> list[dict[str, Any]]:
        query = f"""
            SELECT
                security_code AS symbol,
                security_name_abbr AS name,
                mark_reason,
                created_at,
                updated_at
            FROM {self.table_name}
            ORDER BY updated_at DESC, created_at DESC, security_code ASC
        """
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall() or []
        return list(rows)

    def fetch_by_symbol(self, symbol: str) -> dict[str, Any] | None:
        query = f"""
            SELECT
                security_code AS symbol,
                security_name_abbr AS name,
                mark_reason,
                created_at,
                updated_at
            FROM {self.table_name}
            WHERE security_code = %s
            LIMIT 1
        """
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query, [symbol])
            row = cursor.fetchone()
        return row

    def upsert(self, symbol: str, name: str, mark_reason: str) -> int:
        query = f"""
            INSERT INTO {self.table_name} (
                security_code,
                security_name_abbr,
                mark_reason,
                created_at,
                updated_at
            ) VALUES (
                %(symbol)s,
                %(name)s,
                %(mark_reason)s,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            )
            ON DUPLICATE KEY UPDATE
                security_name_abbr = VALUES(security_name_abbr),
                mark_reason = VALUES(mark_reason),
                updated_at = CURRENT_TIMESTAMP
        """
        params = {
            "symbol": symbol,
            "name": name,
            "mark_reason": mark_reason,
        }
        with self.connect() as connection:
            cursor = connection.cursor()
            affected = int(cursor.execute(query, params))
            connection.commit()
        return affected
