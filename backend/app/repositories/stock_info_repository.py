from __future__ import annotations

from typing import Any

from backend.app.config.database import DatabaseSettings
from backend.app.repositories.db_session import DatabaseSessionFactory


SELECT_COLUMNS = """
    security_code,
    security_name_abbr,
    mark_reason,
    created_at,
    updated_at
"""


class StockInfoRepository:
    table_name = "stock_info"

    def __init__(
        self,
        settings: DatabaseSettings | None = None,
        session_factory: DatabaseSessionFactory | None = None,
    ) -> None:
        self.session_factory = session_factory or DatabaseSessionFactory(settings)
        self.settings = self.session_factory.settings

    @property
    def is_available(self) -> bool:
        return self.session_factory.is_available

    def connect(self):
        return self.session_factory.connect(self.table_name)

    def fetch_by_code(self, security_code: str) -> dict[str, Any] | None:
        query = f"SELECT {SELECT_COLUMNS} FROM {self.table_name} WHERE security_code = %s LIMIT 1"
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query, [security_code])
            return cursor.fetchone()

    def fetch_marked(self) -> list[dict[str, Any]]:
        query = f"""
            SELECT {SELECT_COLUMNS}
            FROM {self.table_name}
            WHERE mark_reason IS NOT NULL AND mark_reason <> ''
            ORDER BY updated_at DESC, created_at DESC, security_code ASC
        """
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            return list(cursor.fetchall() or [])

    def upsert_name(self, security_code: str, security_name_abbr: str) -> int:
        with self.connect() as connection:
            cursor = connection.cursor()
            affected = self.upsert_name_with_cursor(cursor, security_code, security_name_abbr)
            connection.commit()
        return affected

    def upsert_name_with_cursor(self, cursor: Any, security_code: str, security_name_abbr: str) -> int:
        """供备份事务复用，不自行 commit。"""
        query = f"""
            INSERT INTO {self.table_name} (security_code, security_name_abbr)
            VALUES (%(security_code)s, %(security_name_abbr)s)
            ON DUPLICATE KEY UPDATE security_name_abbr = VALUES(security_name_abbr)
        """
        return int(cursor.execute(query, {
            "security_code": security_code,
            "security_name_abbr": security_name_abbr,
        }))

    def upsert_mark_reason(self, security_code: str, mark_reason: str) -> int:
        query = f"""
            INSERT INTO {self.table_name} (security_code, mark_reason)
            VALUES (%(security_code)s, %(mark_reason)s)
            ON DUPLICATE KEY UPDATE mark_reason = VALUES(mark_reason)
        """
        with self.connect() as connection:
            cursor = connection.cursor()
            affected = int(cursor.execute(query, {
                "security_code": security_code,
                "mark_reason": mark_reason,
            }))
            connection.commit()
        return affected
