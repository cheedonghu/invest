from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any

from dbutils.pooled_db import PooledDB

from backend.app.config.database import DatabaseSettings, load_database_settings


logger = logging.getLogger(__name__)


class DatabaseSessionFactory:
    _pools: dict[tuple[Any, ...], PooledDB] = {}

    def __init__(self, settings: DatabaseSettings | None = None) -> None:
        self.settings = settings or load_database_settings()

    @property
    def is_available(self) -> bool:
        return self.settings.is_configured

    @contextmanager
    def connect(self, table_name: str):
        pool = self._get_pool()
        logger.debug(
            "Borrowing DB connection from pool: host=%s port=%s user=%s database=%s table=%s",
            self.settings.host,
            self.settings.port,
            self.settings.user,
            self.settings.database,
            table_name,
        )
        connection = pool.connection()
        try:
            yield connection
        finally:
            connection.close()
            logger.debug(
                "Returned DB connection to pool: host=%s port=%s user=%s database=%s table=%s",
                self.settings.host,
                self.settings.port,
                self.settings.user,
                self.settings.database,
                table_name,
            )

    def _get_pool(self) -> PooledDB:
        if not self.settings.is_configured:
            raise RuntimeError("database settings are not configured")

        key = (
            self.settings.host,
            self.settings.port,
            self.settings.user,
            self.settings.database,
            self.settings.charset,
            self.settings.min_cached,
            self.settings.max_cached,
            self.settings.max_shared,
            self.settings.max_connections,
            self.settings.blocking,
            self.settings.max_usage,
            self.settings.ping,
        )
        pool = self._pools.get(key)
        if pool is not None:
            logger.info(
                "Reusing existing DB pool: host=%s port=%s user=%s database=%s max_connections=%s password_set=%s",
                self.settings.host,
                self.settings.port,
                self.settings.user,
                self.settings.database,
                self.settings.max_connections,
                bool(self.settings.password),
            )
            return pool

        try:
            import pymysql
        except ModuleNotFoundError as exc:
            raise RuntimeError("PyMySQL is not installed") from exc

        logger.info(
            "Initializing DB pool: host=%s port=%s user=%s database=%s charset=%s password_set=%s max_connections=%s min_cached=%s max_cached=%s max_shared=%s blocking=%s max_usage=%s ping=%s",
            self.settings.host,
            self.settings.port,
            self.settings.user,
            self.settings.database,
            self.settings.charset,
            bool(self.settings.password),
            self.settings.max_connections,
            self.settings.min_cached,
            self.settings.max_cached,
            self.settings.max_shared,
            self.settings.blocking,
            self.settings.max_usage,
            self.settings.ping,
        )
        pool = PooledDB(
            creator=pymysql,
            mincached=self.settings.min_cached,
            maxcached=self.settings.max_cached,
            maxshared=self.settings.max_shared,
            maxconnections=self.settings.max_connections,
            blocking=self.settings.blocking,
            maxusage=self.settings.max_usage,
            setsession=list(self.settings.set_session),
            ping=self.settings.ping,
            host=self.settings.host,
            port=self.settings.port,
            user=self.settings.user,
            password=self.settings.password,
            database=self.settings.database,
            charset=self.settings.charset,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )
        self._pools[key] = pool
        return pool
