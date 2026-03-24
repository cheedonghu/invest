from __future__ import annotations

import logging
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = CONFIG_DIR / "config.toml"
EXAMPLE_CONFIG_PATH = CONFIG_DIR / "config.toml.example"


@dataclass(frozen=True)
class DatabaseSettings:
    host: str = ""
    port: int = 3306
    user: str = ""
    password: str = ""
    database: str = ""
    charset: str = "utf8mb4"
    min_cached: int = 1
    max_cached: int = 5
    max_shared: int = 5
    max_connections: int = 10
    blocking: bool = True
    max_usage: int | None = None
    set_session: tuple[str, ...] = ()
    ping: int = 1

    @property
    def is_configured(self) -> bool:
        return all([self.host, self.user, self.database])


def load_database_settings(config_path: str | os.PathLike[str] | None = None) -> DatabaseSettings:
    resolved_path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    file_settings = _load_settings_from_toml(resolved_path)
    env_settings = _load_settings_from_env()
    merged = {**file_settings, **env_settings}
    merged["set_session"] = tuple(merged.get("set_session") or ())
    settings = DatabaseSettings(**merged)

    logger.info(
        "Loaded database settings: config_path=%s toml_keys=%s env_override_keys=%s host=%s port=%s user=%s database=%s charset=%s password_set=%s pool[min=%s,max=%s,shared=%s,connections=%s,blocking=%s,max_usage=%s,ping=%s]",
        str(resolved_path),
        sorted(file_settings.keys()),
        sorted(env_settings.keys()),
        settings.host,
        settings.port,
        settings.user,
        settings.database,
        settings.charset,
        bool(settings.password),
        settings.min_cached,
        settings.max_cached,
        settings.max_shared,
        settings.max_connections,
        settings.blocking,
        settings.max_usage,
        settings.ping,
    )
    return settings


def _load_settings_from_toml(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        logger.warning("Database TOML config file not found: %s", str(config_path))
        return {}

    with config_path.open("rb") as file:
        payload = tomllib.load(file)

    database = payload.get("database") or {}
    pool = payload.get("pool") or {}
    result: dict[str, Any] = {}

    for key in ["host", "port", "user", "password", "database", "charset"]:
        value = database.get(key)
        if value not in (None, ""):
            result[key] = value

    for key in ["min_cached", "max_cached", "max_shared", "max_connections", "blocking", "max_usage", "ping"]:
        value = pool.get(key)
        if value is not None:
            result[key] = value

    if pool.get("set_session") is not None:
        result["set_session"] = pool.get("set_session")
    return result


def _load_settings_from_env() -> dict[str, Any]:
    mapping: list[tuple[str, str, callable]] = [
        ("STOCK_DB_HOST", "host", str),
        ("STOCK_DB_PORT", "port", int),
        ("STOCK_DB_USER", "user", str),
        ("STOCK_DB_PASSWORD", "password", str),
        ("STOCK_DB_NAME", "database", str),
        ("STOCK_DB_CHARSET", "charset", str),
        ("STOCK_DB_POOL_MIN_CACHED", "min_cached", int),
        ("STOCK_DB_POOL_MAX_CACHED", "max_cached", int),
        ("STOCK_DB_POOL_MAX_SHARED", "max_shared", int),
        ("STOCK_DB_POOL_MAX_CONNECTIONS", "max_connections", int),
        ("STOCK_DB_POOL_BLOCKING", "blocking", lambda value: value.lower() == "true"),
        ("STOCK_DB_POOL_MAX_USAGE", "max_usage", lambda value: int(value) if value else None),
        ("STOCK_DB_POOL_PING", "ping", int),
    ]
    result: dict[str, Any] = {}
    for env_key, field_name, caster in mapping:
        raw = os.getenv(env_key)
        if raw in (None, ""):
            continue
        result[field_name] = caster(raw)

    raw_set_session = os.getenv("STOCK_DB_POOL_SET_SESSION")
    if raw_set_session:
        result["set_session"] = [item.strip() for item in raw_set_session.split(";") if item.strip()]
    return result
