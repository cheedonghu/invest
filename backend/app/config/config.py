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


# @dataclass(frozen=True)
class Config:

    def __init__(self, config_path: str | os.PathLike[str] | None = None):
        resolved_path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
        self.config: dict[str, Any] = _load_settings_from_toml(resolved_path)
    
    @property
    def database(self) -> dict[str, Any]:
        return self.config.get("database", {})

    @property
    def pool(self) -> dict[str, Any]:
        return self.config.get("pool", {})

    @property
    def agent(self) -> dict[str, Any]:
        return self.config.get("agent", {})


def _load_settings_from_toml(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        logger.warning("config file not found: %s", str(config_path))
        return {}

    with config_path.open("rb") as file:
        payload = tomllib.load(file)


    return  {
        "database": payload.get("database") or {},
        "pool": payload.get("pool") or {},
        "agent": payload.get("agent") or {},
    }

    
# 在文件末尾添加
_config_instance = Config()

# 导出实例
config = _config_instance


if __name__ == '__main__':
    try:
        config = Config()
        print(config.database["host"])

    except ValueError as e:
        print(e)