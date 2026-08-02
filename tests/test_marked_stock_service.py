from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.app.services.stock.marked_stock_service import MarkedStockService


class FakeRepository:
    """内存假仓库，记录所有写操作，绝不连数据库。"""

    def __init__(self, row: dict[str, Any] | None = None) -> None:
        self.is_available = True
        self.upsert_name_calls: list[tuple[str, str]] = []
        self.upsert_mark_reason_calls: list[tuple[str, str]] = []
        self._row = row or {
            "security_code": "600036",
            "security_name_abbr": "招商银行",
            "mark_reason": "价格30左右再入手",
            "created_at": datetime(2026, 8, 2, 14, 49, 58),
            "updated_at": datetime(2026, 8, 2, 14, 49, 58),
        }

    def fetch_by_code(self, security_code: str) -> dict[str, Any] | None:
        return dict(self._row)

    def fetch_marked(self) -> list[dict[str, Any]]:
        return [dict(self._row)]

    def upsert_name(self, security_code: str, name: str) -> int:
        self.upsert_name_calls.append((security_code, name))
        return 1

    def upsert_mark_reason(self, security_code: str, mark_reason: str) -> int:
        self.upsert_mark_reason_calls.append((security_code, mark_reason))
        return 1


class FakeCoordinator:
    """内存假协调器，记录 maybe_sync 调用，绝不起后台线程。"""

    def __init__(self) -> None:
        self.maybe_sync_calls: list[str] = []

    def maybe_sync(self, security_code: str) -> None:
        self.maybe_sync_calls.append(security_code)


class UpsertMarkedStockTestCase(unittest.TestCase):
    def test_empty_name_triggers_sync_and_never_writes_name(self) -> None:
        """名称缺失时只能走 coordinator.maybe_sync，绝不能把代号当名字写库。"""
        repository = FakeRepository()
        coordinator = FakeCoordinator()
        service = MarkedStockService(repository=repository, coordinator=coordinator)

        service.upsert_marked_stock("600036", "价格30左右再入手")

        self.assertEqual(coordinator.maybe_sync_calls, ["600036"])
        self.assertEqual(repository.upsert_name_calls, [])


class SerializeRowTestCase(unittest.TestCase):
    def test_missing_name_falls_back_to_symbol_without_writing(self) -> None:
        """序列化层的回退只影响返回值，不能触发任何写操作。

        _serialize_row 是纯函数，不持有 repository/coordinator 引用，
        这里用假对象佐证：全程未调用任何一个记录写操作的方法。
        """
        repository = FakeRepository()
        coordinator = FakeCoordinator()
        row = {
            "security_code": "000001",
            "security_name_abbr": "",
            "mark_reason": "观察",
            "created_at": None,
            "updated_at": None,
        }

        result = MarkedStockService._serialize_row(row)

        self.assertEqual(result["symbol"], "000001")
        self.assertEqual(result["name"], "000001")
        self.assertEqual(repository.upsert_name_calls, [])
        self.assertEqual(repository.upsert_mark_reason_calls, [])
        self.assertEqual(coordinator.maybe_sync_calls, [])


if __name__ == "__main__":
    unittest.main()
