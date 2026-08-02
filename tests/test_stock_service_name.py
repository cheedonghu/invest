from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.app.services.stock.stock_service import StockService


class FakeStockInfoRepository:
    """内存假仓库，记录所有写方法调用，绝不连数据库。"""

    def __init__(self, row: dict[str, Any] | None = None) -> None:
        self._row = row
        self.upsert_name_calls: list[tuple[str, str]] = []
        self.upsert_name_with_cursor_calls: list[tuple[str, str]] = []
        self.upsert_mark_reason_calls: list[tuple[str, str]] = []

    def fetch_by_code(self, security_code: str) -> dict[str, Any] | None:
        return dict(self._row) if self._row is not None else None

    def upsert_name(self, security_code: str, security_name_abbr: str) -> int:
        self.upsert_name_calls.append((security_code, security_name_abbr))
        return 1

    def upsert_name_with_cursor(self, cursor: Any, security_code: str, security_name_abbr: str) -> int:
        self.upsert_name_with_cursor_calls.append((security_code, security_name_abbr))
        return 1

    def upsert_mark_reason(self, security_code: str, mark_reason: str) -> int:
        self.upsert_mark_reason_calls.append((security_code, mark_reason))
        return 1


class FakeCoordinator:
    """内存假协调器，记录 maybe_sync 调用，绝不起后台线程、绝不连数据库。"""

    def __init__(self) -> None:
        self.maybe_sync_calls: list[str] = []

    def maybe_sync(self, security_code: str) -> None:
        self.maybe_sync_calls.append(security_code)


def _build_service(row: dict[str, Any] | None) -> tuple[StockService, FakeStockInfoRepository, FakeCoordinator]:
    """构造 StockService 后直接替换掉两个会触碰真实资源的属性。

    StockService.__init__ 会构造若干 provider/repository，但它们在构造期
    只是保存配置，不做任何真实连接（参见 DatabaseSessionFactory.__init__
    只 load_database_settings，不建连接池）。这里仍然选择在构造后立刻替换
    掉 stock_info_repository 与 sync_coordinator，确保本测试100%不触碰
    数据库或网络。
    """
    service = StockService()
    repository = FakeStockInfoRepository(row)
    coordinator = FakeCoordinator()
    service.stock_info_repository = repository
    service.sync_coordinator = coordinator
    return service, repository, coordinator


class GetStockNameTestCase(unittest.TestCase):
    def test_name_present_returns_stock_info_source(self) -> None:
        row = {
            "security_code": "600519",
            "security_name_abbr": "贵州茅台",
            "mark_reason": None,
            "created_at": None,
            "updated_at": None,
        }
        service, repository, coordinator = _build_service(row)

        result = service.get_stock_name("600519")

        self.assertEqual(result["symbol"], "600519")
        self.assertEqual(result["name"], "贵州茅台")
        self.assertEqual(result["source"], "stock_info")
        self.assertEqual(coordinator.maybe_sync_calls, ["600519"])

    def test_missing_row_falls_back_to_symbol_without_writing(self) -> None:
        """Bug 1 的核心保证：名称缺失时只回退展示，绝不写库。"""
        service, repository, coordinator = _build_service(row=None)

        result = service.get_stock_name("600519")

        self.assertEqual(result["symbol"], "600519")
        self.assertEqual(result["name"], "600519")
        self.assertEqual(result["source"], "fallback")
        self.assertEqual(coordinator.maybe_sync_calls, ["600519"])

        # 没有任何写方法被调用过。
        self.assertEqual(repository.upsert_name_calls, [])
        self.assertEqual(repository.upsert_name_with_cursor_calls, [])
        self.assertEqual(repository.upsert_mark_reason_calls, [])


if __name__ == "__main__":
    unittest.main()
