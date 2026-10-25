from __future__ import annotations

import sys
import unittest
import asyncio
import json
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from fastapi import FastAPI, HTTPException

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.app.services.stock.marked_stock_service import MarkedStockService
from backend.app.repositories.stock_info_repository import StockInfoRepository


class FakeRepository:
    """内存假仓库，记录所有写操作，绝不连数据库。"""

    def __init__(self, row: dict[str, Any] | None = None) -> None:
        self.is_available = True
        self.upsert_name_calls: list[tuple[str, str]] = []
        self.upsert_mark_reason_calls: list[tuple[str, str]] = []
        self.clear_mark_reason_calls: list[str] = []
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
        return [dict(self._row)] if self._row.get("mark_reason") else []

    def upsert_name(self, security_code: str, name: str) -> int:
        self.upsert_name_calls.append((security_code, name))
        return 1

    def upsert_mark_reason(self, security_code: str, mark_reason: str) -> int:
        self.upsert_mark_reason_calls.append((security_code, mark_reason))
        self._row["mark_reason"] = mark_reason
        return 1

    def clear_mark_reason(self, security_code: str) -> int:
        self.clear_mark_reason_calls.append(security_code)
        self._row["mark_reason"] = None
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


class StockNoteTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = FakeRepository()
        self.coordinator = FakeCoordinator()
        self.service = MarkedStockService(repository=self.repository, coordinator=self.coordinator)

    def test_long_markdown_round_trip_preserves_whitespace(self) -> None:
        note = "    indented code\n\n" + "研究记录  \n" * 200 + "last line  \n"
        result = self.service.upsert_marked_stock("600036", note)
        self.assertEqual(result["mark_reason"], note)
        self.assertEqual(self.repository.upsert_mark_reason_calls, [("600036", note)])

    def test_empty_and_oversized_notes_do_not_write(self) -> None:
        for note in (" \n\t", "研" * (MarkedStockService.MAX_NOTE_LENGTH + 1)):
            with self.subTest(length=len(note)), self.assertRaises(HTTPException) as raised:
                self.service.upsert_marked_stock("600036", note)
            self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(self.repository.upsert_mark_reason_calls, [])
        self.assertEqual(self.coordinator.maybe_sync_calls, [])

    def test_delete_hides_mark_but_keeps_stock_identity_and_can_be_readded(self) -> None:
        original = self.repository.fetch_by_code("600036")
        self.assertEqual(self.service.delete_marked_stock("600036"), {"symbol": "600036"})
        self.assertEqual(self.service.list_marked_stocks(), {"count": 0, "items": []})
        row = self.repository.fetch_by_code("600036")
        self.assertEqual(row["security_name_abbr"], original["security_name_abbr"])
        self.assertEqual(row["created_at"], original["created_at"])
        self.assertIsNone(row["mark_reason"])
        self.assertEqual(self.coordinator.maybe_sync_calls, [])
        self.service.delete_marked_stock("600036")  # 重复删除保持成功。
        result = self.service.upsert_marked_stock("600036", "## 重新关注")
        self.assertEqual(result["mark_reason"], "## 重新关注")
        self.assertEqual(self.service.list_marked_stocks()["count"], 1)

    def test_delete_rejects_invalid_symbol_or_unavailable_database(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            self.service.delete_marked_stock("123")
        self.assertEqual(raised.exception.status_code, 400)
        self.repository.is_available = False
        with self.assertRaises(HTTPException) as raised:
            self.service.delete_marked_stock("600036")
        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(self.repository.clear_mark_reason_calls, [])

    def test_repository_clears_only_note_with_bound_symbol(self) -> None:
        connection = MagicMock()
        connection.cursor.return_value.execute.return_value = 1

        @contextmanager
        def connect(_table):
            yield connection

        factory = MagicMock()
        factory.connect.side_effect = connect
        repository = StockInfoRepository(session_factory=factory)
        self.assertEqual(repository.clear_mark_reason("600036"), 1)
        query, params = connection.cursor.return_value.execute.call_args.args
        self.assertEqual(" ".join(query.split()), "UPDATE stock_info SET mark_reason = NULL WHERE security_code = %s")
        self.assertEqual(params, ["600036"])
        connection.commit.assert_called_once()


class StockNoteApiTestCase(unittest.TestCase):
    def test_note_http_lifecycle_and_delete_errors(self) -> None:
        from backend.app.controllers import marked_stock_controller as controller

        repository = FakeRepository()
        service = MarkedStockService(repository=repository, coordinator=FakeCoordinator())
        app = FastAPI()
        app.include_router(controller.router)

        async def request(method: str, path: str, payload=None):
            messages = []
            body = json.dumps(payload).encode() if payload is not None else b""

            async def receive():
                return {"type": "http.request", "body": body, "more_body": False}

            async def send(message):
                messages.append(message)

            await app({"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
                       "method": method, "path": path, "query_string": b"",
                       "headers": [(b"content-type", b"application/json")]}, receive, send)
            status = next(message["status"] for message in messages if message["type"] == "http.response.start")
            content = b"".join(message.get("body", b"") for message in messages if message["type"] == "http.response.body")
            return status, json.loads(content)

        with patch.object(controller, "marked_stock_service", service):
            note = "    code\n\n" + "研究记录  \n" * 200
            status, result = asyncio.run(request("POST", "/api/stocks/marked/", {"symbol": "600036", "mark_reason": note}))
            self.assertEqual(status, 200)
            self.assertEqual(result["mark_reason"], note)
            self.assertEqual(asyncio.run(request("DELETE", "/api/stocks/marked/600036")), (200, {"symbol": "600036"}))
            self.assertEqual(asyncio.run(request("GET", "/api/stocks/marked/")), (200, {"count": 0, "items": []}))
            self.assertEqual(asyncio.run(request("DELETE", "/api/stocks/marked/123"))[0], 400)
            repository.is_available = False
            self.assertEqual(asyncio.run(request("DELETE", "/api/stocks/marked/600036"))[0], 503)


if __name__ == "__main__":
    unittest.main()
