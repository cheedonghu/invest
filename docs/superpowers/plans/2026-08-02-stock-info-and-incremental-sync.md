# 股票元数据合并与数据增量更新 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新建 `stock_info` 表统一承载股票元数据，名称改由数据备份流程写入；估值日线与利润表各自按水位线增量更新，取代「有数据就永不更新」的现状。

**Architecture:** 日线用「工作日粗筛 + 真实交易日历确认」两级门控判定新鲜度；利润表用 1 次轻量请求探测上游最新报告期后再决定是否全量拉取。写入全部改为 `ON DUPLICATE KEY UPDATE`。同步在后台线程执行，用进程内 in-flight 集合去重。

**Tech Stack:** Python 3.11、FastAPI 0.135.3、akshare 1.18.47、PyMySQL + DBUtils 连接池、unittest（**本项目无 pytest**）

## Global Constraints

- 测试框架为 **unittest**，运行方式 `python -m unittest tests.<module> -v`，一律从项目根目录 `D:\files\project\invest` 执行
- 所有新文件用 **UTF-8 无 BOM**。本仓库刚清理过 BOM 污染，不要用会写入 BOM 的方式创建文件
- 所有模块首行为 `from __future__ import annotations`，与现有代码一致
- 数据库列名统一采用东财/akshare 命名：`security_code`、`security_name_abbr`、`report_date`
- 6 位 A 股代码的规范化沿用现有 `normalize_symbol`：`re.sub(r"[^0-9]", "", s)` 后必须为 6 位，否则 HTTPException 400
- 上游主机 `push2*.eastmoney.com` **已被封禁不可用**，不得引入依赖该主机的接口；`datacenter-web.eastmoney.com` 与 `emweb.securities.eastmoney.com` 正常
- 每个任务结束时提交，提交信息用中文，结尾附 `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`

---

## 文件结构

| 文件 | 职责 |
|---|---|
| `db/migrations/2026-08-02-stock-info.sql` | 建表/改表 DDL，手工执行 |
| `backend/app/services/stock/trade_calendar.py` | 两级日期判定，纯计算 + 日历缓存 |
| `backend/app/repositories/stock_info_repository.py` | `stock_info` 表读写 |
| `backend/app/providers/profit_sheet/eastmoney_report_period_provider.py` | 轻量探测上游最新报告期 |
| `backend/app/services/stock/stock_sync_coordinator.py` | 新鲜度门控 + in-flight 去重 + 起线程 |
| `backend/app/services/stock/stock_backup_service.py` | 拆为 `sync_valuation` / `sync_profit` |
| `backend/app/repositories/stock_value_daily_repository.py` | 列名改造 + `upsert_rows` |
| `backend/app/repositories/stock_profit_sheet_repository.py` | `latest_report_date` + `upsert_rows` |
| `backend/app/controllers/ops_controller.py` | 两个强制同步端点 |

---

### Task 1: 数据库 DDL

**Files:**
- Create: `db/migrations/2026-08-02-stock-info.sql`

**Interfaces:**
- Produces: 表 `stock_info(security_code, security_name_abbr, mark_reason, created_at, updated_at)`；表 `stock_value_daily` 首列由 `symbol` 改名为 `security_code`

- [ ] **Step 1: 写 DDL 文件**

```sql
-- 2026-08-02 股票元数据合并与增量更新
-- 执行前请确认：stock_marked 与 stock_value_daily 的历史数据将被丢弃（已确认可丢弃）

-- 1. 新建 stock_info，合并原 stock_marked
CREATE TABLE IF NOT EXISTS stock_info (
  security_code       VARCHAR(10)  NOT NULL,
  security_name_abbr  VARCHAR(50)  NULL,
  mark_reason         VARCHAR(500) NULL,
  created_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (security_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. 用现有利润表数据播种名称
INSERT INTO stock_info (security_code, security_name_abbr, created_at, updated_at)
SELECT DISTINCT security_code, security_name_abbr, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM stock_profit_sheet
WHERE security_name_abbr IS NOT NULL AND security_name_abbr <> ''
ON DUPLICATE KEY UPDATE security_name_abbr = VALUES(security_name_abbr);

-- 3. 重建 stock_value_daily，列名 symbol -> security_code，数据丢弃
DROP TABLE IF EXISTS stock_value_daily;
CREATE TABLE stock_value_daily (
  security_code       VARCHAR(10)    NOT NULL,
  trade_date          DATE           NOT NULL,
  close_price         DECIMAL(18,4)  NULL,
  pct_change          DECIMAL(10,4)  NULL,
  total_market_value  DECIMAL(20,2)  NULL,
  float_market_value  DECIMAL(20,2)  NULL,
  total_shares        BIGINT         NULL,
  float_shares        BIGINT         NULL,
  pe_ttm              DECIMAL(18,6)  NULL,
  pe_static           DECIMAL(18,6)  NULL,
  pb                  DECIMAL(18,6)  NULL,
  peg                 DECIMAL(18,6)  NULL,
  pcf                 DECIMAL(18,6)  NULL,
  ps                  DECIMAL(18,6)  NULL,
  created_at          DATE           NULL,
  updated_at          DATE           NULL,
  PRIMARY KEY (security_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. 丢弃旧表
DROP TABLE IF EXISTS stock_marked;
```

- [ ] **Step 2: 执行 DDL**

用你的 MySQL 客户端对配置库执行上述脚本。执行后运行下面的验证：

```bash
python -c "
import logging; logging.disable(logging.CRITICAL)
from backend.app.repositories.db_session import DatabaseSessionFactory
with DatabaseSessionFactory().connect('verify') as c:
    cur = c.cursor()
    cur.execute('SHOW COLUMNS FROM stock_info'); print('stock_info:', [r['Field'] for r in cur.fetchall()])
    cur.execute('SHOW COLUMNS FROM stock_value_daily'); print('stock_value_daily:', [r['Field'] for r in cur.fetchall()])
    cur.execute('SELECT COUNT(*) n FROM stock_info'); print('stock_info 播种行数:', cur.fetchone()['n'])
"
```

Expected:
```
stock_info: ['security_code', 'security_name_abbr', 'mark_reason', 'created_at', 'updated_at']
stock_value_daily: ['security_code', 'trade_date', ...]
stock_info 播种行数: 22
```

- [ ] **Step 3: 提交**

```bash
git add db/migrations/2026-08-02-stock-info.sql
git commit -m "feat: 新增 stock_info 建表与 stock_value_daily 列名改造 DDL

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: TradeCalendar 两级门控

**Files:**
- Create: `backend/app/services/stock/trade_calendar.py`
- Test: `tests/test_trade_calendar.py`

**Interfaces:**
- Produces:
  - `latest_workday(today: date) -> date`（模块级函数）
  - `TradeCalendar(loader: Callable[[], list[date]] | None = None)`
  - `TradeCalendar.latest_trade_day(today: date) -> date | None`
  - `TradeCalendar.is_stale(watermark: date | None, today: date) -> bool`

- [ ] **Step 1: 写失败测试**

创建 `tests/test_trade_calendar.py`：

```python
from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.app.services.stock.trade_calendar import TradeCalendar, latest_workday


class LatestWorkdayTestCase(unittest.TestCase):
    def test_weekday_returns_itself(self) -> None:
        # 2026-07-27 周一 ~ 2026-07-31 周五
        for day in range(27, 32):
            with self.subTest(day=day):
                self.assertEqual(latest_workday(date(2026, 7, day)), date(2026, 7, day))

    def test_saturday_falls_back_to_friday(self) -> None:
        self.assertEqual(latest_workday(date(2026, 8, 1)), date(2026, 7, 31))

    def test_sunday_falls_back_to_friday(self) -> None:
        self.assertEqual(latest_workday(date(2026, 8, 2)), date(2026, 7, 31))


class IsStaleTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.calls = 0

        def loader() -> list[date]:
            self.calls += 1
            # 2026-07-27 ~ 07-31 为交易日，08-01/02 为周末
            return [date(2026, 7, d) for d in (27, 28, 29, 30, 31)]

        self.calendar = TradeCalendar(loader=loader)

    def test_no_watermark_is_stale(self) -> None:
        self.assertTrue(self.calendar.is_stale(None, date(2026, 8, 2)))

    def test_watermark_at_latest_workday_is_fresh(self) -> None:
        self.assertFalse(self.calendar.is_stale(date(2026, 7, 31), date(2026, 8, 2)))

    def test_first_gate_pass_does_not_load_calendar(self) -> None:
        """一级放行时绝不能触碰日历——这是两级门控的核心价值。"""
        self.calendar.is_stale(date(2026, 7, 31), date(2026, 8, 2))
        self.assertEqual(self.calls, 0)

    def test_stale_watermark_triggers_second_gate(self) -> None:
        self.assertTrue(self.calendar.is_stale(date(2026, 3, 20), date(2026, 8, 2)))
        self.assertEqual(self.calls, 1)

    def test_holiday_window_is_not_stale(self) -> None:
        """8/3 周一若为假日（不在交易日历中），水位线 7/31 仍应判为新鲜。"""
        self.assertFalse(self.calendar.is_stale(date(2026, 7, 31), date(2026, 8, 3)))

    def test_calendar_failure_degrades_to_first_gate(self) -> None:
        def boom() -> list[date]:
            raise RuntimeError("calendar source down")

        calendar = TradeCalendar(loader=boom)
        # 一级已判陈旧，日历不可用时保持陈旧结论（宁可多拉一次）
        self.assertTrue(calendar.is_stale(date(2026, 3, 20), date(2026, 8, 2)))

    def test_calendar_loaded_once_per_day(self) -> None:
        self.calendar.is_stale(date(2026, 3, 20), date(2026, 8, 2))
        self.calendar.is_stale(date(2026, 3, 21), date(2026, 8, 2))
        self.assertEqual(self.calls, 1)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_trade_calendar -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'backend.app.services.stock.trade_calendar'`

- [ ] **Step 3: 实现**

创建 `backend/app/services/stock/trade_calendar.py`：

```python
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Callable

import akshare as ak
import pandas as pd


logger = logging.getLogger(__name__)


def latest_workday(today: date) -> date:
    """回退到最近的周一~周五。纯本地计算，不含节假日信息。"""
    cursor = today
    while cursor.weekday() >= 5:  # 5=周六 6=周日
        cursor -= timedelta(days=1)
    return cursor


def _load_from_akshare() -> list[date]:
    df = ak.tool_trade_date_hist_sina()
    series = pd.to_datetime(df.iloc[:, 0], errors="coerce").dropna()
    return sorted(value.date() for value in series)


class TradeCalendar:
    """两级新鲜度门控。

    一级 latest_workday 纯本地、零网络；二级 latest_trade_day 使用真实交易日历。
    交易日集合是工作日集合的子集，故 latest_trade_day <= latest_workday，
    一级判定为新鲜时必然真的新鲜，不会漏判。
    """

    def __init__(self, loader: Callable[[], list[date]] | None = None) -> None:
        self._loader = loader or _load_from_akshare
        self._cached_days: list[date] | None = None
        self._cached_on: date | None = None

    def latest_trade_day(self, today: date) -> date | None:
        past = [day for day in self._trade_days(today) if day <= today]
        return past[-1] if past else None

    def is_stale(self, watermark: date | None, today: date) -> bool:
        if watermark is None:
            return True
        if watermark >= latest_workday(today):
            return False  # 一级放行，不触碰日历

        try:
            trade_day = self.latest_trade_day(today)
        except Exception as exc:
            logger.warning("trade calendar unavailable, degrade to workday gate: %s", exc)
            return True  # 一级已判陈旧，日历不可用时保持该结论

        if trade_day is None:
            return True
        return watermark < trade_day

    def _trade_days(self, today: date) -> list[date]:
        if self._cached_days is not None and self._cached_on == today:
            return self._cached_days
        days = self._loader()
        self._cached_days = days
        self._cached_on = today
        return days
```

- [ ] **Step 4: 运行测试确认通过**

Run: `python -m unittest tests.test_trade_calendar -v`
Expected: PASS，10 个测试全绿

- [ ] **Step 5: 提交**

```bash
git add backend/app/services/stock/trade_calendar.py tests/test_trade_calendar.py
git commit -m "feat: 新增两级门控的交易日新鲜度判定

一级用工作日粗筛，纯本地零网络；二级用真实交易日历确认，避开节假日误判。
日历进程内按天缓存，拉取失败时降级为一级判定。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 3: StockInfoRepository

**Files:**
- Create: `backend/app/repositories/stock_info_repository.py`
- Test: `tests/test_stock_info_repository.py`

**Interfaces:**
- Consumes: `DatabaseSessionFactory`（`backend.app.repositories.db_session`）
- Produces:
  - `StockInfoRepository(settings=None, session_factory=None)`
  - `.table_name -> "stock_info"`
  - `.is_available -> bool`
  - `.fetch_by_code(security_code: str) -> dict[str, Any] | None`
  - `.fetch_marked() -> list[dict[str, Any]]`
  - `.upsert_name(security_code: str, security_name_abbr: str) -> int`
  - `.upsert_name_with_cursor(cursor, security_code: str, security_name_abbr: str) -> int`
  - `.upsert_mark_reason(security_code: str, mark_reason: str) -> int`

行 dict 的键：`security_code`、`security_name_abbr`、`mark_reason`、`created_at`、`updated_at`

- [ ] **Step 1: 写失败测试**

创建 `tests/test_stock_info_repository.py`：

```python
from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.app.repositories.stock_info_repository import StockInfoRepository

TEST_CODE = "000001"


class StockInfoRepositoryTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = StockInfoRepository()
        if not self.repository.is_available:
            self.skipTest("database settings are not configured")
        self._cleanup()

    def tearDown(self) -> None:
        self._cleanup()

    def _cleanup(self) -> None:
        with self.repository.connect() as connection:
            cursor = connection.cursor()
            cursor.execute("DELETE FROM stock_info WHERE security_code = %s", [TEST_CODE])
            connection.commit()

    def test_fetch_missing_returns_none(self) -> None:
        self.assertIsNone(self.repository.fetch_by_code(TEST_CODE))

    def test_upsert_name_then_fetch(self) -> None:
        self.repository.upsert_name(TEST_CODE, "平安银行")
        row = self.repository.fetch_by_code(TEST_CODE)
        self.assertIsNotNone(row)
        self.assertEqual(row["security_name_abbr"], "平安银行")
        self.assertIsNone(row["mark_reason"])

    def test_upsert_mark_reason_keeps_existing_name(self) -> None:
        self.repository.upsert_name(TEST_CODE, "平安银行")
        self.repository.upsert_mark_reason(TEST_CODE, "银行股观察")
        row = self.repository.fetch_by_code(TEST_CODE)
        self.assertEqual(row["security_name_abbr"], "平安银行")
        self.assertEqual(row["mark_reason"], "银行股观察")

    def test_upsert_name_keeps_existing_mark_reason(self) -> None:
        self.repository.upsert_mark_reason(TEST_CODE, "银行股观察")
        self.repository.upsert_name(TEST_CODE, "平安银行")
        row = self.repository.fetch_by_code(TEST_CODE)
        self.assertEqual(row["mark_reason"], "银行股观察")
        self.assertEqual(row["security_name_abbr"], "平安银行")

    def test_mark_without_prior_data_has_null_name(self) -> None:
        """标记一只从未拉过数据的股票，名称应为 NULL 等待回填。"""
        self.repository.upsert_mark_reason(TEST_CODE, "先标记再拉数据")
        row = self.repository.fetch_by_code(TEST_CODE)
        self.assertIsNone(row["security_name_abbr"])

    def test_fetch_marked_only_returns_marked(self) -> None:
        self.repository.upsert_name(TEST_CODE, "平安银行")
        codes = [item["security_code"] for item in self.repository.fetch_marked()]
        self.assertNotIn(TEST_CODE, codes)

        self.repository.upsert_mark_reason(TEST_CODE, "银行股观察")
        codes = [item["security_code"] for item in self.repository.fetch_marked()]
        self.assertIn(TEST_CODE, codes)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_stock_info_repository -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'backend.app.repositories.stock_info_repository'`

- [ ] **Step 3: 实现**

创建 `backend/app/repositories/stock_info_repository.py`：

```python
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
```

- [ ] **Step 4: 运行测试确认通过**

Run: `python -m unittest tests.test_stock_info_repository -v`
Expected: PASS，6 个测试全绿

- [ ] **Step 5: 提交**

```bash
git add backend/app/repositories/stock_info_repository.py tests/test_stock_info_repository.py
git commit -m "feat: 新增 stock_info 仓储

名称与标记原因分别 upsert，互不覆盖：备份流程只写名称，
标记接口只写原因，两者可按任意顺序到达。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: StockValueDailyRepository 列名改造与 upsert

**Files:**
- Modify: `backend/app/repositories/stock_value_daily_repository.py`
- Modify: `backend/app/providers/market_performance/database_market_performance_provider.py:38-56`
- Modify: `tests/test_stock_value_daily_repository.py`
- Test: `tests/test_stock_value_daily_upsert.py`

**Interfaces:**
- Produces:
  - `StockValueDailyRepository.latest_trade_date(security_code: str) -> date | None`（签名不变，参数改名）
  - `StockValueDailyRepository.upsert_rows(cursor, security_code: str, rows: list[dict]) -> int`
  - 行 dict 键：`security_code`、`trade_date`、`close_price`、`pct_change`、`total_market_value`、`float_market_value`、`total_shares`、`float_shares`、`pe_ttm`、`pe_static`、`pb`、`peg`、`pcf`、`ps`
  - `delete_by_symbol` 与 `insert_rows` **删除**

- [ ] **Step 1: 写失败测试**

创建 `tests/test_stock_value_daily_upsert.py`：

```python
from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.app.repositories.stock_value_daily_repository import StockValueDailyRepository

TEST_CODE = "000002"


def _row(trade_date: date, close_price: float) -> dict:
    return {
        "security_code": TEST_CODE,
        "trade_date": trade_date,
        "close_price": close_price,
        "pct_change": 1.5,
        "total_market_value": 1000.0,
        "float_market_value": 900.0,
        "total_shares": 100,
        "float_shares": 90,
        "pe_ttm": 10.0,
        "pe_static": 11.0,
        "pb": 1.2,
        "peg": 0.8,
        "pcf": 5.0,
        "ps": 2.0,
    }


class StockValueDailyUpsertTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = StockValueDailyRepository()
        if not self.repository.is_available:
            self.skipTest("database settings are not configured")
        self._cleanup()

    def tearDown(self) -> None:
        self._cleanup()

    def _cleanup(self) -> None:
        with self.repository.connect() as connection:
            cursor = connection.cursor()
            cursor.execute("DELETE FROM stock_value_daily WHERE security_code = %s", [TEST_CODE])
            connection.commit()

    def _upsert(self, rows: list[dict]) -> int:
        with self.repository.connect() as connection:
            cursor = connection.cursor()
            affected = self.repository.upsert_rows(cursor, TEST_CODE, rows)
            connection.commit()
        return affected

    def test_upsert_is_idempotent(self) -> None:
        rows = [_row(date(2026, 7, 30), 10.0), _row(date(2026, 7, 31), 11.0)]
        self._upsert(rows)
        self.assertEqual(self._upsert(rows), 0, "重复写入相同数据不应产生变更")

    def test_upsert_updates_changed_value(self) -> None:
        self._upsert([_row(date(2026, 7, 31), 10.0)])
        self._upsert([_row(date(2026, 7, 31), 12.5)])
        df = self.repository.fetch_by_security_code(TEST_CODE)
        self.assertEqual(len(df), 1, "同一交易日不应产生重复行")
        self.assertAlmostEqual(float(df.iloc[0]["close_price"]), 12.5, places=4)

    def test_upsert_appends_new_dates_only(self) -> None:
        self._upsert([_row(date(2026, 7, 30), 10.0)])
        self._upsert([_row(date(2026, 7, 30), 10.0), _row(date(2026, 7, 31), 11.0)])
        self.assertEqual(len(self.repository.fetch_by_security_code(TEST_CODE)), 2)

    def test_latest_trade_date(self) -> None:
        self.assertIsNone(self.repository.latest_trade_date(TEST_CODE))
        self._upsert([_row(date(2026, 7, 30), 10.0), _row(date(2026, 7, 31), 11.0)])
        self.assertEqual(self.repository.latest_trade_date(TEST_CODE), date(2026, 7, 31))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_stock_value_daily_upsert -v`
Expected: FAIL — `AttributeError: 'StockValueDailyRepository' object has no attribute 'upsert_rows'`

- [ ] **Step 3: 改造仓储**

在 `backend/app/repositories/stock_value_daily_repository.py` 中：

3a. 把 `fetch_by_symbol` 改名为 `fetch_by_security_code`，并替换列名：

```python
    def fetch_by_security_code(self, security_code: str) -> pd.DataFrame:
        query = f"""
            SELECT
                trade_date,
                close_price,
                pct_change,
                total_market_value,
                float_market_value,
                total_shares,
                float_shares,
                pe_ttm,
                pe_static,
                pb,
                peg,
                pcf,
                ps
            FROM {self.table_name}
            WHERE security_code = %s
            ORDER BY trade_date ASC
        """
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query, [security_code])
            rows = cursor.fetchall() or []
        return pd.DataFrame(rows)
```

3b. `fetch_one_sample` 的 `SELECT symbol,` 改为 `SELECT security_code,`，`ORDER BY trade_date DESC, symbol ASC` 改为 `ORDER BY trade_date DESC, security_code ASC`。

3c. `latest_trade_date` 改列名与参数名：

```python
    def latest_trade_date(self, security_code: str) -> date | None:
        query = f"SELECT MAX(trade_date) AS latest_trade_date FROM {self.table_name} WHERE security_code = %s"
        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query, [security_code])
            row = cursor.fetchone() or {}
        return row.get("latest_trade_date")
```

3d. **删除** `delete_by_symbol` 与 `insert_rows` 两个方法，新增 `upsert_rows`：

```python
    def upsert_rows(self, cursor: Any, security_code: str, rows: list[dict]) -> int:
        query = f"""
            INSERT INTO {self.table_name} (
                security_code,
                trade_date,
                close_price,
                pct_change,
                total_market_value,
                float_market_value,
                total_shares,
                float_shares,
                pe_ttm,
                pe_static,
                pb,
                peg,
                pcf,
                ps,
                created_at,
                updated_at
            ) VALUES (
                %(security_code)s,
                %(trade_date)s,
                %(close_price)s,
                %(pct_change)s,
                %(total_market_value)s,
                %(float_market_value)s,
                %(total_shares)s,
                %(float_shares)s,
                %(pe_ttm)s,
                %(pe_static)s,
                %(pb)s,
                %(peg)s,
                %(pcf)s,
                %(ps)s,
                CURRENT_DATE,
                CURRENT_DATE
            )
            ON DUPLICATE KEY UPDATE
                close_price = VALUES(close_price),
                pct_change = VALUES(pct_change),
                total_market_value = VALUES(total_market_value),
                float_market_value = VALUES(float_market_value),
                total_shares = VALUES(total_shares),
                float_shares = VALUES(float_shares),
                pe_ttm = VALUES(pe_ttm),
                pe_static = VALUES(pe_static),
                pb = VALUES(pb),
                peg = VALUES(peg),
                pcf = VALUES(pcf),
                ps = VALUES(ps),
                updated_at = CURRENT_DATE
        """
        return self._upsert_in_batches(cursor, security_code, query, rows)
```

3e. 把 `_insert_in_batches` 改名为 `_upsert_in_batches`，日志中的 `symbol` 参数名改为 `security_code`，其余逻辑不变。

- [ ] **Step 4: 同步全部调用方**

`fetch_by_symbol` 有**两个**调用方，都必须改，漏掉任一个都会让对应接口 500：

4a. `backend/app/providers/market_performance/database_market_performance_provider.py:36`：

```python
        valuation_df = self.repository.fetch_by_security_code(symbol)
```

该文件的 `rename` 映射把 `trade_date` 映射为 `date`，`fetch_by_security_code` 未返回
`security_code` 列，故 rename 字典无需改动。（同文件 58 行的
`self.profit_repository.fetch_by_symbol` 属利润表仓储，在 Task 5 改。）

4b. `backend/app/providers/valuation/database_valuation_provider.py:25`：

```python
        valuation_df = self.repository.fetch_by_security_code(symbol)
```

改完后确认无残留：

```bash
grep -rn "\.fetch_by_symbol(" backend/app/providers/
```
Expected: 只剩 `database_market_performance_provider.py:58` 一处（Task 5 处理）

- [ ] **Step 5: 修正既有测试**

`tests/test_stock_value_daily_repository.py` 中 `test_fetch_one_sample` 的断言
`self.assertIn("symbol", sample)` 改为：

```python
        self.assertIn("security_code", sample)
```

- [ ] **Step 6: 运行测试确认通过**

Run: `python -m unittest tests.test_stock_value_daily_upsert -v`
Expected: PASS，4 个测试全绿

- [ ] **Step 7: 提交**

```bash
git add backend/app/repositories/stock_value_daily_repository.py \
        backend/app/providers/market_performance/database_market_performance_provider.py \
        tests/test_stock_value_daily_repository.py tests/test_stock_value_daily_upsert.py
git commit -m "feat: stock_value_daily 列名改为 security_code 并改用增量 upsert

删除 delete_by_symbol/insert_rows 的全量替换方式，改为
ON DUPLICATE KEY UPDATE，消除删除与插入之间的空窗期。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 5: StockProfitSheetRepository 水位线与 upsert

**Files:**
- Modify: `backend/app/repositories/stock_profit_sheet_repository.py`
- Test: `tests/test_stock_profit_sheet_repository.py`

**Interfaces:**
- Produces:
  - `.latest_report_date(security_code: str) -> date | None`
  - `.upsert_rows(cursor, security_code: str, rows: list[dict]) -> int`
  - `.fetch_by_security_code(security_code: str) -> pd.DataFrame`（由 `fetch_by_symbol` 改名）
  - `exists_by_symbol`、`delete_by_symbol`、`insert_rows` **删除**

- [ ] **Step 1: 写失败测试**

创建 `tests/test_stock_profit_sheet_repository.py`：

```python
from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.app.repositories.stock_profit_sheet_repository import StockProfitSheetRepository

TEST_CODE = "000003"


def _row(report_date: date, netprofit: float) -> dict:
    return {
        "security_code": TEST_CODE,
        "security_name_abbr": "测试股",
        "report_date": report_date,
        "report_type": "年报",
        "report_date_name": "2025年报",
        "total_operate_income": 1000.0,
        "netprofit": netprofit,
        "basic_eps": 1.0,
        "total_operate_cost": 800.0,
        "opinion_type": "标准无保留意见",
    }


class StockProfitSheetRepositoryTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = StockProfitSheetRepository()
        if not self.repository.is_available:
            self.skipTest("database settings are not configured")
        self._cleanup()

    def tearDown(self) -> None:
        self._cleanup()

    def _cleanup(self) -> None:
        with self.repository.connect() as connection:
            cursor = connection.cursor()
            cursor.execute("DELETE FROM stock_profit_sheet WHERE security_code = %s", [TEST_CODE])
            connection.commit()

    def _upsert(self, rows: list[dict]) -> int:
        with self.repository.connect() as connection:
            cursor = connection.cursor()
            affected = self.repository.upsert_rows(cursor, TEST_CODE, rows)
            connection.commit()
        return affected

    def test_latest_report_date_none_when_empty(self) -> None:
        self.assertIsNone(self.repository.latest_report_date(TEST_CODE))

    def test_latest_report_date(self) -> None:
        self._upsert([_row(date(2025, 12, 31), 100.0), _row(date(2026, 3, 31), 30.0)])
        self.assertEqual(self.repository.latest_report_date(TEST_CODE), date(2026, 3, 31))

    def test_upsert_is_idempotent(self) -> None:
        rows = [_row(date(2026, 3, 31), 30.0)]
        self._upsert(rows)
        self.assertEqual(self._upsert(rows), 0, "重复写入相同数据不应产生变更")

    def test_upsert_updates_changed_value(self) -> None:
        self._upsert([_row(date(2026, 3, 31), 30.0)])
        self._upsert([_row(date(2026, 3, 31), 45.0)])
        df = self.repository.fetch_by_security_code(TEST_CODE)
        self.assertEqual(len(df), 1)
        self.assertAlmostEqual(float(df.iloc[0]["netprofit"]), 45.0, places=2)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_stock_profit_sheet_repository -v`
Expected: FAIL — `AttributeError: 'StockProfitSheetRepository' object has no attribute 'latest_report_date'`

- [ ] **Step 3: 实现**

在 `backend/app/repositories/stock_profit_sheet_repository.py` 中：

3a. 顶部导入补 `from datetime import date`。

3b. `fetch_by_symbol` 改名为 `fetch_by_security_code`，参数名 `symbol` 改为 `security_code`（SQL 无需改，列名本来就是 `security_code`）。

3c. **删除** `exists_by_symbol`、`delete_by_symbol`、`insert_rows`。

3d. 新增两个方法：

```python
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
```

3e. 把 `_insert_in_batches` 改名为 `_upsert_in_batches`，参数名 `symbol` 改为 `security_code`。

- [ ] **Step 4: 同步调用方**

`backend/app/providers/market_performance/database_market_performance_provider.py:58`：

```python
        profit_df = self.profit_repository.fetch_by_security_code(symbol)
```

改完后确认无残留：

```bash
grep -rn "\.fetch_by_symbol(\|\.exists_by_symbol(" backend/app/providers/ backend/app/services/
```
Expected: 无输出（`marked_stock_service.py` 的两处将在 Task 9 整体重写时消失，
若此刻仍有输出属正常，Task 9 后须为空）

- [ ] **Step 5: 运行测试确认通过**

Run: `python -m unittest tests.test_stock_profit_sheet_repository -v`
Expected: PASS，4 个测试全绿

- [ ] **Step 6: 提交**

```bash
git add backend/app/repositories/stock_profit_sheet_repository.py \
        backend/app/providers/market_performance/database_market_performance_provider.py \
        tests/test_stock_profit_sheet_repository.py
git commit -m "feat: stock_profit_sheet 新增 report_date 水位线并改用增量 upsert

移除 exists_by_symbol —— 存在性判断正是数据永不更新的根因。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 6: 上游报告期轻量探测

**Files:**
- Create: `backend/app/providers/profit_sheet/__init__.py`
- Create: `backend/app/providers/profit_sheet/eastmoney_report_period_provider.py`
- Test: `tests/test_eastmoney_report_period_provider.py`

**Interfaces:**
- Produces:
  - `EastmoneyReportPeriodProvider()`
  - `.source_name -> "eastmoney_report_period"`
  - `.latest_report_period(security_code: str) -> date | None` — 入参为 6 位代码，内部转 `SH`/`SZ` 前缀

- [ ] **Step 1: 写失败测试**

创建 `tests/test_eastmoney_report_period_provider.py`：

```python
from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.app.providers.profit_sheet.eastmoney_report_period_provider import (
    EastmoneyReportPeriodProvider,
)


class SymbolConversionTestCase(unittest.TestCase):
    def test_shanghai_prefix(self) -> None:
        self.assertEqual(EastmoneyReportPeriodProvider.to_eastmoney_symbol("600519"), "SH600519")

    def test_shenzhen_prefix(self) -> None:
        self.assertEqual(EastmoneyReportPeriodProvider.to_eastmoney_symbol("300308"), "SZ300308")


class ParseTestCase(unittest.TestCase):
    def test_picks_max_report_date(self) -> None:
        payload = {"data": [
            {"REPORT_DATE": "2025-12-31 00:00:00"},
            {"REPORT_DATE": "2026-03-31 00:00:00"},
            {"REPORT_DATE": "2025-09-30 00:00:00"},
        ]}
        self.assertEqual(
            EastmoneyReportPeriodProvider.parse_latest(payload), date(2026, 3, 31)
        )

    def test_empty_payload_returns_none(self) -> None:
        self.assertIsNone(EastmoneyReportPeriodProvider.parse_latest({"data": []}))

    def test_missing_data_key_returns_none(self) -> None:
        self.assertIsNone(EastmoneyReportPeriodProvider.parse_latest({}))

    def test_unparsable_dates_return_none(self) -> None:
        payload = {"data": [{"REPORT_DATE": "not-a-date"}]}
        self.assertIsNone(EastmoneyReportPeriodProvider.parse_latest(payload))


class LiveFetchTestCase(unittest.TestCase):
    """真实网络调用。上游不可用时跳过而非失败。"""

    def test_fetch_returns_a_date(self) -> None:
        provider = EastmoneyReportPeriodProvider()
        try:
            result = provider.latest_report_period("600519")
        except Exception as exc:
            self.skipTest(f"upstream unavailable: {exc}")
        self.assertIsInstance(result, date)
        self.assertGreater(result, date(2020, 1, 1))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_eastmoney_report_period_provider -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'backend.app.providers.profit_sheet'`

- [ ] **Step 3: 实现**

创建 `backend/app/providers/profit_sheet/__init__.py`（空文件即可，仅需一行）：

```python
"""利润表相关数据源。"""
```

创建 `backend/app/providers/profit_sheet/eastmoney_report_period_provider.py`：

```python
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

import requests


logger = logging.getLogger(__name__)

# akshare 的 stock_profit_sheet_by_report_em 内部先请求该地址取报告期列表，
# 再按报告期分页拉数据（约 20 次请求）。此处只取列表用于判断是否需要拉全量。
REPORT_DATE_URL = "https://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/lrbDateAjaxNew"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
REQUEST_TIMEOUT = 15


class EastmoneyReportPeriodProvider:
    source_name = "eastmoney_report_period"

    @staticmethod
    def to_eastmoney_symbol(security_code: str) -> str:
        prefix = "SH" if security_code.startswith(("5", "6", "9")) else "SZ"
        return f"{prefix}{security_code}"

    @staticmethod
    def parse_latest(payload: dict[str, Any]) -> date | None:
        items = payload.get("data") or []
        parsed: list[date] = []
        for item in items:
            raw = str(item.get("REPORT_DATE") or "").strip()
            if not raw:
                continue
            try:
                parsed.append(datetime.strptime(raw[:10], "%Y-%m-%d").date())
            except ValueError:
                continue
        return max(parsed) if parsed else None

    def latest_report_period(self, security_code: str) -> date | None:
        eastmoney_symbol = self.to_eastmoney_symbol(security_code)
        logger.info("Probing latest report period for %s", eastmoney_symbol)
        response = requests.get(
            REPORT_DATE_URL,
            params={"companyType": "4", "reportDateType": "0", "code": eastmoney_symbol},
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return self.parse_latest(response.json())
```

- [ ] **Step 4: 运行测试确认通过**

Run: `python -m unittest tests.test_eastmoney_report_period_provider -v`
Expected: PASS，7 个测试全绿（上游不通时 `LiveFetchTestCase` 显示 skipped）

- [ ] **Step 5: 提交**

```bash
git add backend/app/providers/profit_sheet/ tests/test_eastmoney_report_period_provider.py
git commit -m "feat: 新增上游报告期轻量探测

利润表是季度数据，按日频重拉是纯浪费。先用 1 次请求取报告期列表
（实测 0.1s），晚于库中水位线才拉全量，请求数由 21 降至 1。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 7: StockBackupService 拆分为两个同步方法

**Files:**
- Modify: `backend/app/services/stock/stock_backup_service.py`

**Interfaces:**
- Consumes: `StockValueDailyRepository.upsert_rows`、`StockProfitSheetRepository.upsert_rows`、`StockInfoRepository.upsert_name_with_cursor`（Task 3/4/5）
- Produces:
  - `StockBackupService.sync_valuation(security_code: str) -> dict[str, Any]`
    返回 `{"security_code", "source", "table", "affected_rows", "records", "latest_available_date"}`
  - `StockBackupService.sync_profit(security_code: str) -> dict[str, Any]`
    返回 `{"security_code", "source", "table", "affected_rows", "records", "latest_report_date", "security_name_abbr"}`
  - `StockBackupService.normalize_symbol(symbol: str) -> str`（保留不变）
  - `backup_stock_value`、`trigger_profit_sheet_sync_async`、`_sync_profit_sheet_if_missing`、`_replace_backup_rows` **删除**

- [ ] **Step 1: 改造 provider 使两侧可独立拉取**

`EastmoneyMarketPerformanceProvider.fetch` 目前一次返回两个 DataFrame，
拆开后 `sync_valuation` 不应被利润表的失败拖累。在
`backend/app/providers/market_performance/eastmoney_market_performance_provider.py`
的类中新增两个方法（保留原 `fetch` 不动，`DatabaseMarketPerformanceProvider` 仍在用它的接口约定）：

```python
    def fetch_valuation(self, symbol: str) -> pd.DataFrame:
        logger.info("Fetching valuation for symbol=%s", symbol)
        valuation_df = ak.stock_value_em(symbol=symbol)
        if not isinstance(valuation_df, pd.DataFrame) or valuation_df.empty:
            raise RuntimeError("stock_value_em returned empty dataframe")
        return valuation_df

    def fetch_profit(self, symbol: str) -> pd.DataFrame:
        logger.info("Fetching profit sheet for symbol=%s", symbol)
        profit_df = ak.stock_profit_sheet_by_report_em(symbol=self._to_eastmoney_symbol(symbol))
        if not isinstance(profit_df, pd.DataFrame) or profit_df.empty:
            raise RuntimeError("stock_profit_sheet_by_report_em returned empty dataframe")
        return profit_df
```

- [ ] **Step 2: 替换 StockBackupService 的同步方法**

在 `backend/app/services/stock/stock_backup_service.py` 中：

2a. 顶部导入补上：

```python
from backend.app.repositories.stock_info_repository import StockInfoRepository
```

2b. `__init__` 增加一行：

```python
        self.stock_info_repository = StockInfoRepository(session_factory=self.session_factory)
```

2c. **删除** `backup_stock_value`、`trigger_profit_sheet_sync_async`、
`_sync_profit_sheet_if_missing`、`_replace_backup_rows` 四个方法，替换为：

```python
    def sync_valuation(self, security_code: str) -> dict[str, Any]:
        normalized = self.normalize_symbol(security_code)
        try:
            valuation_df = self.provider.fetch_valuation(normalized)
            rows = self._normalize_stock_value_daily_rows(valuation_df, normalized)
            with self.session_factory.connect("stock_value_sync") as connection:
                cursor = connection.cursor()
                try:
                    affected = self.stock_value_daily_repository.upsert_rows(cursor, normalized, rows)
                    connection.commit()
                except Exception:
                    connection.rollback()
                    logger.exception("valuation sync rolled back for %s", normalized)
                    raise
        except Exception as exc:
            logger.exception("valuation sync failed for security_code=%s", normalized)
            raise HTTPException(status_code=503, detail=f"valuation sync failed: {exc}") from exc

        return {
            "security_code": normalized,
            "source": self.provider.source_name,
            "table": self.stock_value_daily_repository.table_name,
            "affected_rows": affected,
            "records": len(rows),
            "latest_available_date": self._extract_latest_available_date(valuation_df),
        }

    def sync_profit(self, security_code: str) -> dict[str, Any]:
        normalized = self.normalize_symbol(security_code)
        try:
            profit_df = self.provider.fetch_profit(normalized)
            rows = self._normalize_stock_profit_sheet_rows(profit_df, normalized)
            name = self._extract_name(rows)
            with self.session_factory.connect("stock_profit_sync") as connection:
                cursor = connection.cursor()
                try:
                    affected = self.stock_profit_sheet_repository.upsert_rows(cursor, normalized, rows)
                    if name:
                        self.stock_info_repository.upsert_name_with_cursor(cursor, normalized, name)
                    connection.commit()
                except Exception:
                    connection.rollback()
                    logger.exception("profit sync rolled back for %s", normalized)
                    raise
        except Exception as exc:
            logger.exception("profit sync failed for security_code=%s", normalized)
            raise HTTPException(status_code=503, detail=f"profit sync failed: {exc}") from exc

        return {
            "security_code": normalized,
            "source": self.provider.source_name,
            "table": self.stock_profit_sheet_repository.table_name,
            "affected_rows": affected,
            "records": len(rows),
            "latest_report_date": self._extract_latest_available_date(profit_df),
            "security_name_abbr": name,
        }

    @staticmethod
    def _extract_name(rows: list[dict[str, Any]]) -> str | None:
        for row in reversed(rows):
            candidate = (row.get("security_name_abbr") or "").strip()
            if candidate:
                return candidate
        return None
```

- [ ] **Step 3: 手工验证两个方法**

Run:
```bash
python -X utf8 -c "
import logging; logging.disable(logging.CRITICAL)
from backend.app.services.stock.stock_backup_service import StockBackupService
s = StockBackupService()
print('valuation:', s.sync_valuation('600519'))
print('profit   :', s.sync_profit('600519'))
print('二次调用应幂等:')
print('valuation:', s.sync_valuation('600519')['affected_rows'])
"
```
Expected: 首次 `affected_rows` 为正数；第二次 `sync_valuation` 的 `affected_rows` 为 `0`（数据未变），
且 `profit` 结果中 `security_name_abbr` 为 `'贵州茅台'`

- [ ] **Step 4: 确认名称已落入 stock_info**

Run:
```bash
python -X utf8 -c "
import logging; logging.disable(logging.CRITICAL)
from backend.app.repositories.stock_info_repository import StockInfoRepository
print(StockInfoRepository().fetch_by_code('600519'))
"
```
Expected: 输出中 `'security_name_abbr': '贵州茅台'`

- [ ] **Step 5: 提交**

```bash
git add backend/app/services/stock/stock_backup_service.py \
        backend/app/providers/market_performance/eastmoney_market_performance_provider.py
git commit -m "feat: 备份服务拆为 sync_valuation 与 sync_profit

两张表现在各自门控、可独立触发，故拆为两个独立事务。
名称在利润表事务内一并写入 stock_info。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 8: StockSyncCoordinator

**Files:**
- Create: `backend/app/services/stock/stock_sync_coordinator.py`
- Test: `tests/test_stock_sync_coordinator.py`

**Interfaces:**
- Consumes: `TradeCalendar.is_stale`（Task 2）、`StockValueDailyRepository.latest_trade_date`（Task 4）、`StockProfitSheetRepository.latest_report_date`（Task 5）、`EastmoneyReportPeriodProvider.latest_report_period`（Task 6）、`StockBackupService.sync_valuation` / `.sync_profit`（Task 7）
- Produces:
  - `StockSyncCoordinator(backup_service=None, calendar=None, value_repository=None, profit_repository=None, period_provider=None)`
  - `.maybe_sync(security_code: str) -> None` — 非阻塞，请求线程内不做任何 I/O
  - `.run_sync(security_code: str) -> None` — 同步执行门控与拉取，供测试与后台线程复用
  - `.force_sync_valuation(security_code: str) -> dict[str, Any]` — 跳过门控，冲突时抛 HTTPException 409
  - `.force_sync_profit(security_code: str) -> dict[str, Any]` — 同上

- [ ] **Step 1: 写失败测试**

创建 `tests/test_stock_sync_coordinator.py`：

```python
from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from fastapi import HTTPException

from backend.app.services.stock.stock_sync_coordinator import StockSyncCoordinator


class FakeBackupService:
    def __init__(self) -> None:
        self.valuation_calls: list[str] = []
        self.profit_calls: list[str] = []

    def sync_valuation(self, security_code: str) -> dict:
        self.valuation_calls.append(security_code)
        return {"security_code": security_code, "affected_rows": 1}

    def sync_profit(self, security_code: str) -> dict:
        self.profit_calls.append(security_code)
        return {"security_code": security_code, "affected_rows": 1}


class FakeCalendar:
    def __init__(self, stale: bool) -> None:
        self.stale = stale

    def is_stale(self, watermark, today) -> bool:
        return self.stale


class FakeValueRepo:
    def latest_trade_date(self, security_code: str):
        return date(2026, 3, 20)


class FakeProfitRepo:
    def __init__(self, watermark) -> None:
        self.watermark = watermark

    def latest_report_date(self, security_code: str):
        return self.watermark


class FakePeriodProvider:
    def __init__(self, period, error: Exception | None = None) -> None:
        self.period = period
        self.error = error
        self.calls = 0

    def latest_report_period(self, security_code: str):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.period


def build(stale=True, db_watermark=date(2026, 3, 31), upstream=date(2026, 3, 31), probe_error=None):
    backup = FakeBackupService()
    provider = FakePeriodProvider(upstream, probe_error)
    coordinator = StockSyncCoordinator(
        backup_service=backup,
        calendar=FakeCalendar(stale),
        value_repository=FakeValueRepo(),
        profit_repository=FakeProfitRepo(db_watermark),
        period_provider=provider,
    )
    return coordinator, backup, provider


class GatingTestCase(unittest.TestCase):
    def test_fresh_watermark_skips_everything(self) -> None:
        coordinator, backup, provider = build(stale=False)
        coordinator.run_sync("600519")
        self.assertEqual(backup.valuation_calls, [])
        self.assertEqual(provider.calls, 0, "日线新鲜时不应探测报告期")

    def test_stale_watermark_syncs_valuation(self) -> None:
        coordinator, backup, _ = build(stale=True)
        coordinator.run_sync("600519")
        self.assertEqual(backup.valuation_calls, ["600519"])

    def test_same_report_period_does_not_pull_profit(self) -> None:
        """上游报告期与库中水位线相同时绝不能拉全量——本门控的核心价值。"""
        coordinator, backup, provider = build(
            db_watermark=date(2026, 3, 31), upstream=date(2026, 3, 31)
        )
        coordinator.run_sync("600519")
        self.assertEqual(provider.calls, 1)
        self.assertEqual(backup.profit_calls, [], "报告期无变化不应拉取利润表")

    def test_newer_report_period_pulls_profit(self) -> None:
        coordinator, backup, _ = build(
            db_watermark=date(2026, 3, 31), upstream=date(2026, 6, 30)
        )
        coordinator.run_sync("600519")
        self.assertEqual(backup.profit_calls, ["600519"])

    def test_null_watermark_pulls_profit(self) -> None:
        """新股票利润表水位线为 NULL，必须拉取，否则名称永远写不进去。"""
        coordinator, backup, _ = build(db_watermark=None, upstream=date(2026, 3, 31))
        coordinator.run_sync("600519")
        self.assertEqual(backup.profit_calls, ["600519"])

    def test_probe_failure_skips_profit_but_keeps_valuation(self) -> None:
        coordinator, backup, _ = build(probe_error=RuntimeError("upstream down"))
        coordinator.run_sync("600519")
        self.assertEqual(backup.valuation_calls, ["600519"])
        self.assertEqual(backup.profit_calls, [], "探测失败时不应盲拉")


class InFlightTestCase(unittest.TestCase):
    def test_force_sync_releases_slot(self) -> None:
        coordinator, backup, _ = build()
        coordinator.force_sync_valuation("600519")
        coordinator.force_sync_valuation("600519")
        self.assertEqual(backup.valuation_calls, ["600519", "600519"])

    def test_force_sync_conflicts_with_held_slot(self) -> None:
        coordinator, _, _ = build()
        self.assertTrue(coordinator._acquire("600519"))
        with self.assertRaises(HTTPException) as ctx:
            coordinator.force_sync_valuation("600519")
        self.assertEqual(ctx.exception.status_code, 409)
        coordinator._release("600519")

    def test_force_sync_skips_gating(self) -> None:
        """手工接口即使数据新鲜也必须真实拉取。"""
        coordinator, backup, _ = build(stale=False)
        coordinator.force_sync_profit("600519")
        self.assertEqual(backup.profit_calls, ["600519"])

    def test_slot_released_after_failure(self) -> None:
        coordinator, backup, _ = build()

        def boom(security_code: str) -> dict:
            raise RuntimeError("sync exploded")

        backup.sync_valuation = boom
        with self.assertRaises(RuntimeError):
            coordinator.force_sync_valuation("600519")
        self.assertTrue(coordinator._acquire("600519"), "异常后槽位必须已释放")
        coordinator._release("600519")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_stock_sync_coordinator -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'backend.app.services.stock.stock_sync_coordinator'`

- [ ] **Step 3: 实现**

创建 `backend/app/services/stock/stock_sync_coordinator.py`：

```python
from __future__ import annotations

import logging
import threading
from datetime import date
from typing import Any

from fastapi import HTTPException

from backend.app.providers.profit_sheet.eastmoney_report_period_provider import (
    EastmoneyReportPeriodProvider,
)
from backend.app.repositories.stock_profit_sheet_repository import StockProfitSheetRepository
from backend.app.repositories.stock_value_daily_repository import StockValueDailyRepository
from backend.app.services.stock.stock_backup_service import StockBackupService
from backend.app.services.stock.trade_calendar import TradeCalendar


logger = logging.getLogger(__name__)


class StockSyncCoordinator:
    """新鲜度门控 + in-flight 去重 + 后台线程调度。

    请求线程内不做任何 I/O：先占 in-flight 槽位（纯内存操作），
    再把水位线查询与门控判定全部交给后台线程，避免拖慢接口响应。
    """

    def __init__(
        self,
        backup_service: StockBackupService | None = None,
        calendar: TradeCalendar | None = None,
        value_repository: StockValueDailyRepository | None = None,
        profit_repository: StockProfitSheetRepository | None = None,
        period_provider: EastmoneyReportPeriodProvider | None = None,
    ) -> None:
        self.backup_service = backup_service or StockBackupService()
        self.calendar = calendar or TradeCalendar()
        self.value_repository = value_repository or StockValueDailyRepository()
        self.profit_repository = profit_repository or StockProfitSheetRepository()
        self.period_provider = period_provider or EastmoneyReportPeriodProvider()
        self._in_flight: set[str] = set()
        self._lock = threading.Lock()

    # ---- in-flight 去重 ----

    def _acquire(self, security_code: str) -> bool:
        with self._lock:
            if security_code in self._in_flight:
                return False
            self._in_flight.add(security_code)
            return True

    def _release(self, security_code: str) -> None:
        with self._lock:
            self._in_flight.discard(security_code)

    # ---- 惰性触发 ----

    def maybe_sync(self, security_code: str) -> None:
        if not self._acquire(security_code):
            logger.info("Sync already in flight for %s, skip", security_code)
            return

        worker = threading.Thread(
            target=self._run_and_release,
            args=(security_code,),
            name=f"stock-sync-{security_code}",
            daemon=True,
        )
        try:
            worker.start()
        except Exception:
            self._release(security_code)
            raise

    def _run_and_release(self, security_code: str) -> None:
        try:
            self.run_sync(security_code)
        except Exception:
            logger.exception("Background sync failed for %s", security_code)
        finally:
            self._release(security_code)

    def run_sync(self, security_code: str) -> None:
        """门控 + 拉取。同步执行，不涉及 in-flight 槽位管理。"""
        today = date.today()
        watermark = self.value_repository.latest_trade_date(security_code)
        if not self.calendar.is_stale(watermark, today):
            logger.info("Valuation is fresh for %s (watermark=%s), skip sync", security_code, watermark)
            return

        self.backup_service.sync_valuation(security_code)
        self._sync_profit_if_new_report(security_code)

    def _sync_profit_if_new_report(self, security_code: str) -> None:
        try:
            upstream = self.period_provider.latest_report_period(security_code)
        except Exception as exc:
            # 上游出问题时再猛拉是火上浇油，跳过本次，下个交易日重试
            logger.warning("Report period probe failed for %s, skip profit sync: %s", security_code, exc)
            return

        if upstream is None:
            logger.warning("Report period probe returned nothing for %s, skip profit sync", security_code)
            return

        current = self.profit_repository.latest_report_date(security_code)
        if current is not None and upstream <= current:
            logger.info(
                "Profit sheet is fresh for %s (db=%s upstream=%s), skip pull",
                security_code, current, upstream,
            )
            return

        self.backup_service.sync_profit(security_code)

    # ---- 手工运维：跳过门控，保留并发保护 ----

    def force_sync_valuation(self, security_code: str) -> dict[str, Any]:
        return self._force(security_code, self.backup_service.sync_valuation)

    def force_sync_profit(self, security_code: str) -> dict[str, Any]:
        return self._force(security_code, self.backup_service.sync_profit)

    def _force(self, security_code: str, action) -> dict[str, Any]:
        if not self._acquire(security_code):
            raise HTTPException(
                status_code=409,
                detail=f"sync already in progress for {security_code}, retry later",
            )
        try:
            return action(security_code)
        finally:
            self._release(security_code)
```

- [ ] **Step 4: 运行测试确认通过**

Run: `python -m unittest tests.test_stock_sync_coordinator -v`
Expected: PASS，10 个测试全绿

- [ ] **Step 5: 提交**

```bash
git add backend/app/services/stock/stock_sync_coordinator.py tests/test_stock_sync_coordinator.py
git commit -m "feat: 新增同步协调器，按新鲜度惰性触发

取代 exists_by_symbol 短路。日线走两级门控，利润表走报告期探测，
两者都不满足才真正拉取。in-flight 集合防止重复起线程，
槽位在 finally 中释放，避免异常导致永久占位。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 9: MarkedStockService 改用 stock_info

**Files:**
- Modify: `backend/app/services/stock/marked_stock_service.py`

**Interfaces:**
- Consumes: `StockInfoRepository`（Task 3）、`StockSyncCoordinator.maybe_sync`（Task 8）
- Produces:
  - `MarkedStockService(repository=None, coordinator=None)`
  - `.list_marked_stocks() -> {"count": int, "items": list[dict]}`
  - `.upsert_marked_stock(symbol: str, mark_reason: str, name: str | None = None) -> dict`
  - 返回项键名保持不变：`symbol`、`name`、`mark_reason`、`created_at`、`updated_at`（前端 `marked-stocks.js` 依赖）

- [ ] **Step 1: 整体替换文件内容**

用以下内容覆盖 `backend/app/services/stock/marked_stock_service.py`：

```python
from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Any

from fastapi import HTTPException

from backend.app.repositories.stock_info_repository import StockInfoRepository
from backend.app.services.stock.stock_sync_coordinator import StockSyncCoordinator


logger = logging.getLogger(__name__)


class MarkedStockService:
    def __init__(
        self,
        repository: StockInfoRepository | None = None,
        coordinator: StockSyncCoordinator | None = None,
    ) -> None:
        self.repository = repository or StockInfoRepository()
        self.coordinator = coordinator or StockSyncCoordinator()

    def list_marked_stocks(self) -> dict[str, Any]:
        self._ensure_database_available()
        items = [self._serialize_row(row) for row in self.repository.fetch_marked()]
        return {"count": len(items), "items": items}

    def upsert_marked_stock(self, symbol: str, mark_reason: str, name: str | None = None) -> dict[str, Any]:
        self._ensure_database_available()
        security_code = self.normalize_symbol(symbol)
        normalized_reason = self.normalize_reason(mark_reason)

        self.repository.upsert_mark_reason(security_code, normalized_reason)

        # 名称由备份流程写入。若调用方显式给了名称则直接采用，
        # 否则触发一次同步，名称会随利润表一并落库。
        explicit_name = (name or "").strip()
        if explicit_name:
            self.repository.upsert_name(security_code, explicit_name)
        else:
            self.coordinator.maybe_sync(security_code)

        row = self.repository.fetch_by_code(security_code)
        if row is None:
            raise HTTPException(status_code=500, detail="marked stock was not persisted")
        return self._serialize_row(row)

    def _ensure_database_available(self) -> None:
        if not self.repository.is_available:
            raise HTTPException(status_code=503, detail="database settings are not configured")

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        cleaned = re.sub(r"[^0-9]", "", symbol or "")
        if len(cleaned) != 6:
            raise HTTPException(status_code=400, detail="symbol must be a 6-digit A-share stock code")
        return cleaned

    @staticmethod
    def normalize_reason(reason: str) -> str:
        cleaned = (reason or "").strip()
        if not cleaned:
            raise HTTPException(status_code=400, detail="mark_reason must not be empty")
        return cleaned

    @classmethod
    def _serialize_row(cls, row: dict[str, Any]) -> dict[str, Any]:
        security_code = str(row.get("security_code") or "")
        name = str(row.get("security_name_abbr") or "").strip()
        return {
            "symbol": security_code,
            # 名称缺失时临时回退为代号供前端展示，不写回数据库，
            # 待备份流程补齐后自然恢复。
            "name": name or security_code,
            "mark_reason": str(row.get("mark_reason") or ""),
            "created_at": cls._format_datetime(row.get("created_at")),
            "updated_at": cls._format_datetime(row.get("updated_at")),
        }

    @staticmethod
    def _format_datetime(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, date):
            return value.strftime("%Y-%m-%d")
        text = str(value).strip()
        return text or None
```

- [ ] **Step 2: 手工验证**

Run:
```bash
python -X utf8 -c "
import logging; logging.disable(logging.CRITICAL)
from backend.app.services.stock.marked_stock_service import MarkedStockService
s = MarkedStockService()
print(s.upsert_marked_stock('600036', '价格30左右再入手'))
print(s.list_marked_stocks())
"
```
Expected: 返回的 `name` 为 `'招商银行'`（Task 1 已播种），`mark_reason` 正确写入

- [ ] **Step 3: 提交**

```bash
git add backend/app/services/stock/marked_stock_service.py
git commit -m "refactor: 标记服务改用 stock_info，去掉名称 provider 链

名称不再单独获取，缺失时临时回退为代号展示且不写库，
消除旧实现中兜底代号被固化、永不重试的问题。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 10: StockService 名称查询改造与触发接入

**Files:**
- Modify: `backend/app/services/stock/stock_service.py`

**Interfaces:**
- Consumes: `StockInfoRepository`（Task 3）、`StockSyncCoordinator`（Task 8）
- Produces: `StockService.get_stock_name(symbol) -> {"symbol", "name", "source"}`（键名不变，前端依赖）

- [ ] **Step 1: 调整导入与构造**

在 `backend/app/services/stock/stock_service.py` 中：

1a. **删除**这三行导入：

```python
from backend.app.providers.name.eastmoney_individual_info_name_provider import EastmoneyIndividualInfoNameProvider
from backend.app.providers.name.xueqiu_individual_info_name_provider import XueqiuIndividualInfoNameProvider
```

以及 `from backend.app.providers.interfaces.stock_provider import (...)` 中的 `StockNameProvider` 一项。

1b. **新增**导入：

```python
from backend.app.repositories.stock_info_repository import StockInfoRepository
from backend.app.services.stock.stock_sync_coordinator import StockSyncCoordinator
```

1c. `__init__` 中**删除** `self.name_providers = [...]` 整个赋值块与
`self.stock_backup_service = StockBackupService()`，改为：

```python
        self.stock_info_repository = StockInfoRepository()
        self.sync_coordinator = StockSyncCoordinator()
```

同时删除文件顶部 `from backend.app.services.stock.stock_backup_service import StockBackupService` 导入。

- [ ] **Step 2: 替换 get_stock_name 与 _fetch_name_with_failover**

`get_stock_name` 替换为：

```python
    def get_stock_name(self, symbol: str) -> dict[str, Any]:
        normalized_symbol = self.normalize_symbol(symbol)
        self.sync_coordinator.maybe_sync(normalized_symbol)

        row = self.stock_info_repository.fetch_by_code(normalized_symbol)
        name = str((row or {}).get("security_name_abbr") or "").strip()
        if name:
            return {"symbol": normalized_symbol, "name": name, "source": "stock_info"}

        # 名称尚未落库（首次查询该股票），临时回退为代号，不写库。
        # 上面已触发同步，名称会在备份完成后补齐。
        return {"symbol": normalized_symbol, "name": normalized_symbol, "source": "fallback"}
```

**删除** `_fetch_name_with_failover` 方法整体。

- [ ] **Step 3: 在其余查询入口接入惰性触发**

`get_stock_valuation_metrics` 与 `get_stock_market_performance` 两个方法中，
在 `normalized_symbol = self.normalize_symbol(symbol)` 之后各加一行：

```python
        self.sync_coordinator.maybe_sync(normalized_symbol)
```

- [ ] **Step 4: 验证应用可正常导入且路由完整**

Run:
```bash
python -c "
from backend.app.main import app
print('routes:', len([r for r in app.routes if hasattr(r, 'methods')]))
" 2>&1 | tail -2
```
Expected: 输出 `routes: 14`，无 ImportError

- [ ] **Step 5: 提交**

```bash
git add backend/app/services/stock/stock_service.py
git commit -m "refactor: 名称查询改查 stock_info 并接入惰性同步

由两次外部 HTTP 请求变为一次主键查询。估值与行情查询入口
一并接入 maybe_sync，实现按新鲜度的惰性增量更新。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 11: 手工运维端点

**Files:**
- Modify: `backend/app/controllers/ops_controller.py`

**Interfaces:**
- Consumes: `StockSyncCoordinator.force_sync_valuation` / `.force_sync_profit`（Task 8）
- Produces: `POST /api/ops/sync/valuation`、`POST /api/ops/sync/profit`

- [ ] **Step 1: 整体替换文件内容**

用以下内容覆盖 `backend/app/controllers/ops_controller.py`：

```python
from __future__ import annotations

from fastapi import APIRouter, Query

from backend.app.services.stock.stock_sync_coordinator import StockSyncCoordinator


router = APIRouter(prefix="/api/ops", tags=["ops"])

sync_coordinator = StockSyncCoordinator()


@router.post("/sync/valuation")
def force_sync_valuation(symbol: str = Query(..., description="6-digit A-share stock code")) -> dict:
    """强制同步估值日线，跳过新鲜度门控。冲突时返回 409。"""
    return sync_coordinator.force_sync_valuation(symbol)


@router.post("/sync/profit")
def force_sync_profit(symbol: str = Query(..., description="6-digit A-share stock code")) -> dict:
    """强制同步利润表，跳过报告期探测。冲突时返回 409。"""
    return sync_coordinator.force_sync_profit(symbol)
```

- [ ] **Step 2: 验证路由注册**

Run:
```bash
python -c "
from backend.app.main import app
for r in app.routes:
    if hasattr(r, 'methods') and 'ops' in r.path:
        print(sorted(r.methods), r.path)
" 2>&1 | tail -3
```
Expected:
```
['POST'] /api/ops/sync/valuation
['POST'] /api/ops/sync/profit
```

- [ ] **Step 3: 端到端验证强制同步跳过门控**

启动服务：`python -m uvicorn backend.app.main:app --port 8000`，另开终端：

```bash
curl -X POST "http://127.0.0.1:8000/api/ops/sync/valuation?symbol=600519"
curl -X POST "http://127.0.0.1:8000/api/ops/sync/profit?symbol=600519"
```
Expected: 两次均返回 200 与写入统计 JSON。**即使数据已是最新**（Task 7 刚同步过），
也应真实执行拉取而非跳过——这正是「不校验」的要求。

- [ ] **Step 4: 提交**

```bash
git add backend/app/controllers/ops_controller.py
git commit -m "feat: 新增两个强制同步运维端点

POST /api/ops/sync/valuation 与 /api/ops/sync/profit 跳过全部
新鲜度门控直接拉取，用于数据修复与排障；仍参与 in-flight 去重，
冲突返回 409。移除无调用方的 /api/ops/backup/stock-value。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 12: 清理废弃的名称 provider 链

**Files:**
- Delete: `backend/app/providers/name/` 整个目录
- Delete: `backend/app/repositories/stock_marked_repository.py`
- Modify: `backend/app/providers/interfaces/stock_provider.py`

**Interfaces:**
- Produces: 无新接口；确认 `StockNameProvider` 抽象与两个实现类全部移除

- [ ] **Step 1: 确认无残留引用**

Run:
```bash
grep -rn "StockNameProvider\|providers.name\|stock_marked_repository\|StockMarkedRepository\|exists_by_symbol\|fetch_by_symbol\|delete_by_symbol\|insert_rows\|backup_stock_value\|trigger_profit_sheet_sync_async" backend/ tests/ --include=*.py
```
Expected: 无输出。若有输出，先修掉对应引用再继续。

- [ ] **Step 2: 删除文件**

```bash
rm -rf backend/app/providers/name
rm -f backend/app/repositories/stock_marked_repository.py
```

- [ ] **Step 3: 移除 StockNameProvider 抽象**

从 `backend/app/providers/interfaces/stock_provider.py` 末尾删除整个类：

```python
class StockNameProvider(ABC):
    source_name: str

    @abstractmethod
    def fetch(self, symbol: str) -> str:
        raise NotImplementedError
```

- [ ] **Step 4: 全量验证**

```bash
python -m compileall -q backend tests && echo "COMPILE OK"
python -c "from backend.app.main import app; print('routes:', len([r for r in app.routes if hasattr(r,'methods')]))"
python -m unittest discover -s tests -p "test_*.py" -v
```
Expected:
- `COMPILE OK`
- `routes: 14`
- 全部测试通过或 skipped（无 FAIL / ERROR）

- [ ] **Step 5: 确认 BOM 未被重新引入**

```bash
python -c "
import pathlib
bad = [str(p) for p in pathlib.Path('backend').rglob('*.py')
       if p.read_bytes()[:3] == b'\xef\xbb\xbf']
print('BOM 文件:', bad or '无')
"
```
Expected: `BOM 文件: 无`

- [ ] **Step 6: 提交**

```bash
git add -A backend/ tests/
git commit -m "chore: 移除废弃的名称 provider 链与 stock_marked 仓储

雪球 provider 依赖 akshare 内置的已过期 token，东财 provider 走
已被封禁的 push2 主机族，两者均永久不可用。名称改由备份流程提供后
整条链路不再需要。

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## 自查记录

**规格覆盖**：规格 9 节逐条对应 —— 3.1 建表 → Task 1；3.2 列名改造 → Task 1/4；
4.1 两级门控 → Task 2；4.2 协调器与利润表探测 → Task 6/8；4.3 增量写入 → Task 4/5/7；
4.4 名称读取 → Task 3/9/10；4.5 运维端点 → Task 11；第 5 节改动面 → Task 1-12 全覆盖；
第 6 节错误处理 → Task 2（日历降级）/Task 8（探测失败、槽位释放）/Task 9-10（名称回退）；
第 7 节测试 → Task 2/3/4/5/6/8 的测试步骤。

**已知取舍**：`maybe_sync` 把门控判定移入后台线程（规格 4.2 的流程图把门控画在请求线程内）。
原因是 `is_stale` 可能触发交易日历网络请求，放在请求线程会拖慢接口响应。
效果等价：新鲜时后台线程立即返回，不产生任何网络请求。

**规格补正**：规格第 5 节的文件改动面漏列了
`backend/app/providers/valuation/database_valuation_provider.py`。
该文件调用 `StockValueDailyRepository.fetch_by_symbol`，列名改造后必须同步，
否则估值接口 500。已在 Task 4 Step 4 补上。

---

## 执行方式

Plan complete and saved to `docs/superpowers/plans/2026-08-02-stock-info-and-incremental-sync.md`.
