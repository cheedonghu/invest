# 股票名称获取修复 + 数据增量更新 设计规格

日期：2026-08-02
状态：已确认，待编写实现计划

## 1. 背景与问题

### 1.1 问题一：股票名称经常拿不到，只显示兜底代号

现有两个名称 provider **全部不可用**，实测 5 只股票 × 2 个 provider = 10/10 失败，100% 稳定复现。

**根因 A — 雪球 provider（确定性失败，不会自愈）**

`ak.stock_individual_basic_info_xq` 使用 akshare 1.18.47 **硬编码**在 `akshare.stock.cons` 中的
`xq_a_token='826f0dd021d646522fa7b6413f5be324c52d33e4'`。该 token 已过期，雪球返回：

```
HTTP 400  {"error_code":"400016","error_description":"遇到错误，请刷新页面或者重新登录帐号后再试"}
```

响应体不含 `data` 键，akshare 内部 `pd.DataFrame(data_json["data"])` 抛 `KeyError: 'data'`。
token 随 akshare 发版写死，过期后对所有股票永久失败。

**根因 B — 东财 provider（`push2` 主机族被封）**

`ak.stock_individual_info_em` 请求 `push2.eastmoney.com`，该主机族对本机 IP 直接关闭连接
（`RemoteDisconnected`）。分层探测结果：

| 目标 | 结果 |
|---|---|
| baidu.com（控制组） | 200 — 本机网络正常 |
| `push2.eastmoney.com` | RemoteDisconnected |
| `push2his.eastmoney.com` | RemoteDisconnected |
| `datacenter-web.eastmoney.com` | 200 — 同为东财，正常 |
| `emweb.securities.eastmoney.com` | 200 — 同为东财，正常 |
| `hq.sinajs.cn` | 200 |

已隔离验证：http/https、带与不带 User-Agent、带与不带 `ut` 参数共 8 种组合全部失败；
退避重试至 16s 不恢复。故非网络问题、非东财整站封禁，是 `push2*` 集群专有的反爬。
项目的估值与财报功能不受影响，因其走 `datacenter-web` 与 `emweb`。

**根因 C — 坏值会被固化（代码层面，属潜在缺陷）**

`marked_stock_service.py` 的 `_resolve_stock_name` 在两个 provider 均失败时 `return symbol`，
且写库前不校验。一旦兜底值入库，后续调用命中：

```python
existing = self.repository.fetch_by_symbol(symbol)
if existing and str(existing.get("name") or "").strip():
    return str(existing["name"]).strip()   # "600519" 非空 → 永远返回代号，不再重试
```

坏值永不自愈。

### 1.2 问题二：已获取过的股票不会更新到最新日期

根因在 `stock_backup_service.py` 的 `_sync_profit_sheet_if_missing`：

```python
if self.stock_profit_sheet_repository.exists_by_symbol(symbol):
    return          # 判断「存不存在」而非「新不新鲜」→ 永远不再更新
```

生产数据印证：`stock_value_daily` 共 43130 行、22 只股票，其中 16 只停留在
2026-03-20 ~ 03-27（首次拉取当天），落后约 4 个月；上游 `ak.stock_value_em` 当日已有
到 2026-07-31 的数据。

此外 `backup_stock_value` 采用 `delete_by_symbol` + `insert_rows` 全量替换，
不具备增量能力，且存在「已删除但尚未插入」的空窗期。

### 1.3 关键观察

`stock_profit_sheet.security_name_abbr` 实测 **0 行为空**，名称完全正确。
即：拉取利润表时名称已随数据到手并落库。因此**名称不需要独立数据源**，
它是数据备份流程的副产品。`ak.stock_value_em`（估值日线）不含名称列，
名称唯一来源是利润表接口 `ak.stock_profit_sheet_by_report_em`（走 `emweb`，实测可用）。

## 2. 设计决策

| 议题 | 决策 |
|---|---|
| 增量更新触发时机 | 查询时按新鲜度惰性触发，异步后台线程，不阻塞响应 |
| 新鲜度判定 | 两级门控：一级工作日近似（纯本地），二级真实交易日历确认；一次实现 |
| 名称数据源 | 删除雪球与东财两个 name provider，名称改由备份流程写入 |
| 元数据存储 | 新建 `stock_info` 表，合并 `stock_marked`；`mark_reason` 非空即为已标记 |
| 字段命名 | 与 akshare / 东财返回字段一致（`security_code`、`security_name_abbr`） |
| 并发控制 | 进程内 in-flight 集合去重 |
| 历史数据 | `stock_marked` 与 `stock_value_daily` 数据全部丢弃，重建表 |

## 3. 数据模型

### 3.1 新建 stock_info

```sql
CREATE TABLE stock_info (
  security_code       VARCHAR(10)  NOT NULL,
  security_name_abbr  VARCHAR(50)  NULL,      -- 备份流程写入；未拉过数据时为 NULL
  mark_reason         VARCHAR(500) NULL,      -- 非空 = 已标记
  created_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (security_code)
);
DROP TABLE stock_marked;
```

相比旧 `stock_marked`，两列必须放宽为 NULL：

- `security_name_abbr`：标记了但尚未拉过数据时为空，等备份回填
- `mark_reason`：拉过数据但未标记的股票（当前 22 只中有 20 只属此类）

建表后用现有数据播种名称：

```sql
INSERT INTO stock_info (security_code, security_name_abbr, created_at, updated_at)
SELECT DISTINCT security_code, security_name_abbr, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM stock_profit_sheet
WHERE security_name_abbr IS NOT NULL AND security_name_abbr <> '';
```

### 3.2 重建 stock_value_daily

列名 `symbol` 改为 `security_code`，与另两张表统一。数据全部丢弃，
惰性触发会在首次查询时补回全量历史。其余列不变。

主键保持 `(security_code, trade_date)`。

### 3.3 stock_profit_sheet

结构与数据均不变，主键 `(security_code, report_date)`。

## 4. 组件设计

### 4.1 TradeCalendar（新增 `services/stock/trade_calendar.py`）

```python
latest_workday(today: date)   -> date   # 一级：纯本地，回退到最近的周一~周五
latest_trade_day(today: date) -> date   # 二级：ak.tool_trade_date_hist_sina()，进程内缓存
```

日历进程内缓存，按天失效。实测该接口 0.4s、覆盖至 2026-12-31。

**新鲜度判定：**

```python
def is_stale(watermark: date | None, today: date) -> bool:
    if watermark is None:                    return True    # 从没拉过
    if watermark >= latest_workday(today):   return False   # 一级放行，零网络
    return watermark < latest_trade_day(today)              # 二级确认，避开节假日
```

**一级门控的正确性**：交易日集合 ⊆ 工作日集合，故恒有 `latest_trade_day ≤ latest_workday`。
当 `watermark ≥ latest_workday ≥ latest_trade_day` 时数据必然最新，一级不会漏判陈旧。
一级只可能误报陈旧（节假日期间），该误报由二级用真实日历排除。

### 4.2 StockSyncCoordinator（新增 `services/stock/stock_sync_coordinator.py`）

```
GET /api/stocks/valuation-metrics?symbol=600519
  ├─ StockService 照常查库并立即返回          ← 不阻塞响应
  └─ StockSyncCoordinator.maybe_sync("600519")
       ├ in-flight 集合已含该 security_code → return
       ├ is_stale(水位线) == False          → return（零网络开销）
       └ 否则 → 起后台线程 → StockBackupService.sync() → finally 移出 in-flight
```

水位线取 `StockValueDailyRepository.latest_trade_date(security_code)`
（该方法已存在于代码库但从未被调用）。

**只用日线水位线做判定，一次同步刷新两张表。** 利润表是季度数据，
按日线水位线触发意味着每个交易日都会重拉一次利润表
（`ak.stock_profit_sheet_by_report_em` 实测 3.9s、内部 21 次分页请求）。
接受这一开销，理由是：同步在后台异步执行、有 in-flight 去重、
且 upsert 幂等不会脏数据；换来的是判定逻辑只有一条水位线，不必维护两套。
若后续该开销成为问题，可为利润表单独引入 `report_date` 水位线按季度判定。

in-flight 集合用锁保护；**清理必须在 `finally` 中**，否则一次异常会使该股票
永久占位、再也不会同步。

取代现有 `trigger_profit_sheet_sync_async` 中的 `exists_by_symbol` 短路逻辑。

### 4.3 增量写入

两张表的复合主键即天然 upsert 键，改用 `INSERT ... ON DUPLICATE KEY UPDATE`，
去掉 `delete_by_symbol` 调用。除实现增量外，另消除了全量替换的空窗期。

备份事务内额外 upsert `stock_info` 的 `security_name_abbr`（取自利润表的
`SECURITY_NAME_ABBR`），与数据写入同一事务。

### 4.4 名称读取路径

```
旧：查询 → XueqiuProvider(已死) → EastmoneyProvider(已死) → 兜底代号 → 固化进库
新：备份流程拉利润表 → security_name_abbr → 同事务 upsert 进 stock_info
    查询 → stock_info 主键查一次 → 完
```

`/api/stocks/name` 接口保留（前端 `app.js:447`、`balance-sheet.js:98` 在用，
带 2.5s 超时且失败静默降级），实现改为查 `stock_info` 主键，
由两次外部 HTTP 变为一次主键查询。

## 5. 文件改动面

| 动作 | 文件 |
|---|---|
| 新增 | `services/stock/trade_calendar.py` |
| 新增 | `services/stock/stock_sync_coordinator.py` |
| 新增 | `repositories/stock_info_repository.py` |
| 改 | `services/stock/stock_backup_service.py` — upsert 化、事务内写 stock_info 名称、去掉 exists_by_symbol 短路 |
| 改 | `services/stock/marked_stock_service.py` — 去掉 name provider 链，读写 stock_info |
| 改 | `services/stock/stock_service.py` — `get_stock_name` 改查 stock_info，移除 `name_providers` |
| 改 | `repositories/stock_value_daily_repository.py` — 列名 `symbol`→`security_code`，新增 `upsert_rows` |
| 改 | `repositories/stock_profit_sheet_repository.py` — 新增 `upsert_rows` |
| 改 | `providers/market_performance/database_market_performance_provider.py` — rename 映射同步列名 |
| 改 | `tests/test_stock_value_daily_repository.py` — 跟随列名调整 |
| 删 | `providers/name/`（整目录，含 2 个 provider 与 `__init__.py`） |
| 删 | `repositories/stock_marked_repository.py` |
| 删 | `providers/interfaces/stock_provider.py` 中的 `StockNameProvider` 抽象 |

## 6. 错误处理

| 场景 | 处理 |
|---|---|
| 交易日历拉取失败 | 降级为只用一级工作日判定。宁可多拉一次，不因日历故障卡住更新 |
| 备份线程抛异常 | 记日志，不影响主查询（本就异步）。in-flight 在 `finally` 中清理 |
| 名称缺失（已标记但未拉过数据） | `security_name_abbr` 为 NULL，API 返回时临时回退为代号，**不写库**；备份完成后自动回填，源恢复即自愈 |
| 数据库未配置 | 与现状一致：503 / 跳过同步 |
| 去重漏网（多 worker 部署） | upsert 幂等兜底，最坏多一次网络请求，不会脏数据 |

## 7. 测试策略

沿用项目现有 unittest。

**纯单元测试（无网络、无 DB — 主要保障）**

- `latest_workday`：周一至周日 7 个输入各断言一次
- `is_stale`：水位线为 None / 恰等于最近工作日 / 落后数月 / 处于节假日窗口
- **一级放行时不查日历**：mock 断言日历函数未被调用。这是两级门控的核心价值，必须锁死

**集成测试（需 DB）**

- `upsert_rows` 幂等：同一批数据写入两次，第二次新增 0 行
- `stock_info` 流转：标记后 `security_name_abbr` 为 NULL → 备份后被回填
- `tests/test_stock_value_daily_repository.py` 跟随列名调整

## 8. 明确不做的事

- 不引入定时调度器（APScheduler/cron）
- 不引入全市场名称表缓存（`ak.stock_info_a_code_name`）
- 不为雪球 token 增加配置项（该源整体废弃）
- 不改动 `stock_profit_sheet` 表结构与数据
- 不迁移 `stock_marked` 与 `stock_value_daily` 的旧数据，直接丢弃
  （`stock_info` 的名称列例外：由 `stock_profit_sheet` 现有数据播种，见 3.1）
- 不为利润表单独维护 `report_date` 水位线（见 4.2）
