-- 2026-08-02 股票元数据合并与增量更新
--
-- 影响：
--   * 新建 stock_info，承载 security_name_abbr 与 mark_reason
--   * stock_value_daily 重建，列名 symbol -> security_code，历史行情数据丢弃（已确认）
--   * stock_marked 删除，其 mark_reason 先并入 stock_info（保留）
--
-- 执行前建议：mysqldump 备份 stock_marked 与 stock_value_daily

-- ---------------------------------------------------------------
-- 1. 新建 stock_info
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stock_info (
  security_code       VARCHAR(10)  NOT NULL,
  security_name_abbr  VARCHAR(50)  NULL,
  mark_reason         MEDIUMTEXT NULL COMMENT '股票备注（Markdown 原文）',
  created_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (security_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------
-- 2. 用现有利润表数据播种名称
-- ---------------------------------------------------------------
INSERT INTO stock_info (security_code, security_name_abbr)
SELECT DISTINCT security_code, security_name_abbr
FROM stock_profit_sheet
WHERE security_name_abbr IS NOT NULL AND security_name_abbr <> ''
ON DUPLICATE KEY UPDATE security_name_abbr = VALUES(security_name_abbr);

-- ---------------------------------------------------------------
-- 3. 保留原有标记原因（必须在 DROP TABLE stock_marked 之前执行）
-- ---------------------------------------------------------------
INSERT INTO stock_info (security_code, security_name_abbr, mark_reason)
SELECT security_code, security_name_abbr, mark_reason
FROM stock_marked
ON DUPLICATE KEY UPDATE
  mark_reason = VALUES(mark_reason),
  security_name_abbr = COALESCE(stock_info.security_name_abbr, VALUES(security_name_abbr));

-- ---------------------------------------------------------------
-- 4. 重建 stock_value_daily：列名改为 security_code，数据丢弃
-- ---------------------------------------------------------------
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

-- ---------------------------------------------------------------
-- 5. 删除旧表
-- ---------------------------------------------------------------
DROP TABLE IF EXISTS stock_marked;
