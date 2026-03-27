CREATE TABLE stock_value_daily (
    symbol VARCHAR(10) NOT NULL COMMENT '股票代码，如 600036 / 002594',
    trade_date DATE NOT NULL COMMENT '数据日期',
    close_price DECIMAL(18,4) NULL COMMENT '当日收盘价，单位：元',
    pct_change DECIMAL(10,4) NULL COMMENT '当日涨跌幅，单位：%',
    total_market_value DECIMAL(20,2) NULL COMMENT '总市值，单位：元',
    float_market_value DECIMAL(20,2) NULL COMMENT '流通市值，单位：元',
    total_shares BIGINT NULL COMMENT '总股本，单位：股',
    float_shares BIGINT NULL COMMENT '流通股本，单位：股',
    pe_ttm DECIMAL(18,6) NULL COMMENT 'PE(TTM)',
    pe_static DECIMAL(18,6) NULL COMMENT 'PE(静)',
    pb DECIMAL(18,6) NULL COMMENT '市净率',
    peg DECIMAL(18,6) NULL COMMENT 'PEG值',
    pcf DECIMAL(18,6) NULL COMMENT '市现率',
    ps DECIMAL(18,6) NULL COMMENT '市销率',
    created_at DATE NULL COMMENT '创建日期',
    updated_at DATE NULL COMMENT '更新日期',
    PRIMARY KEY (symbol, trade_date),
    KEY idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票日度估值指标表';


CREATE TABLE stock_profit_sheet(
    security_code VARCHAR(10) NOT NULL COMMENT '股票代码 600519',
    security_name_abbr VARCHAR(50) COMMENT '股票简称',
    report_date DATE NOT NULL COMMENT '报告期日期',
    report_type VARCHAR(20) NOT NULL COMMENT '报告类型',
    report_date_name VARCHAR(20) COMMENT '报告期名称',
    total_operate_income DECIMAL(20, 2) NULL COMMENT '营业总收入',
    netprofit DECIMAL(20, 2) NULL COMMENT '净利润',
    basic_eps DECIMAL(18, 6) NULL COMMENT '基本每股收益',
    total_operate_cost DECIMAL(20, 2) NULL COMMENT '营业总成本',
    opinion_type VARCHAR(50) NULL COMMENT '审计意见类型',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (security_code, report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='利润表';


CREATE TABLE stock_marked (
    security_code VARCHAR(10) NOT NULL COMMENT '股票代码 600036',
    security_name_abbr VARCHAR(50) NOT NULL COMMENT '股票简称',
    mark_reason VARCHAR(500) NOT NULL COMMENT '标记原因',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (security_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票标记表';
