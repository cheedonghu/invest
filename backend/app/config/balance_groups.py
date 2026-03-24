from __future__ import annotations


def contains(pattern: str, include_in_total: bool = True, detail_label: str | None = None) -> dict:
    return {
        "pattern": pattern,
        "match": "contains",
        "include_in_total": include_in_total,
        "detail_label": detail_label,
    }


def exact(pattern: str, include_in_total: bool = True, detail_label: str | None = None) -> dict:
    return {
        "pattern": pattern,
        "match": "exact",
        "include_in_total": include_in_total,
        "detail_label": detail_label,
    }


BALANCE_GROUPS = {
    "assets": [
        {
            "key": "cash",
            "label": "现金",
            "section": "current",
            "matchers": [
                exact("货币资金"),
                exact("现金及存放中央银行款项"),
                exact("现金及现金等价物"),
                exact("交易性金融资产"),
                exact("拆出资金"),
                exact("结算备付金"),
            ],
        },
        {
            "key": "receivables",
            "label": "应收款",
            "section": "current",
            "matchers": [
                exact("应收票据及应收账款"),
                exact("应收账款", include_in_total=False),
                exact("应收票据", include_in_total=False),
                exact("应收款项融资", include_in_total=True),
                exact("其他应收款"),
                exact("应收利息"),
                exact("应收股利"),
            ],
        },
        {
            "key": "prepayments",
            "label": "预付款",
            "section": "current",
            "matchers": [exact("预付款项"), exact("预付款")],
        },
        {
            "key": "inventory",
            "label": "存货",
            "section": "current",
            "matchers": [exact("存货")],
        },
        {
            "key": "other_current_assets",
            "label": "其他流动",
            "section": "current",
            "matchers": [
                exact("衍生金融资产"),
                exact("发放贷款和垫款"),
                exact("买入返售金融资产"),
                exact("其他流动资产"),
            ],
            "residual_of": "流动资产合计",
        },
        {
            "key": "long_term_investment",
            "label": "长期投资",
            "section": "non_current",
            "matchers": [
                exact("债权投资"),
                exact("其他债权投资"),
                exact("长期股权投资"),
                exact("一年内到期的非流动资产"),
                exact("其他权益工具投资"),
                exact("其他非流动金融资产"),
                exact("持有至到期投资"),
                exact("可供出售金融资产"),
                exact("长期应收款"),
            ],
        },
        {
            "key": "fixed_assets",
            "label": "固定资产",
            "section": "non_current",
            "matchers": [
                exact("固定资产及清理合计"),
                exact("固定资产原值", include_in_total=False),
                exact("固定资产净值", include_in_total=False),
                exact("固定资产减值准备", include_in_total=False),
                exact("固定资产及清理", include_in_total=False),
                exact("在建工程合计"),
                exact("在建工程", include_in_total=False),
                exact("投资性房地产"),
                exact("油气资产"),
                exact("生产性生物资产"),
            ],
        },
        {
            "key": "intangible_goodwill",
            "label": "无形&商誉",
            "section": "non_current",
            "matchers": [exact("无形资产"), 
                         exact("商誉"), 
                         exact("使用权资产"),
                         exact("生物性资产")],
        },
        {
            "key": "other_non_current_assets",
            "label": "其他非流动",
            "section": "non_current",
            "matchers": [
                exact("递延所得税资产"), 
                exact("其他非流动资产"),
                exact("长期待分摊费用"), ],
            "residual_of": "非流动资产合计",
        },
    ],
    "liabilities": [
        {
            "key": "short_term_borrowings",
            "label": "短期借款",
            "section": "current",
            "matchers": [
                exact("短期借款"), 
                exact("向中央银行借款"), 
                exact("拆入资金"), 
                exact("交易性金融负债"), 
                exact("衍生金融负债"),
                exact("一年内到期的非流动负债"),
                ],
        },
        {
            "key": "payables",
            "label": "应付款",
            "section": "current",
            "matchers": [
                exact("应付票据及应付账款"),
                exact("应付账款", include_in_total=False),
                exact("应付票据", include_in_total=False),
                exact("其他应付款"),
                exact("长期应付款"),
                exact("应付利息"),
                exact("应付股利"),
            ],
        },
        {
            "key": "advance_receipts",
            "label": "预收款",
            "section": "current",
            "matchers": [exact("预收款项"), 
                         exact("预收账款", include_in_total=False), 
                         exact("合同负债"), 
                         exact("卖出回购金融资产款"), 
                         exact("吸收存款及同业存放"), 
                         exact("代理买卖证券款")],
        },
        {
            "key": "payroll_tax",
            "label": "薪酬&税",
            "section": "current",
            "matchers": [exact("应付职工薪酬"), exact("应交税费")],
        },
        {
            "key": "other_current_liabilities",
            "label": "其他流动负债",
            "section": "current",
            "matchers": [
                exact("应付手续费及佣金"),
                exact("应付分保账款"),
                exact("持有待售负债"),
                exact("其他流动负债"),
                exact("合同履约成本"), 
            ],
            "residual_of": "流动负债合计",
        },
        {
            "key": "long_term_borrowings",
            "label": "长期借款",
            "section": "non_current",
            "matchers": [exact("长期借款"), exact("应付债券")],
        },
        {
            "key": "other_non_current_liabilities",
            "label": "其他非流动",
            "section": "non_current",
            "matchers": [
                exact("长期应付款"), 
                exact("预计负债"), 
                exact("递延收益"), 
                exact("递延所得税负债"),
                exact("租赁负债")],
            "residual_of": "非流动负债合计",
        },
    ],
}

EXCLUDED_BALANCE_FIELDS = {"报告日", "公告日期", "更新日期", "数据源", "是否审计", "币种", "类型"}
