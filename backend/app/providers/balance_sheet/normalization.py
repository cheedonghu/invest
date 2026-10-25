from __future__ import annotations

import unicodedata
from decimal import Decimal, InvalidOperation

import pandas as pd


# Providers expose different field names; amounts from both feeds are in yuan.
BALANCE_FIELD_ALIASES = {
    "REPORT_DATE": "报告日",
    "UPDATE_DATE": "更新日期",
    "TOTAL_ASSETS": "资产总计",
    "TOTAL_LIABILITIES": "负债合计",
    "TOTAL_CURRENT_ASSETS": "流动资产合计",
    "TOTAL_NONCURRENT_ASSETS": "非流动资产合计",
    "TOTAL_CURRENT_LIAB": "流动负债合计",
    "TOTAL_NONCURRENT_LIAB": "非流动负债合计",
    "MONETARYFUNDS": "货币资金",
    "CASH_DEPOSIT_PBC": "现金及存放中央银行款项",
    "TRADE_FINASSET": "交易性金融资产",
    "FVTPL_FINASSET": "以公允价值计量且其变动计入当期损益的金融资产",
    "APPOINT_FVTPL_FINASSET": "指定为以公允价值计量且其变动计入当期损益的金融资产",
    "DEPOSIT_INTERBANK": "存放同业款项",
    "LEND_FUND": "拆出资金",
    "SETTLE_EXCESS_RESERVE": "结算备付金",
    "NOTE_ACCOUNTS_RECE": "应收票据及应收账款",
    "ACCOUNTS_RECE": "应收账款",
    "NOTE_RECE": "应收票据",
    "FINANCE_RECE": "应收款项融资",
    "TOTAL_OTHER_RECE": "其他应收款合计",
    "OTHER_RECE": "其他应收款",
    "INTEREST_RECE": "应收利息",
    "DIVIDEND_RECE": "应收股利",
    "PREPAYMENT": "预付款项",
    "INVENTORY": "存货",
    "DERIVE_FINASSET": "衍生金融资产",
    "LOAN_ADVANCE": "发放贷款和垫款净额",
    "BUY_RESALE_FINASSET": "买入返售金融资产",
    "NONCURRENT_ASSET_1YEAR": "一年内到期的非流动资产",
    "OTHER_CURRENT_ASSET": "其他流动资产",
    "CREDITOR_INVEST": "债权投资",
    "OTHER_CREDITOR_INVEST": "其他债权投资",
    "LONG_EQUITY_INVEST": "长期股权投资",
    "OTHER_EQUITY_INVEST": "其他权益工具投资",
    "AMORTIZE_COST_FINASSET": "以摊余成本计量的金融资产",
    "FVTOCI_FINASSET": "其他综合收益金融资产合计",
    "OTHER_NONCURRENT_FINASSET": "其他非流动金融资产",
    "HOLD_MATURITY_INVEST": "持有至到期投资",
    "AVAILABLE_SALE_FINASSET": "可供出售金融资产",
    "LONG_RECE": "长期应收款",
    "TOTAL_FIXED_ASSET": "固定资产及清理合计",
    "FIXED_ASSET": "固定资产",
    "FIXED_ASSET_DISPOSAL": "固定资产清理",
    "TOTAL_CIP": "在建工程合计",
    "CIP": "在建工程",
    "PROJECT_MATERIAL": "工程物资",
    "INVEST_REALESTATE": "投资性房地产",
    "OIL_GAS_ASSET": "油气资产",
    "PRODUCTIVE_BIOLOGY_ASSET": "生产性生物资产",
    "INTANGIBLE_ASSET": "无形资产",
    "GOODWILL": "商誉",
    "USERIGHT_ASSET": "使用权资产",
    "DEFER_TAX_ASSET": "递延所得税资产",
    "OTHER_NONCURRENT_ASSET": "其他非流动资产",
    "LONG_PREPAID_EXPENSE": "长期待摊费用",
    "SHORT_LOAN": "短期借款",
    "LOAN_PBC": "向中央银行借款",
    "BORROW_FUND": "拆入资金",
    "TRADE_FINLIAB": "交易性金融负债",
    "FVTPL_FINLIAB": "以公允价值计量且其变动计入当期损益的金融负债",
    "APPOINT_FVTPL_FINLIAB": "指定为以公允价值计量且其变动计入当期损益的金融负债",
    "DERIVE_FINLIAB": "衍生金融负债",
    "NONCURRENT_LIAB_1YEAR": "一年内到期的非流动负债",
    "NOTE_ACCOUNTS_PAYABLE": "应付票据及应付账款",
    "ACCOUNTS_PAYABLE": "应付账款",
    "NOTE_PAYABLE": "应付票据",
    "TOTAL_OTHER_PAYABLE": "其他应付款合计",
    "OTHER_PAYABLE": "其他应付款",
    "INTEREST_PAYABLE": "应付利息",
    "DIVIDEND_PAYABLE": "应付股利",
    "ADVANCE_RECEIVABLES": "预收款项",
    "CONTRACT_LIAB": "合同负债",
    "SELL_REPO_FINASSET": "卖出回购金融资产款",
    "ACCEPT_DEPOSIT_INTERBANK": "吸收存款及同业存放",
    "ACCEPT_DEPOSIT": "吸收存款",
    "IOFI_DEPOSIT": "其中:同业存放款项",
    "AGENT_TRADE_SECURITY": "代理买卖证券款",
    "STAFF_SALARY_PAYABLE": "应付职工薪酬",
    "TAX_PAYABLE": "应交税费",
    "FEE_COMMISSION_PAYABLE": "应付手续费及佣金",
    "REINSURE_PAYABLE": "应付分保账款",
    "HOLD_SALE_LIAB": "持有待售负债",
    "OTHER_CURRENT_LIAB": "其他流动负债",
    "LONG_LOAN": "长期借款",
    "BOND_PAYABLE": "应付债券",
    "LONG_PAYABLE": "长期应付款",
    "TOTAL_LONG_PAYABLE": "长期应付款合计",
    "LONG_STAFFSALARY_PAYABLE": "长期应付职工薪酬",
    "PREDICT_LIAB": "预计负债",
    "DEFER_INCOME": "递延收益",
    "DEFER_TAX_LIAB": "递延所得税负债",
    "LEASE_LIAB": "租赁负债",
    "OTHER_NONCURRENT_LIAB": "其他非流动负债",
    "其他应收款(合计)": "其他应收款合计",
    "其他应收款（合计）": "其他应收款合计",
    "发放贷款及垫款净额": "发放贷款和垫款净额",
    "发放贷款和垫款": "发放贷款和垫款净额",
    "客户存款(吸收存款)": "吸收存款",
    "衍生金融工具资产": "衍生金融资产",
    "衍生金融工具负债": "衍生金融负债",
    "递延税款借项": "递延所得税资产",
    "长期待分摊费用": "长期待摊费用",
    "长期递延收益": "递延收益",
    "预计非流动负债": "预计负债",
    "资产合计": "资产总计",
    "负债总计": "负债合计",
}


def _field_key(label: object) -> str:
    # Formatting differences do not change accounting meaning; semantic aliases
    # still require an explicit dictionary entry, never substring matching.
    return "".join(unicodedata.normalize("NFKC", str(label)).split()).upper()


_ALIASES_BY_KEY = {_field_key(alias): field for alias, field in BALANCE_FIELD_ALIASES.items()}
_MISSING_VALUES = {"", "-", "--", "—", "N/A", "NA", "NULL", "NONE", "NAN"}


def _amounts_conflict(first: pd.Series, second: pd.Series) -> bool:
    for left, right in zip(first, second):
        try:
            left_amount = Decimal(str(left).strip().replace(",", ""))
            right_amount = Decimal(str(right).strip().replace(",", ""))
        except InvalidOperation:
            continue
        if (left_amount.is_finite() and right_amount.is_finite()
                and abs(left_amount - right_amount) > Decimal("0.01")):
            return True
    return False


def normalize_balance_sheet(raw_df: pd.DataFrame) -> pd.DataFrame:
    columns: dict[str, pd.Series] = {}
    sources: dict[str, list[str]] = {}
    for index, column in enumerate(raw_df.columns):
        key = _field_key(column)
        normalized = _ALIASES_BY_KEY.get(key, key)
        values = raw_df.iloc[:, index]
        missing = values.astype("string").str.strip().str.upper().isin(_MISSING_VALUES)
        values = values.mask(missing)
        sources.setdefault(normalized, []).append(str(column))
        if normalized in columns:
            # Equal aliases represent one amount, not two amounts to sum. A
            # disagreement must surface so the service can try another provider.
            if _amounts_conflict(columns[normalized], values):
                raise ValueError(f"conflicting balance sheet aliases for {normalized}: {sources[normalized]}")
            if columns[normalized].isna().all():
                columns[normalized] = values
            elif (columns[normalized].isna() & values.notna()).any():
                columns[normalized] = columns[normalized].combine_first(values)
        else:
            columns[normalized] = values
    result = pd.DataFrame(columns, index=raw_df.index)
    result.attrs["balance_field_sources"] = sources
    return result
