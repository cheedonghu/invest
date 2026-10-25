from __future__ import annotations

import json
import unittest
from pathlib import Path

import pandas as pd
from fastapi import HTTPException

from backend.app.services.stock.balance_sheet_aggregation_service import BalanceSheetAggregationService
from backend.app.services.stock.stock_service import StockService


class BalanceSheetAggregationTestCase(unittest.TestCase):
    def setUp(self):
        self.service = BalanceSheetAggregationService()

    def aggregate(self, record):
        return self.service.aggregate(pd.DataFrame([{"报告日": "2025-03-31", **record}]), "600036")[0]

    def value(self, period, side, key):
        return next(item for item in period[side] if item["key"] == key)["value"]

    def test_receivables_components_used_when_subtotal_missing(self):
        result = self.aggregate({"应收账款": 8e8, "应收票据": 2e8, "流动资产合计": 10e8})
        self.assertEqual(self.value(result, "assets", "receivables"), 10)

    def test_subtotals_do_not_double_count_details(self):
        result = self.aggregate({"应收票据及应收账款": 10e8, "应收账款": 8e8, "应收票据": 2e8,
                                 "其他应收款合计": 5e8, "其他应收款": 3e8, "应收利息": 2e8})
        self.assertEqual(self.value(result, "assets", "receivables"), 15)
        self.assertEqual(result["assets"][1]["details"][1]["included_in_total"], False)

    def test_zero_subtotal_is_present(self):
        result = self.aggregate({"应收票据及应收账款": 0, "应收账款": 8e8})
        self.assertEqual(self.value(result, "assets", "receivables"), 0)

    def test_fixed_assets_and_construction_fall_back_to_components(self):
        result = self.aggregate({"固定资产": 10e8, "固定资产净值": 10e8, "固定资产原值": 15e8,
                                 "固定资产清理": 1e8, "在建工程": 3e8, "工程物资": 1e8})
        self.assertEqual(self.value(result, "assets", "fixed_assets"), 15)

    def test_long_payables_only_counted_in_noncurrent_liabilities(self):
        result = self.aggregate({"长期应付款": 10e8, "流动负债合计": 0, "非流动负债合计": 10e8})
        self.assertEqual(self.value(result, "liabilities", "payables"), 0)
        self.assertEqual(sum(item["value"] for item in result["liabilities"]), 10)

    def test_current_maturities_merged_into_other_current_liabilities(self):
        result = self.aggregate({"短期借款": 2e8, "一年内到期的非流动负债": 1e8,
                                 "其他流动负债": 0.5e8, "流动负债合计": 3.5e8, "负债合计": 3.5e8})
        self.assertEqual(self.value(result, "liabilities", "short_term_borrowings"), 2)
        self.assertNotIn("current_maturities", {item["key"] for item in result["liabilities"]})
        other = next(item for item in result["liabilities"] if item["key"] == "other_current_liabilities")
        self.assertEqual(other["value"], 1.5)
        self.assertEqual({detail["label"]: detail["value"] for detail in other["details"]},
                         {"一年内到期的非流动负债": 1, "其他流动负债": 0.5})
        self.assertEqual(sum(item["value"] for item in result["liabilities"]), 3.5)

    def test_cambricon_liabilities_match_cninfo_report_for_both_providers(self):
        fixture = json.loads((Path(__file__).parent / "fixtures" /
                              "balance_sheet_688256_20260930.json").read_text(encoding="utf-8"))
        results = []
        for source, record in fixture["records"].items():
            with self.subTest(source=source):
                result = self.service.aggregate(pd.DataFrame([record]), fixture["symbol"])[0]
                self.assertEqual(result["report_date"], fixture["report_date"])
                self.assertEqual(result["liabilities_total"], 72.36)
                self.assertEqual(self.value(result, "liabilities", "payables"), 67.94)
                self.assertEqual(self.value(result, "liabilities", "short_term_borrowings"), 0)
                self.assertEqual(self.value(result, "liabilities", "long_term_borrowings"), 0)
                self.assertNotIn("current_maturities", {item["key"] for item in result["liabilities"]})
                current = next(item for item in result["liabilities"] if item["key"] == "other_current_liabilities")
                self.assertEqual(current["value"], 0.13)
                self.assertEqual(current["details"], [{"label": "一年内到期的非流动负债", "value": 0.13,
                                                       "included_in_total": True}])
                other = next(item for item in result["liabilities"]
                             if item["key"] == "other_non_current_liabilities")
                self.assertEqual(other["value"], 1.86)
                self.assertEqual({detail["label"]: detail["value"] for detail in other["details"]},
                                 {"递延收益": 1.73, "租赁负债": 0.06,
                                  "递延所得税负债": 0, "其他非流动负债": 0.07})
                payables = next(item for item in result["liabilities"] if item["key"] == "payables")
                for detail in payables["details"]:
                    self.assertEqual(detail["included_in_total"],
                                     detail["label"] not in {"应付票据", "应付账款"})
                results.append(result["liabilities"])
        self.assertEqual(results[0], results[1])

    def test_noncurrent_liability_aliases_do_not_double_count(self):
        result = self.aggregate({"递延收益": 3e8, "长期递延收益": 3e8,
                                 "预计非流动负债": 1e8, "LONG_STAFFSALARY_PAYABLE": 2e8,
                                 "OTHER_NONCURRENT_LIAB": 4e8, "非流动负债合计": 10e8})
        other = next(item for item in result["liabilities"] if item["key"] == "other_non_current_liabilities")
        self.assertEqual(other["value"], 10)
        self.assertEqual({detail["label"]: detail["value"] for detail in other["details"]},
                         {"递延收益": 3, "预计负债": 1, "长期应付职工薪酬": 2, "其他非流动负债": 4})

    def test_current_maturities_not_counted_as_long_term_investment(self):
        result = self.aggregate({"一年内到期的非流动资产": 5e8, "流动资产合计": 5e8, "非流动资产合计": 0})
        self.assertEqual(self.value(result, "assets", "long_term_investment"), 0)
        self.assertEqual(result["assets_total"], 5)

    def test_eastmoney_fields_and_overall_totals(self):
        raw = pd.DataFrame([{"REPORT_DATE": "2025-03-31", "MONETARYFUNDS": 10e8,
                             "ACCOUNTS_RECE": 2e8, "TOTAL_CURRENT_ASSETS": 15e8,
                             "TOTAL_NONCURRENT_ASSETS": 5e8, "TOTAL_ASSETS": 20e8,
                             "TOTAL_LIABILITIES": 8e8}])
        result = self.service.aggregate(raw, "600036")[0]
        self.assertEqual(result["assets_total"], 20)
        self.assertEqual(sum(item["value"] for item in result["assets"]), 20)
        self.assertEqual(sum(item["value"] for item in result["liabilities"]), 8)

    def test_financial_company_without_section_totals_reconciles(self):
        result = self.aggregate({"现金及存放中央银行款项": 10e8, "资产总计": 100e8,
                                 "负债合计": 80e8, "吸收存款及同业存放": 50e8})
        self.assertEqual(sum(item["value"] for item in result["assets"]), 100)
        self.assertEqual(sum(item["value"] for item in result["liabilities"]), 80)

    def test_bank_fixed_total_includes_construction_and_net_loans(self):
        result = self.aggregate({"固定资产合计": 12e8, "固定资产净额": 10e8, "在建工程": 2e8,
                                 "发放贷款及垫款净额": 90e8, "发放贷款及垫款": 100e8})
        self.assertEqual(self.value(result, "assets", "fixed_assets"), 12)
        self.assertEqual(self.value(result, "assets", "loans"), 90)

    def test_bank_eastmoney_aliases_match_sina_categories(self):
        sina = self.aggregate({"存放同业款项": 10e8, "交易性金融资产": 20e8,
                               "以摊余成本计量的金融资产": 30e8, "其他债权投资": 35e8,
                               "其他权益工具投资": 5e8, "客户存款(吸收存款)": 80e8,
                               "其中:同业存放款项": 10e8, "交易性金融负债": 2e8})
        eastmoney = self.aggregate({"DEPOSIT_INTERBANK": 10e8, "FVTPL_FINASSET": 20e8,
                                    "AMORTIZE_COST_FINASSET": 30e8, "FVTOCI_FINASSET": 40e8,
                                    "ACCEPT_DEPOSIT": 80e8, "IOFI_DEPOSIT": 10e8, "FVTPL_FINLIAB": 2e8})
        for side in ("assets", "liabilities"):
            self.assertEqual([(x["key"], x["value"]) for x in sina[side]],
                             [(x["key"], x["value"]) for x in eastmoney[side]])

    def test_loan_component_used_only_without_net_subtotal(self):
        result = self.aggregate({"发放贷款及垫款": 5e8})
        self.assertEqual(self.value(result, "assets", "loans"), 5)

    def test_fair_value_subtotals_are_distinct_from_trading_components(self):
        components = {"TRADE_FINASSET": 8e8, "APPOINT_FVTPL_FINASSET": 2e8,
                      "TRADE_FINLIAB": 2e8, "APPOINT_FVTPL_FINLIAB": 1e8}
        for subtotals in [{}, {"FVTPL_FINASSET": 10e8, "FVTPL_FINLIAB": 3e8}]:
            with self.subTest(subtotals=subtotals):
                result = self.aggregate({**components, **subtotals})
                self.assertEqual(self.value(result, "assets", "cash"), 10)
                self.assertEqual(self.value(result, "liabilities", "short_term_borrowings"), 3)
                for side, key, label in [("assets", "cash", "交易性金融资产"),
                                         ("liabilities", "short_term_borrowings", "交易性金融负债")]:
                    item = next(item for item in result[side] if item["key"] == key)
                    detail = next(detail for detail in item["details"] if detail["label"] == label)
                    self.assertEqual(detail["included_in_total"], not bool(subtotals))

    def test_negative_values_preserved(self):
        result = self.aggregate({"应交税费": -1e8, "流动负债合计": 5e8})
        self.assertEqual(self.value(result, "liabilities", "payroll_tax"), -1)
        self.assertEqual(sum(item["value"] for item in result["liabilities"]), 5)

    def test_duplicate_quarters_use_latest_revision(self):
        raw = pd.DataFrame([
            {"报告日": 20250331, "货币资金": 12e8, "更新日期": "2025-05-01"},
            {"报告日": 20250331, "货币资金": 10e8, "更新日期": "2025-04-01"},
            {"报告日": 20250330, "货币资金": 5e8, "更新日期": "2025-04-01"},
        ])
        result = self.service.aggregate(raw, "600036")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["report_date"], "2025-03-31")
        self.assertEqual(self.value(result[0], "assets", "cash"), 12)

    def test_invalid_schema_triggers_provider_failover(self):
        class Provider:
            def __init__(self, name, frame):
                self.source_name, self.frame = name, frame

            def fetch(self, symbol):
                return self.frame

        service = StockService.__new__(StockService)
        service.balance_sheet_aggregation_service = self.service
        service.balance_sheet_providers = [Provider("invalid", pd.DataFrame({"unknown": [1]})),
            Provider("valid", pd.DataFrame({"REPORT_DATE": ["2025-03-31"], "TOTAL_ASSETS": [10e8]}))]
        with self.assertLogs("backend.app.services.stock.stock_service", level="ERROR"):
            result = service.get_stock_balance_sheet("600036")
        self.assertEqual(result["provider_summary"]["used_source"], "valid")
        self.assertEqual(result["periods"][0]["assets_total"], 10)

    def test_missing_report_date_rejected(self):
        with self.assertRaises(HTTPException):
            self.service.aggregate(pd.DataFrame({"货币资金": [10e8]}), "600036")

    def test_alias_conflict_triggers_provider_failover(self):
        class Provider:
            def __init__(self, name, record):
                self.source_name, self.record = name, record

            def fetch(self, symbol):
                return pd.DataFrame([{"报告日": "2026-09-30", **self.record}])

        service = StockService.__new__(StockService)
        service.balance_sheet_aggregation_service = self.service
        service.balance_sheet_providers = [
            Provider("conflicting", {"递延收益": 1e8, "长期递延收益": 2e8}),
            Provider("valid", {"DEFER_INCOME": 1e8, "TOTAL_LIABILITIES": 1e8}),
        ]
        with self.assertLogs("backend.app.services.stock.stock_service", level="ERROR"):
            result = service.get_stock_balance_sheet("688256")
        self.assertEqual(result["provider_summary"]["used_source"], "valid")
        self.assertEqual(result["periods"][0]["liabilities_total"], 1)

    def test_unrecognized_liability_preserved_as_visible_difference(self):
        result = self.aggregate({"短期借款": 2e8, "供应商新增负债字段": 4e8,
                                 "流动负债合计": 6e8, "负债合计": 6e8})
        other = next(item for item in result["liabilities"] if item["key"] == "other_current_liabilities")
        self.assertEqual(other["value"], 4)
        self.assertEqual(other["details"], [{"label": "其他未单列项目", "value": 4,
                                            "included_in_total": True}])
        self.assertEqual(result["liabilities_total"], 6)


if __name__ == "__main__":
    unittest.main()
