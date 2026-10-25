from __future__ import annotations

import unittest

import pandas as pd

from backend.app.providers.balance_sheet.normalization import normalize_balance_sheet


class BalanceSheetNormalizationTestCase(unittest.TestCase):
    def test_formatting_variants_map_to_canonical_fields(self):
        result = normalize_balance_sheet(pd.DataFrame([{
            " 其他应收款 （合计） ": 2e8, "ｄｅｆｅｒ＿ｉｎｃｏｍｅ": 3e8,
            " 长期\u3000递延收益 ": 3e8,
        }]))
        self.assertEqual(result.iloc[0].to_dict(), {"其他应收款合计": 2e8, "递延收益": 3e8})
        self.assertEqual(result.attrs["balance_field_sources"]["递延收益"],
                         ["ｄｅｆｅｒ＿ｉｎｃｏｍｅ", " 长期\u3000递延收益 "])

    def test_missing_alias_values_are_filled_and_zero_is_preserved(self):
        for missing in [None, "", "--", "—", "N/A"]:
            with self.subTest(missing=missing):
                raw = pd.DataFrame({"递延收益": [missing, 0], "长期递延收益": [3e8, None]})
                original = raw.copy(deep=True)
                result = normalize_balance_sheet(raw)
                self.assertEqual(result["递延收益"].tolist(), [3e8, 0])
                pd.testing.assert_frame_equal(raw, original)

    def test_equal_numeric_aliases_are_not_summed(self):
        result = normalize_balance_sheet(pd.DataFrame([{
            "递延收益": "100,000,000", "DEFER_INCOME": 1e8,
        }]))
        self.assertEqual(result.columns.tolist(), ["递延收益"])
        self.assertEqual(result.iloc[0]["递延收益"], "100,000,000")

    def test_conflicting_aliases_rejected_regardless_of_column_order(self):
        for record in [{"递延收益": 0, "长期递延收益": 3e8},
                       {"长期递延收益": 3e8, "递延收益": 0}]:
            with self.subTest(record=record):
                with self.assertRaisesRegex(ValueError, "conflicting balance sheet aliases for 递延收益"):
                    normalize_balance_sheet(pd.DataFrame([record]))

    def test_conflict_tolerance_is_exactly_one_cent(self):
        for amount, conflict in [(100000000.009, False), (100000000.01, False), (100000000.02, True)]:
            records = [{"递延收益": "100,000,000.00", "DEFER_INCOME": amount},
                       {"DEFER_INCOME": amount, "递延收益": "100,000,000.00"}]
            for record in records:
                with self.subTest(amount=amount, columns=list(record)):
                    raw = pd.DataFrame([record])
                    if conflict:
                        with self.assertRaises(ValueError):
                            normalize_balance_sheet(raw)
                    else:
                        result = normalize_balance_sheet(raw)
                        self.assertEqual(result.columns.tolist(), ["递延收益"])
                        self.assertIn(result.iloc[0]["递延收益"], ["100,000,000.00", amount])

    def test_similar_names_with_different_meanings_stay_separate(self):
        result = normalize_balance_sheet(pd.DataFrame([{
            "递延收益": 3e8, "一年内到期的递延收益": 1e8,
        }]))
        self.assertEqual(result.iloc[0].to_dict(), {"递延收益": 3e8, "一年内到期的递延收益": 1e8})

    def test_duplicate_raw_column_labels_are_coalesced(self):
        result = normalize_balance_sheet(pd.DataFrame([[None, 3e8]], columns=["递延收益", "递延收益"]))
        self.assertEqual(result.iloc[0].to_dict(), {"递延收益": 3e8})


if __name__ == "__main__":
    unittest.main()
