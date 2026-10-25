from __future__ import annotations

from typing import Any
from math import isfinite

import pandas as pd
from fastapi import HTTPException

from backend.app.config.balance_groups import BALANCE_GROUPS, EXCLUDED_BALANCE_FIELDS
from backend.app.providers.balance_sheet.normalization import normalize_balance_sheet


REPORT_DATE_COLUMN = "报告日"
UNMATCHED_ITEM_LABEL = "其他未单列项目"


class BalanceSheetAggregationService:
    def aggregate(self, raw_df: pd.DataFrame, symbol: str, years: int = 3) -> list[dict[str, Any]]:
        if not isinstance(raw_df, pd.DataFrame) or raw_df.empty:
            raise HTTPException(status_code=404, detail=f"balance sheet data is empty for symbol {symbol}")
        if years <= 0:
            raise HTTPException(status_code=400, detail="years must be a positive integer")

        cleaned = normalize_balance_sheet(raw_df)
        if REPORT_DATE_COLUMN not in cleaned.columns:
            raise HTTPException(status_code=500, detail=f"balance sheet data missing {REPORT_DATE_COLUMN} column")
        known_fields = {"资产总计", "负债合计"} | {
            matcher["pattern"] for items in BALANCE_GROUPS.values() for item in items for matcher in item["matchers"]
        }
        if not known_fields.intersection(cleaned.columns):
            raise HTTPException(status_code=500, detail="balance sheet data has no recognized amount fields")

        # Integer YYYYMMDD values must not be interpreted as nanoseconds since 1970.
        cleaned[REPORT_DATE_COLUMN] = pd.to_datetime(cleaned[REPORT_DATE_COLUMN].astype(str), errors="coerce", format="mixed")
        if "更新日期" in cleaned:
            cleaned = cleaned.assign(_updated=pd.to_datetime(cleaned["更新日期"], errors="coerce", format="mixed"))
            cleaned = cleaned.sort_values("_updated", na_position="first", kind="stable")
        cleaned = (
            cleaned.dropna(subset=[REPORT_DATE_COLUMN])
            .loc[lambda df: df[REPORT_DATE_COLUMN].dt.is_quarter_end]
            .drop_duplicates(subset=[REPORT_DATE_COLUMN], keep="last")
            .sort_values(REPORT_DATE_COLUMN, ascending=False)
            .reset_index(drop=True)
        )
        if cleaned.empty:
            raise HTTPException(status_code=404, detail=f"no valid quarterly balance sheet rows for symbol {symbol}")

        latest_report_date = cleaned.iloc[0][REPORT_DATE_COLUMN]
        cutoff_date = latest_report_date - pd.DateOffset(years=years)
        cleaned = cleaned.loc[cleaned[REPORT_DATE_COLUMN] >= cutoff_date].reset_index(drop=True)
        if cleaned.empty:
            raise HTTPException(status_code=404, detail=f"no balance sheet rows found within the last {years} years for symbol {symbol}")

        periods: list[dict[str, Any]] = []
        for _, row in cleaned.iterrows():
            record = row.to_dict()
            assets = self._aggregate_group(record, BALANCE_GROUPS["assets"], "资产总计")
            liabilities = self._aggregate_group(record, BALANCE_GROUPS["liabilities"], "负债合计")
            periods.append(
                {
                    "report_date": pd.Timestamp(record[REPORT_DATE_COLUMN]).strftime("%Y-%m-%d"),
                    "assets": assets,
                    "liabilities": liabilities,
                    "assets_total": self._reported_total(record, "资产总计", assets),
                    "liabilities_total": self._reported_total(record, "负债合计", liabilities),
                }
            )
        return periods

    def _aggregate_group(self, record: dict[str, Any], group_items: list[dict[str, Any]], total_field: str) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []

        for item in group_items:
            details: list[dict[str, Any]] = []
            total_value = 0.0

            for column, raw_value in record.items():
                if column in EXCLUDED_BALANCE_FIELDS:
                    continue
                matched_rule = self._find_match_rule(str(column), item.get("matchers", []))
                if not matched_rule:
                    continue

                numeric_value = self._coerce_numeric(raw_value)
                if numeric_value == 0:
                    continue

                included_in_total = bool(matched_rule.get("include_in_total", True))
                if any(self._has_numeric(record.get(field)) for field in matched_rule.get("fallback_for", ())):
                    included_in_total = False
                details.append(
                    {
                        "label": matched_rule.get("detail_label") or str(column),
                        "value": self._to_yi(numeric_value),
                        "included_in_total": included_in_total,
                    }
                )
                if included_in_total:
                    total_value += numeric_value

            result.append(
                {
                    "key": item["key"],
                    "label": item["label"],
                    "section": item["section"],
                    "residual_of": item.get("residual_of"),
                    "value": total_value,
                    "details": details,
                }
            )

        for item in result:
            if not item["residual_of"]:
                continue
            section_sum = sum(
                candidate["value"]
                for candidate in result
                if candidate["section"] == item["section"] and candidate["key"] != item["key"]
            )
            total_amount = record.get(item["residual_of"])
            if not self._has_numeric(total_amount):
                continue
            residual_value = self._coerce_numeric(total_amount) - section_sum
            unmatched_value = residual_value - item["value"]
            item["value"] = residual_value
            if abs(unmatched_value) > 0.01:
                item["details"].append(
                    {
                        "label": UNMATCHED_ITEM_LABEL if unmatched_value > 0 else "分类与报表差额",
                        "value": self._to_yi(unmatched_value),
                        "included_in_total": True,
                    }
                )

        # Financial companies may expose only overall totals, without current/non-current subtotals.
        if self._has_numeric(record.get(total_field)):
            difference = self._coerce_numeric(record[total_field]) - sum(item["value"] for item in result)
            if abs(difference) > 0.01:
                result.append({
                    "key": "unclassified",
                    "label": "未分类资产" if total_field == "资产总计" else "未分类负债",
                    "value": difference,
                    "details": [{
                        "label": UNMATCHED_ITEM_LABEL if difference > 0 else "分类与报表差额",
                        "value": self._to_yi(difference),
                        "included_in_total": True,
                    }],
                })

        return [
            {
                "key": item["key"],
                "label": item["label"],
                "value": self._to_yi(item["value"]),
                "details": sorted(item["details"], key=lambda detail: detail["value"], reverse=True),
            }
            for item in result
        ]

    @staticmethod
    def _find_match_rule(column: str, matchers: list[dict[str, Any]]) -> dict[str, Any] | None:
        for matcher in matchers:
            pattern = str(matcher["pattern"])
            match_type = matcher.get("match", "contains")
            if match_type == "exact" and column == pattern:
                return matcher
            if match_type == "contains" and pattern in column:
                return matcher
        return None

    @staticmethod
    def _has_numeric(value: Any) -> bool:
        try:
            return isfinite(float(str(value).strip().replace(",", "")))
        except (TypeError, ValueError):
            return False

    @classmethod
    def _reported_total(cls, record: dict[str, Any], field: str, items: list[dict[str, Any]]) -> float:
        if cls._has_numeric(record.get(field)):
            return cls._to_yi(cls._coerce_numeric(record[field]))
        return round(sum(item["value"] for item in items), 2)

    @staticmethod
    def _coerce_numeric(value: Any) -> float:
        if pd.isna(value):
            return 0.0
        if isinstance(value, (int, float)):
            return float(value) if isfinite(float(value)) else 0.0
        text = str(value).strip().replace(",", "")
        if not text or text in {"--", "nan", "None"}:
            return 0.0
        try:
            number = float(text)
            return number if isfinite(number) else 0.0
        except ValueError:
            return 0.0

    @staticmethod
    def _to_yi(value: float) -> float:
        return round(value / 1e8, 2)
