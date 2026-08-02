from __future__ import annotations

from typing import Any

import pandas as pd
from fastapi import HTTPException

from backend.app.config.balance_groups import BALANCE_GROUPS, EXCLUDED_BALANCE_FIELDS


REPORT_DATE_COLUMN = "报告日"
UNMATCHED_ITEM_LABEL = "其他未单列项目"


class BalanceSheetAggregationService:
    def aggregate(self, raw_df: pd.DataFrame, symbol: str, years: int = 3) -> list[dict[str, Any]]:
        if not isinstance(raw_df, pd.DataFrame) or raw_df.empty:
            raise HTTPException(status_code=404, detail=f"balance sheet data is empty for symbol {symbol}")
        if years <= 0:
            raise HTTPException(status_code=400, detail="years must be a positive integer")

        cleaned = raw_df.copy()
        if REPORT_DATE_COLUMN not in cleaned.columns:
            raise HTTPException(status_code=500, detail=f"balance sheet data missing {REPORT_DATE_COLUMN} column")

        cleaned[REPORT_DATE_COLUMN] = pd.to_datetime(cleaned[REPORT_DATE_COLUMN], errors="coerce")
        cleaned = (
            cleaned.dropna(subset=[REPORT_DATE_COLUMN])
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
            periods.append(
                {
                    "report_date": pd.Timestamp(record[REPORT_DATE_COLUMN]).strftime("%Y-%m-%d"),
                    "assets": self._aggregate_group(record, BALANCE_GROUPS["assets"]),
                    "liabilities": self._aggregate_group(record, BALANCE_GROUPS["liabilities"]),
                }
            )
        return periods

    def _aggregate_group(self, record: dict[str, Any], group_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
                if numeric_value <= 0:
                    continue

                included_in_total = bool(matched_rule.get("include_in_total", True))
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
            total_amount = self._coerce_numeric(record.get(item["residual_of"]))
            residual_value = max(total_amount - section_sum, item["value"], 0.0)
            unmatched_value = max(residual_value - item["value"], 0.0)
            item["value"] = residual_value
            if unmatched_value > 0:
                item["details"].append(
                    {
                        "label": UNMATCHED_ITEM_LABEL,
                        "value": self._to_yi(unmatched_value),
                        "included_in_total": True,
                    }
                )

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
    def _coerce_numeric(value: Any) -> float:
        if pd.isna(value):
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip().replace(",", "")
        if not text or text in {"--", "nan", "None"}:
            return 0.0
        try:
            return float(text)
        except ValueError:
            return 0.0

    @staticmethod
    def _to_yi(value: float) -> float:
        return round(value / 1e8, 2)
