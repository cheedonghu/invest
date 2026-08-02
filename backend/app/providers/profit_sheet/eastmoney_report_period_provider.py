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
