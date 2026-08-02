from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Callable

import akshare as ak
import pandas as pd


logger = logging.getLogger(__name__)


def latest_workday(today: date) -> date:
    """回退到最近的周一~周五。纯本地计算，不含节假日信息。"""
    cursor = today
    while cursor.weekday() >= 5:  # 5=周六 6=周日
        cursor -= timedelta(days=1)
    return cursor


def _load_from_akshare() -> list[date]:
    df = ak.tool_trade_date_hist_sina()
    series = pd.to_datetime(df.iloc[:, 0], errors="coerce").dropna()
    return sorted(value.date() for value in series)


class TradeCalendar:
    """两级新鲜度门控。

    一级 latest_workday 纯本地、零网络；二级 latest_trade_day 使用真实交易日历。
    交易日集合是工作日集合的子集，故 latest_trade_day <= latest_workday，
    一级判定为新鲜时必然真的新鲜，不会漏判。
    """

    def __init__(self, loader: Callable[[], list[date]] | None = None) -> None:
        self._loader = loader or _load_from_akshare
        self._cached_days: list[date] | None = None
        self._cached_on: date | None = None

    def latest_trade_day(self, today: date) -> date | None:
        past = [day for day in self._trade_days(today) if day <= today]
        return past[-1] if past else None

    def is_stale(self, watermark: date | None, today: date) -> bool:
        if watermark is None:
            return True
        if watermark >= latest_workday(today):
            return False  # 一级放行，不触碰日历

        try:
            trade_day = self.latest_trade_day(today)
        except Exception as exc:
            logger.warning("trade calendar unavailable, degrade to workday gate: %s", exc)
            return True  # 一级已判陈旧，日历不可用时保持该结论

        if trade_day is None:
            return True
        return watermark < trade_day

    def _trade_days(self, today: date) -> list[date]:
        if self._cached_days is not None and self._cached_on == today:
            return self._cached_days
        days = self._loader()
        self._cached_days = days
        self._cached_on = today
        return days
