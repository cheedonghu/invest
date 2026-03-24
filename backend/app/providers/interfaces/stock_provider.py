from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class BalanceSheetProvider(ABC):
    source_name: str

    @abstractmethod
    def fetch(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError


class ValuationMetricsProvider(ABC):
    source_name: str

    @abstractmethod
    def fetch(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError


class MarketPerformanceProvider(ABC):
    source_name: str

    @abstractmethod
    def fetch(self, symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        raise NotImplementedError


class StockNameProvider(ABC):
    source_name: str

    @abstractmethod
    def fetch(self, symbol: str) -> str:
        raise NotImplementedError
