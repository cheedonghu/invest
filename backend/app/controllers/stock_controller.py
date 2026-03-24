from __future__ import annotations

from fastapi import APIRouter, Query

from backend.app.services.stock_service import StockService


router = APIRouter(prefix="/api/stocks", tags=["stocks"])
stock_service = StockService()


@router.get("/name")
def get_stock_name(symbol: str = Query(..., description="6-digit A-share stock code")) -> dict:
    return stock_service.get_stock_name(symbol)


@router.get("/valuation-metrics")
def get_stock_valuation_metrics(
    symbol: str = Query(..., description="6-digit A-share stock code"),
    years: int = Query(10, ge=1, le=10, description="Number of recent years of valuation metrics to process"),
) -> dict:
    return stock_service.get_stock_valuation_metrics(symbol, years=years)


@router.get("/market-performance")
def get_stock_market_performance(
    symbol: str = Query(..., description="6-digit A-share stock code"),
    years: int = Query(10, ge=1, le=10, description="Number of recent years of market performance data to process"),
) -> dict:
    return stock_service.get_stock_market_performance(symbol, years=years)


@router.get("/balance-sheet")
def get_stock_balance_sheet(
    symbol: str = Query(..., description="6-digit A-share stock code"),
    years: int = Query(3, ge=1, le=20, description="Number of recent years of balance sheet periods to process"),
) -> dict:
    return stock_service.get_stock_balance_sheet(symbol, years=years)
