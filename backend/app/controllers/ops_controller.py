from __future__ import annotations

from fastapi import APIRouter, Query

from backend.app.services.stock.stock_sync_coordinator import get_default_coordinator


router = APIRouter(prefix="/api/ops", tags=["ops"])

sync_coordinator = get_default_coordinator()


@router.post("/sync/valuation")
def force_sync_valuation(symbol: str = Query(..., description="6-digit A-share stock code")) -> dict:
    """强制同步估值日线，跳过新鲜度门控。冲突时返回 409。"""
    return sync_coordinator.force_sync_valuation(symbol)


@router.post("/sync/profit")
def force_sync_profit(symbol: str = Query(..., description="6-digit A-share stock code")) -> dict:
    """强制同步利润表，跳过报告期探测。冲突时返回 409。"""
    return sync_coordinator.force_sync_profit(symbol)
