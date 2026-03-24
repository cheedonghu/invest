from __future__ import annotations

from fastapi import APIRouter, Query

from backend.app.services.stock_backup_service import StockBackupService


router = APIRouter(prefix="/api/ops", tags=["ops"])
stock_backup_service = StockBackupService()


# @router.post("/backup/stock-value")
@router.get("/backup/stock-value")
def backup_stock_value(symbol: str = Query(..., description="6-digit A-share stock code")) -> dict:
    return stock_backup_service.backup_stock_value(symbol)
