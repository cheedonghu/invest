from __future__ import annotations

from pydantic import BaseModel, Field
from fastapi import APIRouter

from backend.app.services.marked_stock_service import MarkedStockService


router = APIRouter(prefix="/api/stocks/marked", tags=["marked-stocks"])
marked_stock_service = MarkedStockService()


class UpsertMarkedStockRequest(BaseModel):
    symbol: str = Field(..., description="6-digit A-share stock code")
    mark_reason: str = Field(..., description="Reason for marking the stock")
    name: str | None = Field(default=None, description="Optional stock name")


@router.get("/")
def list_marked_stocks() -> dict:
    return marked_stock_service.list_marked_stocks()


@router.post("/")
def upsert_marked_stock(payload: UpsertMarkedStockRequest) -> dict:
    return marked_stock_service.upsert_marked_stock(
        symbol=payload.symbol,
        mark_reason=payload.mark_reason,
        name=payload.name,
    )
