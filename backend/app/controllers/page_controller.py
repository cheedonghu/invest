from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse


BASE_DIR = Path(__file__).resolve().parents[3]
STATIC_DIR = BASE_DIR / "frontend"

router = APIRouter(tags=["pages"])


@router.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/balance-sheet")
def balance_sheet_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "balance-sheet.html")


@router.get("/valuation")
def valuation_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "valuation.html")
