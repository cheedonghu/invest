from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from backend.app.controllers.ops_controller import router as ops_router
from backend.app.controllers.page_controller import router as page_router
from backend.app.controllers.stock_controller import router as stock_router


BASE_DIR = Path(__file__).resolve().parents[2]
STATIC_DIR = BASE_DIR / "frontend"


def create_app() -> FastAPI:
    app = FastAPI(title="Stock PE Dashboard")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")
    app.include_router(stock_router)
    app.include_router(ops_router)
    app.include_router(page_router)
    return app


app = create_app()
