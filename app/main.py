from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR / "data" / "viewer_data.json"

app = FastAPI(title="Hackstock Viewer")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


class LinkUpdate(BaseModel):
    status: str = Field(pattern="^(proposed|confirmed|rejected)$")


class LinkCreate(BaseModel):
    source_id: str
    target_id: str
    link_type: str
    confidence: float | None = None
    status: str = Field(pattern="^(proposed|confirmed|rejected)$")


def load_data() -> dict[str, Any]:
    return json.loads(DATA_FILE.read_text(encoding="utf-8"))


def save_data(data: dict[str, Any]) -> None:
    DATA_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(BASE_DIR / "static" / "index.html")


@app.get("/api/data")
def get_data() -> dict[str, Any]:
    return load_data()


@app.patch("/api/links/{link_id}")
def update_link(link_id: str, payload: LinkUpdate) -> dict[str, Any]:
    data = load_data()
    for link in data["links"]:
        if link["id"] == link_id:
            link["status"] = payload.status
            save_data(data)
            return link
    raise HTTPException(status_code=404, detail="Link nicht gefunden")


@app.delete("/api/links/{link_id}")
def delete_link(link_id: str) -> dict[str, str]:
    data = load_data()
    before = len(data["links"])
    data["links"] = [link for link in data["links"] if link["id"] != link_id]
    if len(data["links"]) == before:
        raise HTTPException(status_code=404, detail="Link nicht gefunden")
    save_data(data)
    return {"status": "ok"}


@app.post("/api/links")
def create_link(payload: LinkCreate) -> dict[str, Any]:
    data = load_data()
    link_id = f"L{len(data['links']) + 1:03d}"
    new_link = payload.model_dump()
    new_link["id"] = link_id
    data["links"].append(new_link)
    save_data(data)
    return new_link
