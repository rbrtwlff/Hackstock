from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class AppConfig(BaseModel):
    base_url: str = "https://api.moonshot.ai/v1"
    api_key: str | None = None
    model: str = "moonshotai/kimi-k2.5"
    timeout_seconds: int = 45
    retries: int = 3
    max_parallelism: int = 1
    token_budget: int = 2200
    max_tokens: int | None = None
    logging_mode: str = "errors+metadata"
    host: str = "127.0.0.1"
    port: int = 8000
    db_path: str = "case.sqlite"


def load_config(path: str | Path = "config.yaml") -> AppConfig:
    path = Path(path)
    data: dict = {}
    if path.exists():
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    api_key = data.get("api_key") or os.environ.get("MOONSHOT_API_KEY")
    data["api_key"] = api_key
    return AppConfig(**data)
