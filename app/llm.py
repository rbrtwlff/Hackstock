from __future__ import annotations

import asyncio
import json
import math
from typing import Any

import httpx

from app.config import AppConfig
from app.models import LinkProposalModel, ParagraphAnalysisModel


class LLMClient:
    def __init__(self, config: AppConfig):
        self.config = config

    def estimate_tokens(self, text: str) -> int:
        return max(1, math.ceil(len(text) / 4))

    def cap_text(self, text: str, budget: int) -> str:
        max_chars = budget * 4
        return text[:max_chars]

    async def call_json(self, system: str, user: str) -> dict[str, Any]:
        payload = {
            "model": self.config.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": 0,
            "response_format": {"type": "json_object"},
        }
        headers = {"Authorization": f"Bearer {self.config.api_key}"}
        backoff = 1
        for attempt in range(1, self.config.retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self.config.timeout_seconds) as client:
                    resp = await client.post(f"{self.config.base_url}/chat/completions", json=payload, headers=headers)
                if resp.status_code == 429:
                    raise httpx.HTTPStatusError("rate limit", request=resp.request, response=resp)
                resp.raise_for_status()
                content = resp.json()["choices"][0]["message"]["content"]
                return json.loads(content)
            except Exception:
                if attempt == self.config.retries:
                    raise
                await asyncio.sleep(backoff)
                backoff *= 2
        raise RuntimeError("unreachable")

    async def analyze_paragraph(self, text: str, context: str) -> ParagraphAnalysisModel:
        text = self.cap_text(text, self.config.token_budget)
        context = self.cap_text(context, max(200, self.config.token_budget // 3))
        user = f"Kontext:\n{context}\n\nAbsatz:\n{text}\n\nGib NUR valides JSON laut Schema zurück."
        system = "Du analysierst Schriftsatz-Absätze auf Deutsch."
        data = await self.call_json(system, user)
        try:
            return ParagraphAnalysisModel.model_validate(data)
        except Exception:
            repair_user = f"Mache aus folgendem JSON valides JSON für das gewünschte Schema, ohne Zusatztext:\n{json.dumps(data, ensure_ascii=False)}"
            fixed = await self.call_json("Return valid JSON only.", repair_user)
            return ParagraphAnalysisModel.model_validate(fixed)

    async def classify_link(self, left: str, right: str) -> LinkProposalModel:
        user = f"Kläger-Argument:\n{self.cap_text(left,600)}\n\nBeklagten-Argument:\n{self.cap_text(right,600)}"
        data = await self.call_json("Bestimme Link-Typ und gib JSON.", user)
        try:
            return LinkProposalModel.model_validate(data)
        except Exception:
            fixed = await self.call_json("Return valid JSON only.", json.dumps(data, ensure_ascii=False))
            return LinkProposalModel.model_validate(fixed)
