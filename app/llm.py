from __future__ import annotations

import asyncio
import json
import logging
import math
import queue
from typing import Any
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path

import httpx

from app.config import AppConfig
from app.db import db_conn
from app.models import LinkProposalModel, ParagraphAnalysisModel


class LLMClient:
    def __init__(self, config: AppConfig):
        self.config = config
        self._job_state_updater = None
        self._context_provider = None
        self._debug_logger = self._setup_debug_logger()
        self._consecutive_failures = 0

    def _setup_debug_logger(self) -> logging.Logger:
        logs_dir = Path("logs")
        logs_dir.mkdir(parents=True, exist_ok=True)
        logger = logging.getLogger("app.llm_debug")
        if logger.handlers:
            return logger
        logger.setLevel(logging.DEBUG)
        log_queue: queue.Queue[Any] = queue.Queue(-1)
        queue_handler = QueueHandler(log_queue)
        file_handler = logging.FileHandler(logs_dir / "llm_debug.log", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(job_id)s | %(phase)s | %(message)s"))
        listener = QueueListener(log_queue, file_handler, respect_handler_level=True)
        listener.daemon = True
        listener.start()
        logger.addHandler(queue_handler)
        logger.propagate = False
        return logger

    def set_job_state_updater(self, updater):
        self._job_state_updater = updater

    def set_debug_context_provider(self, provider):
        self._context_provider = provider

    def _log_debug(self, level: int, message: str, *args, **kwargs):
        context = self._context_provider() if self._context_provider else {}
        extra = {
            "job_id": context.get("job_id") or "-",
            "phase": context.get("phase") or "-",
        }
        self._debug_logger.log(level, message, *args, extra=extra, **kwargs)

    def _update_job_state(self, **updates):
        if self._job_state_updater:
            self._job_state_updater(**updates)

    def estimate_tokens(self, text: str) -> int:
        return max(1, math.ceil(len(text) / 4))

    def cap_text(self, text: str, budget: int) -> str:
        max_chars = budget * 4
        return text[:max_chars]

    def _max_tokens(self) -> int:
        return int(self.config.max_tokens or self.config.token_budget)

    def _error_text(self, resp: httpx.Response) -> str:
        body = resp.text.strip()
        return f"HTTP {resp.status_code}: {body}"[:2000]

    def _is_response_format_unsupported(self, resp: httpx.Response) -> bool:
        if resp.status_code != 400:
            return False
        body_text = resp.text.lower()
        if "response_format" in body_text:
            return True
        try:
            body = resp.json()
        except Exception:
            return False
        error = body.get("error") if isinstance(body, dict) else None
        code = (error or {}).get("code") if isinstance(error, dict) else None
        code_text = str(code).lower() if code is not None else ""
        return "response_format" in code_text or "unsupported" in code_text

    def _record_failure(self, error_text: str, status_code: int | None, payload: dict[str, Any]):
        with db_conn(self.config.db_path) as conn:
            conn.execute(
                """INSERT INTO llm_failures(model,endpoint,status_code,error_text,request_payload_json)
                VALUES(?,?,?,?,?)""",
                (
                    self.config.model,
                    f"{self.config.base_url}/chat/completions",
                    status_code,
                    error_text[:2000],
                    json.dumps(payload, ensure_ascii=False)[:4000],
                ),
            )

    async def call_json(self, system: str, user: str) -> dict[str, Any]:
        payload = {
            "model": self.config.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "max_tokens": self._max_tokens(),
        }
        headers = {"Authorization": f"Bearer {self.config.api_key}"}
        backoff = 1
        for attempt in range(1, self.config.retries + 1):
            try:
                for without_response_format in (False, True):
                    request_payload = dict(payload)
                    if without_response_format:
                        request_payload.pop("response_format", None)
                        request_payload["messages"] = [
                            {
                                "role": "system",
                                "content": f"{system}\n\nReturn ONLY a valid JSON object. No markdown. No prose.",
                            },
                            {"role": "user", "content": user},
                        ]
                    async with httpx.AsyncClient(timeout=self.config.timeout_seconds) as client:
                        self._log_debug(
                            logging.DEBUG,
                            "LLM request started model=%s base_url=%s api_key_set=%s prompt_length=%s",
                            self.config.model,
                            self.config.base_url,
                            bool(self.config.api_key),
                            len(f"{system}\n{user}"),
                        )
                        resp = await client.post(f"{self.config.base_url}/chat/completions", json=request_payload, headers=headers)
                    response_excerpt = (resp.text or "")[:2000]
                    self._update_job_state(last_llm_status_code=resp.status_code, last_llm_response_excerpt=response_excerpt)
                    self._log_debug(logging.DEBUG, "LLM response received status=%s body=%s", resp.status_code, response_excerpt)
                    if resp.status_code == 429:
                        raise httpx.HTTPStatusError("rate limit", request=resp.request, response=resp)
                    if not without_response_format and self._is_response_format_unsupported(resp):
                        continue
                    resp.raise_for_status()
                    content = resp.json()["choices"][0]["message"]["content"]
                    self._consecutive_failures = 0
                    self._update_job_state(consecutive_llm_failures=0)
                    return json.loads(content)
                raise RuntimeError("LLM request exhausted fallback paths")
            except Exception as exc:
                status_code = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) and exc.response else None
                error_text = self._error_text(exc.response) if isinstance(exc, httpx.HTTPStatusError) and exc.response else str(exc)
                response_excerpt = exc.response.text[:2000] if isinstance(exc, httpx.HTTPStatusError) and exc.response else None
                self._consecutive_failures += 1
                self._update_job_state(
                    last_error=str(exc),
                    llm_failed_increment=1,
                    last_llm_status_code=status_code,
                    last_llm_response_excerpt=response_excerpt,
                    consecutive_llm_failures=self._consecutive_failures,
                )
                if self._consecutive_failures > 20:
                    self._update_job_state(phase="error", running=False)
                self._log_debug(logging.ERROR, "LLM request failed: %s", str(exc), exc_info=True)
                self._record_failure(error_text, status_code, request_payload)
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



    async def classify_noise_line(self, line: str, prev_line: str | None = None, next_line: str | None = None) -> dict[str, str]:
        context = f"Vorher: {prev_line or ''}\nZeile: {line}\nNachher: {next_line or ''}"
        user = (
            "Klassifiziere diese OCR-Zeile als KEEP oder REMOVE. "
            "Bei Unsicherheit KEEP. Antworte exakt als JSON mit Feldern action und reason.\n"
            f"{context}"
        )
        data = await self.call_json("Du entfernst OCR-Noise, ohne Kontenüberschriften zu löschen.", user)
        action = str(data.get("action", "KEEP")).upper()
        if action not in {"KEEP", "REMOVE"}:
            action = "KEEP"
        reason = str(data.get("reason", "fallback_keep"))
        return {"action": action, "reason": reason}

    async def classify_link(self, left: str, right: str) -> LinkProposalModel:
        user = f"Kläger-Argument:\n{self.cap_text(left,600)}\n\nBeklagten-Argument:\n{self.cap_text(right,600)}"
        data = await self.call_json("Bestimme Link-Typ und gib JSON.", user)
        try:
            return LinkProposalModel.model_validate(data)
        except Exception:
            fixed = await self.call_json("Return valid JSON only.", json.dumps(data, ensure_ascii=False))
            return LinkProposalModel.model_validate(fixed)
