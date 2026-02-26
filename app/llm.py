from __future__ import annotations

import asyncio
import json
import logging
import math
import random
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

    def _temperature_for_model(self) -> int:
        model = (self.config.model or "").lower()
        # Moonshot Kimi K2.5 variants reject non-1 temperatures (API returns HTTP 400 otherwise).
        if "kimi-k2.5" in model:
            return 1
        return 0

    def _apply_model_overrides(self, payload: dict[str, Any]) -> dict[str, Any]:
        model = str(payload.get("model") or "")
        if "kimi-k2.5" in model.lower():
            payload["temperature"] = 1
        return payload

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

    def _is_engine_overloaded(self, resp: httpx.Response) -> bool:
        if resp.status_code == 429:
            return True
        try:
            body = resp.json()
        except Exception:
            return False
        error = body.get("error") if isinstance(body, dict) else None
        error_type = (error or {}).get("type") if isinstance(error, dict) else None
        return str(error_type).lower() == "engine_overloaded_error"

    def _retry_delay(self, attempt: int) -> float:
        schedule = [2, 4, 8, 16, 24, 30]
        idx = min(max(attempt - 1, 0), len(schedule) - 1)
        return schedule[idx] + random.uniform(0, 1)

    def _is_retryable_http_status(self, resp: httpx.Response) -> bool:
        return resp.status_code in {429, 502, 503, 504}

    def _is_non_retryable_http_status(self, resp: httpx.Response) -> bool:
        if resp.status_code in {401, 403}:
            return True
        if resp.status_code == 400:
            try:
                body = resp.json()
            except Exception:
                body = None
            error = body.get("error") if isinstance(body, dict) else None
            error_type = (error or {}).get("type") if isinstance(error, dict) else None
            return str(error_type).lower() == "invalid_request_error"
        return False

    def _first_json_object(self, content: str) -> str | None:
        start = content.find("{")
        if start < 0:
            return None
        depth = 0
        in_string = False
        escaped = False
        for idx in range(start, len(content)):
            ch = content[idx]
            if in_string:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_string = False
                continue
            if ch == '"':
                in_string = True
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return content[start : idx + 1]
        return None

    async def call_json(self, system: str, user: str) -> dict[str, Any]:
        payload = {
            "model": self.config.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": self._temperature_for_model(),
            "response_format": {"type": "json_object"},
            "max_tokens": self._max_tokens(),
        }
        payload = self._apply_model_overrides(payload)
        headers = {"Authorization": f"Bearer {self.config.api_key}"}
        timeout = httpx.Timeout(connect=20.0, read=240.0, write=60.0, pool=20.0)
        max_attempts = 6
        empty_content_retry_count = 0
        parse_retry_count = 0
        for attempt in range(1, max_attempts + 1):
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
                    request_payload = self._apply_model_overrides(request_payload)
                    async with httpx.AsyncClient(timeout=timeout) as client:
                        self._log_debug(
                            logging.DEBUG,
                            "LLM request started model=%s base_url=%s api_key_set=%s prompt_length=%s",
                            self.config.model,
                            self.config.base_url,
                            bool(self.config.api_key),
                            len(f"{system}\n{user}"),
                        )
                        self._log_debug(
                            logging.DEBUG,
                            "Sending LLM payload model=%s temperature=%s keys=%s",
                            request_payload.get("model"),
                            request_payload.get("temperature"),
                            list(request_payload.keys())[:12],
                        )
                        resp = await client.post(f"{self.config.base_url}/chat/completions", json=request_payload, headers=headers)
                    response_excerpt = (resp.text or "")[:2000]
                    self._update_job_state(last_llm_status_code=resp.status_code, last_llm_response_excerpt=response_excerpt)
                    self._log_debug(logging.DEBUG, "LLM response received status=%s body=%s", resp.status_code, response_excerpt)
                    if self._is_retryable_http_status(resp):
                        raise httpx.HTTPStatusError("retryable LLM status", request=resp.request, response=resp)
                    if not without_response_format and self._is_response_format_unsupported(resp):
                        continue
                    resp.raise_for_status()
                    content = resp.json()["choices"][0]["message"]["content"]
                    if not str(content).strip():
                        if empty_content_retry_count >= 3:
                            raise RuntimeError("LLM content empty after retries")
                        empty_content_retry_count += 1
                        wait_seconds = empty_content_retry_count + random.uniform(0, 1)
                        self._log_debug(
                            logging.WARNING,
                            "LLM returned empty content. Retry %s/%s in %.2fs",
                            empty_content_retry_count,
                            3,
                            wait_seconds,
                        )
                        await asyncio.sleep(wait_seconds)
                        continue
                    candidate = content if content.strip().startswith("{") else (self._first_json_object(content) or content)
                    try:
                        parsed = json.loads(candidate)
                    except json.JSONDecodeError:
                        if parse_retry_count >= 1:
                            raise
                        parse_retry_count += 1
                        self._log_debug(
                            logging.WARNING,
                            "Failed to parse LLM JSON (attempt %s/1). Content excerpt=%s",
                            parse_retry_count,
                            str(content)[:500],
                        )
                        await asyncio.sleep(0.5 + random.uniform(0, 1))
                        continue
                    self._consecutive_failures = 0
                    self._update_job_state(consecutive_llm_failures=0)
                    return parsed
                raise RuntimeError("LLM request exhausted fallback paths")
            except Exception as exc:
                status_code = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) and exc.response else None
                error_text = self._error_text(exc.response) if isinstance(exc, httpx.HTTPStatusError) and exc.response else f"{type(exc).__name__}: {repr(exc)}"
                response_excerpt = exc.response.text[:2000] if isinstance(exc, httpx.HTTPStatusError) and exc.response else None
                if status_code is not None:
                    error_summary = f"HTTP {status_code}: {type(exc).__name__}: {repr(exc)}"
                else:
                    error_summary = f"{type(exc).__name__}: {repr(exc)}"
                self._consecutive_failures += 1
                self._update_job_state(
                    last_error=error_summary,
                    llm_failed_increment=1,
                    last_llm_status_code=status_code,
                    last_llm_response_excerpt=response_excerpt,
                    consecutive_llm_failures=self._consecutive_failures,
                )
                if self._consecutive_failures > 20:
                    self._update_job_state(phase="error", running=False)
                self._log_debug(logging.ERROR, f"LLM request failed: {type(exc).__name__}: {repr(exc)}", exc_info=True)
                self._record_failure(error_text, status_code, request_payload)
                retryable_exception = isinstance(exc, (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError, httpx.TransportError))
                retryable_http = isinstance(exc, httpx.HTTPStatusError) and exc.response is not None and self._is_retryable_http_status(exc.response)
                non_retryable_http = isinstance(exc, httpx.HTTPStatusError) and exc.response is not None and self._is_non_retryable_http_status(exc.response)
                if non_retryable_http or (not retryable_exception and not retryable_http):
                    raise
                if attempt == max_attempts:
                    final_error = f"LLM call failed after {max_attempts} attempts: {error_summary}"
                    self._update_job_state(last_error=final_error)
                    raise RuntimeError(final_error) from exc
                wait_seconds = self._retry_delay(attempt)
                retry_reason = error_summary if status_code is not None else f"{type(exc).__name__}: {repr(exc)}"
                self._log_debug(
                    logging.WARNING,
                    "Retrying LLM call attempt=%s/%s in %.2fs due to %s",
                    attempt + 1,
                    max_attempts,
                    wait_seconds,
                    retry_reason[:600],
                )
                await asyncio.sleep(wait_seconds)
        raise RuntimeError("unreachable")

    async def analyze_paragraph(self, text: str, context: str) -> ParagraphAnalysisModel:
        text = self.cap_text(text, self.config.token_budget)
        context = self.cap_text(context, max(200, self.config.token_budget // 3))
        user = (
            f"Kontext:\n{context}\n\nAbsatz:\n{text}\n\n"
            "Gib NUR valides JSON laut Schema zurück."
        )
        system = (
            "Du analysierst Schriftsatz-Absätze auf Deutsch. "
            "Return ONLY valid JSON matching this schema, no extra keys: "
            "{"
            '"keywords": ["..."], '
            '"issues": ["..."], '
            '"role": "FACT_ASSERTION|FACT_DENIAL|LEGAL_POSITION|SUBSUMPTION|CONTRACT_INTERPRETATION|CALCULATION|EVIDENCE_OFFER|EVIDENCE_ATTACK|PROCEDURAL|BACKGROUND_NARRATIVE|REQUEST_RELIEF|OTHER", '
            '"summary_3_sentences": "exactly three sentences", '
            '"continuation_of_previous": true|false, '
            '"continuation_reason": "optional", '
            '"citations_norms": ["..."], '
            '"citations_cases": ["..."], '
            '"citations_contract": ["..."], '
            '"citations_exhibits": ["..."]'
            "}."
        )
        data = await self.call_json(system, user)
        try:
            allowed = {key: data[key] for key in ParagraphAnalysisModel.model_fields.keys() if key in data}
            return ParagraphAnalysisModel.model_validate(allowed)
        except Exception:
            repair_user = f"Mache aus folgendem JSON valides JSON für das gewünschte Schema, ohne Zusatztext:\n{json.dumps(data, ensure_ascii=False)}"
            fixed = await self.call_json("Return valid JSON only.", repair_user)
            allowed = {key: fixed[key] for key in ParagraphAnalysisModel.model_fields.keys() if key in fixed}
            return ParagraphAnalysisModel.model_validate(allowed)



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
