from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any, Callable


DEFAULT_BASE_URL = "https://api.moonshot.ai/v1"


@dataclass
class JobTelemetry:
    model: str
    prompt_version: str
    request_id: str | None
    duration_ms: int
    prompt_tokens: int | None
    completion_tokens: int | None
    total_tokens: int | None


@dataclass
class LLMResult:
    payload: dict[str, Any]
    telemetry: JobTelemetry


class LLMIntegrationError(RuntimeError):
    """Raised when an LLM request or response cannot be processed."""


class OpenAICompatibleLLMClient:
    def __init__(
        self,
        api_key: str,
        model: str,
        prompt_version: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: int = 30,
        transport: Callable[[urllib.request.Request, int], dict[str, Any]] | None = None,
    ) -> None:
        self.api_key = api_key
        self.model = model
        self.prompt_version = prompt_version
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._transport = transport or self._default_transport

    def run_job(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_tokens: int,
        temperature: float = 0.2,
    ) -> LLMResult:
        started = time.perf_counter()

        request_body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "response_format": {"type": "json_object"},
        }

        response = self._request(request_body)
        used_fallback = False
        if self._response_format_unsupported(response):
            used_fallback = True
            fallback_system_prompt = (
                f"{system_prompt}\n\n"
                "You must return a valid JSON object only."
                " Do not include markdown, prose, or code fences."
            )
            fallback_body = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": fallback_system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
            response = self._request(fallback_body)

        payload = self._extract_payload(response, allow_repair=used_fallback)
        elapsed_ms = int((time.perf_counter() - started) * 1000)

        usage = response.get("usage") or {}
        telemetry = JobTelemetry(
            model=self.model,
            prompt_version=self.prompt_version,
            request_id=self._extract_request_id(response),
            duration_ms=elapsed_ms,
            prompt_tokens=usage.get("prompt_tokens"),
            completion_tokens=usage.get("completion_tokens"),
            total_tokens=usage.get("total_tokens"),
        )

        return LLMResult(payload=payload, telemetry=telemetry)

    def _request(self, request_body: dict[str, Any]) -> dict[str, Any]:
        endpoint = f"{self.base_url}/chat/completions"
        req = urllib.request.Request(
            endpoint,
            data=json.dumps(request_body).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "X-Request-ID": str(uuid.uuid4()),
            },
            method="POST",
        )
        try:
            return self._transport(req, self.timeout_seconds)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise LLMIntegrationError(f"LLM request failed ({exc.code}): {body}") from exc
        except urllib.error.URLError as exc:
            raise LLMIntegrationError(f"LLM request failed: {exc.reason}") from exc

    @staticmethod
    def _default_transport(req: urllib.request.Request, timeout_seconds: int) -> dict[str, Any]:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))

    @staticmethod
    def _response_format_unsupported(response: dict[str, Any]) -> bool:
        error = response.get("error")
        if not isinstance(error, dict):
            return False
        message = str(error.get("message", "")).lower()
        code = str(error.get("code", "")).lower()
        return "response_format" in message or "response_format" in code

    @staticmethod
    def _extract_payload(response: dict[str, Any], allow_repair: bool) -> dict[str, Any]:
        choices = response.get("choices")
        if not choices:
            raise LLMIntegrationError(f"No choices in response: {response}")

        message = choices[0].get("message") if isinstance(choices[0], dict) else None
        if not isinstance(message, dict):
            raise LLMIntegrationError(f"No message in first choice: {choices[0]}")

        content = message.get("content")
        if not isinstance(content, str):
            raise LLMIntegrationError(f"Expected text content, got: {type(content)}")

        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            if not allow_repair:
                raise LLMIntegrationError("Response content is not valid JSON")
            parsed = OpenAICompatibleLLMClient._repair_json(content)

        if not isinstance(parsed, dict):
            raise LLMIntegrationError("Expected JSON object response")
        return parsed

    @staticmethod
    def _repair_json(content: str) -> dict[str, Any]:
        stripped = content.strip()

        if stripped.startswith("```"):
            lines = [line for line in stripped.splitlines() if not line.startswith("```")]
            stripped = "\n".join(lines).strip()

        if "{" in stripped and "}" in stripped:
            start = stripped.find("{")
            end = stripped.rfind("}") + 1
            stripped = stripped[start:end]

        try:
            repaired = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise LLMIntegrationError("Unable to parse JSON response after repair") from exc

        if not isinstance(repaired, dict):
            raise LLMIntegrationError("Repaired JSON is not an object")
        return repaired

    @staticmethod
    def _extract_request_id(response: dict[str, Any]) -> str | None:
        request_id = response.get("request_id") or response.get("id")
        if request_id is None:
            return None
        return str(request_id)
