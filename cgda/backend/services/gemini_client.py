from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Any, Literal

import requests

from config import settings


RetryReason = Literal["http", "timeout", "invalid_json"]


@dataclass(frozen=True)
class GeminiError(Exception):
    message: str
    model: str
    reason: RetryReason | str
    http_status: int | None = None
    response_snippet: str | None = None

    def __str__(self) -> str:  # pragma: no cover
        base = f"{self.reason} model={self.model}: {self.message}"
        if self.http_status:
            base += f" (HTTP {self.http_status})"
        return base


@dataclass(frozen=True)
class GeminiResult:
    ok: bool
    model_used: str
    parsed_json: Any | None
    raw_text: str | None
    error: str | None
    prompt_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None


class GeminiClient:
    """
    Single wrapper used everywhere for Gemini calls.
    Implements:
    - primary then fallback model selection from config
    - retry (max 2) with exponential backoff per model
    - invalid-JSON retry + fallback
    - always continues upstream (returns GeminiResult with error if all fail)
    """

    def __init__(self) -> None:
        pass

    def generate_json(
        self,
        *,
        prompt: str,
        temperature: float | None = None,
        max_output_tokens: int | None = None,
        expect: Literal["dict", "list", "any"] = "any",
        timeout_s: int | None = None,
    ) -> GeminiResult:
        if not settings.gemini_api_key:
            return GeminiResult(
                ok=False,
                model_used=settings.gemini_model_primary,
                parsed_json=None,
                raw_text=None,
                error="GEMINI_API_KEY not configured",
                prompt_tokens=None,
                output_tokens=None,
                total_tokens=None,
            )

        temp = float(settings.gemini_temperature if temperature is None else temperature)
        mot = int(settings.gemini_max_output_tokens if max_output_tokens is None else max_output_tokens)
        to_s = int(settings.gemini_timeout_s if timeout_s is None else timeout_s)
        attempts_per_model = max(1, int(getattr(settings, "gemini_attempts_per_model", 2)))

        last_error_msg: str | None = None
        usage_acc = {"prompt_tokens": 0, "output_tokens": 0, "total_tokens": 0}

        for model in (settings.gemini_model_primary, settings.gemini_model_fallback):
            last_err: Exception | None = None
            for attempt in range(attempts_per_model):
                try:
                    text, usage = self._call_text(
                        model=model, prompt=prompt, temperature=temp, max_output_tokens=mot, timeout_s=to_s
                    )
                    # Always account tokens for cost analytics, even if JSON parsing fails and we retry.
                    for k in ("prompt_tokens", "output_tokens", "total_tokens"):
                        if usage.get(k) is not None:
                            usage_acc[k] += int(usage.get(k) or 0)
                    parsed = self._parse_json(text, model=model)
                    if expect == "dict" and not isinstance(parsed, dict):
                        raise GeminiError("Expected JSON object", model=model, reason="invalid_json", response_snippet=text[:500])
                    if expect == "list" and not isinstance(parsed, list):
                        raise GeminiError("Expected JSON array", model=model, reason="invalid_json", response_snippet=text[:500])
                    return GeminiResult(
                        ok=True,
                        model_used=model,
                        parsed_json=parsed,
                        raw_text=text,
                        error=None,
                        prompt_tokens=usage_acc.get("prompt_tokens"),
                        output_tokens=usage_acc.get("output_tokens"),
                        total_tokens=usage_acc.get("total_tokens"),
                    )
                except GeminiError as e:
                    last_err = e
                    if attempt < (attempts_per_model - 1):
                        time.sleep(1.0 * (2**attempt))
                    continue
                except (requests.Timeout, requests.ConnectionError) as e:
                    last_err = e
                    if attempt < (attempts_per_model - 1):
                        time.sleep(1.0 * (2**attempt))
                    continue
                except Exception as e:
                    # Non-classified failure: still retry once, then move to fallback.
                    last_err = e
                    if attempt < (attempts_per_model - 1):
                        time.sleep(1.0 * (2**attempt))
                    continue

            last_error_msg = f"{type(last_err).__name__}: {last_err}"
            # try next model (fallback)
            continue

        return GeminiResult(
            ok=False,
            model_used=settings.gemini_model_fallback,
            parsed_json=None,
            raw_text=None,
            error=last_error_msg or "Gemini failed",
            prompt_tokens=usage_acc.get("prompt_tokens") or None,
            output_tokens=usage_acc.get("output_tokens") or None,
            total_tokens=usage_acc.get("total_tokens") or None,
        )

    def _try_extract_json(self, text: str) -> str | None:
        """
        Robust JSON extraction for cases where Gemini wraps JSON with extra text/fences.
        We still require a single JSON object/array payload; we just recover it safely.
        """
        s = (text or "").strip()
        if not s:
            return None
        # Common case: ```json ... ```
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s*```$", "", s)

        # Prefer arrays for our batch prompts
        m = re.search(r"\[[\s\S]*\]", s)
        if m:
            return m.group(0).strip()
        m = re.search(r"\{[\s\S]*\}", s)
        if m:
            return m.group(0).strip()
        return None

    def _parse_json(self, text: str, *, model: str) -> Any:
        # Gemini sometimes returns whitespace around JSON; responseMimeType helps but we still guard.
        try:
            return json.loads((text or "").strip())
        except Exception as e:
            extracted = self._try_extract_json(text)
            if extracted:
                try:
                    return json.loads(extracted)
                except Exception:
                    pass
            raise GeminiError(
                "Invalid JSON returned", model=model, reason="invalid_json", response_snippet=(text or "")[:500]
            ) from e

    def _call_text(
        self,
        *,
        model: str,
        prompt: str,
        temperature: float,
        max_output_tokens: int,
        timeout_s: int,
    ) -> tuple[str, dict[str, int | None]]:
        url = settings.gemini_endpoint.format(model=model)
        params = {"key": settings.gemini_api_key}
        payload: dict[str, Any] = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_output_tokens,
                "responseMimeType": "application/json",
            },
        }

        resp = requests.post(url, params=params, json=payload, timeout=timeout_s)
        if resp.status_code in (429, 500, 502, 503, 504):
            raise GeminiError(
                "Retryable Gemini error",
                model=model,
                reason="http",
                http_status=resp.status_code,
                response_snippet=(resp.text or "")[:500],
            )
        if resp.status_code >= 400:
            # Common real-world failure: Google flags leaked keys and hard-denies all requests.
            # Surface this clearly so callers can stop spamming and users know to rotate keys.
            if resp.status_code == 403 and "reported as leaked" in (resp.text or "").lower():
                raise GeminiError(
                    "API key reported as leaked. Please use another API key.",
                    model=model,
                    reason="http",
                    http_status=resp.status_code,
                    response_snippet=(resp.text or "")[:500],
                )
            raise GeminiError(
                "Non-retryable Gemini error",
                model=model,
                reason="http",
                http_status=resp.status_code,
                response_snippet=(resp.text or "")[:500],
            )

        data = resp.json()
        usage_md = data.get("usageMetadata") or {}
        usage = {
            "prompt_tokens": int(usage_md.get("promptTokenCount")) if usage_md.get("promptTokenCount") is not None else None,
            "output_tokens": int(usage_md.get("candidatesTokenCount")) if usage_md.get("candidatesTokenCount") is not None else None,
            "total_tokens": int(usage_md.get("totalTokenCount")) if usage_md.get("totalTokenCount") is not None else None,
        }
        out = ""
        for cand in data.get("candidates", []):
            for part in cand.get("content", {}).get("parts", []):
                out += part.get("text", "") + "\n"
        return out.strip(), usage


