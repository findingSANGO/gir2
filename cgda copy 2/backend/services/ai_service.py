from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from config import settings
from services.gemini_client import GeminiClient

AI_VERSION = "v4-configurable"


def _prompt_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "prompts"


def _read_prompt(name: str) -> str:
    return (_prompt_dir() / name).read_text(encoding="utf-8")


def _title(v: str) -> str:
    v = (v or "").strip()
    if not v:
        return "Unknown"
    return v[:1].upper() + v[1:]


def _normalize_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "y")
    return False


@dataclass(frozen=True)
class AIOutput:
    category: str
    sub_issue: str
    sentiment: str
    severity: str
    repeat_flag: bool
    delay_risk: str
    dissatisfaction_reason: str
    ai_provider: str
    ai_engine: str
    ai_model: str
    raw_ok: bool


class AIService:
    """
    FREE tier Gemini only:
    - default: gemini-2.0-flash
    - fallback: gemini-2.0-flash-lite
    - strict JSON-only output
    - batch-safe (pipeline never crashes; fill Unknown + continue)
    """

    def structure_grievance(self, record: dict[str, Any]) -> AIOutput:
        if not settings.gemini_api_key:
            return self._fallback_unknown(settings.gemini_model_primary)

        prompt_tpl = _read_prompt("grievance_structuring.txt")
        prompt = prompt_tpl.replace("{{INPUT_JSON}}", json.dumps(record, ensure_ascii=False))
        res = GeminiClient().generate_json(prompt=prompt, temperature=min(0.2, settings.gemini_temperature), expect="dict")
        if not res.ok or not isinstance(res.parsed_json, dict):
            print(f"[AI] Gemini structuring failed: {res.error}")
            return self._fallback_unknown(res.model_used)
        return self._validate_and_fill(res.parsed_json, model=res.model_used)

    def commissioner_summary(self, analytics: dict[str, Any]) -> dict[str, Any]:
        # Summary uses the same configured models; never crashes.
        if not settings.gemini_api_key:
            return {
                "summary_bullets": [
                    "Gemini key not configured; using deterministic analytics insights only.",
                ],
                "ai_provider": "caseA",
                "ai_engine": "Gemini",
                "ai_model": settings.gemini_model_primary,
            }
        prompt_tpl = _read_prompt("commissioner_summary.txt")
        prompt = prompt_tpl.replace("{{INPUT_JSON}}", json.dumps(analytics, ensure_ascii=False))
        res = GeminiClient().generate_json(prompt=prompt, temperature=min(0.2, settings.gemini_temperature), expect="dict")
        if res.ok and isinstance(res.parsed_json, dict):
            out = res.parsed_json
            out["ai_provider"] = "caseA"
            out["ai_engine"] = "Gemini"
            out["ai_model"] = res.model_used
            return out
        return {
            "summary_bullets": ["Commissioner summary unavailable due to Gemini error; continue with charts."],
            "ai_provider": "caseA",
            "ai_engine": "Gemini",
            "ai_model": res.model_used,
        }

    def _validate_and_fill(self, parsed: dict[str, Any], *, model: str) -> AIOutput:
        # Strict schema keys; fill missing with "Unknown"
        category = _title(str(parsed.get("category", "") or "Unknown"))
        sub_issue = _title(str(parsed.get("sub_issue", "") or "Unknown"))
        sentiment = _title(str(parsed.get("sentiment", "") or "Unknown"))
        severity = _title(str(parsed.get("severity", "") or "Unknown"))
        delay_risk = _title(str(parsed.get("delay_risk", "") or "Unknown"))
        dissatisfaction_reason = _title(str(parsed.get("dissatisfaction_reason", "") or "Unknown"))
        repeat_flag = _normalize_bool(parsed.get("repeat_flag"))

        return AIOutput(
            category=category[:128],
            sub_issue=sub_issue[:256],
            sentiment=sentiment[:32],
            severity=severity[:32],
            repeat_flag=repeat_flag,
            delay_risk=delay_risk[:32],
            dissatisfaction_reason=dissatisfaction_reason[:240],
            ai_provider="caseA",
            ai_engine="Gemini",
            ai_model=model,
            raw_ok=True,
        )

    def _fallback_unknown(self, model: str) -> AIOutput:
        return AIOutput(
            category="Unknown",
            sub_issue="Unknown",
            sentiment="Unknown",
            severity="Unknown",
            repeat_flag=False,
            delay_risk="Unknown",
            dissatisfaction_reason="Unknown",
            ai_provider="caseA",
            ai_engine="Gemini",
            ai_model=model,
            raw_ok=False,
        )

    # All Gemini calls go through services/gemini_client.py (single wrapper).


