from __future__ import annotations

import csv
import datetime as dt
import json
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from config import settings
from models import GrievanceRaw, GrievanceStructured
from services.ai_service import AIService


def _ensure_dirs() -> None:
    Path(settings.data_raw_dir).mkdir(parents=True, exist_ok=True)
    Path(settings.data_processed_dir).mkdir(parents=True, exist_ok=True)


def _parse_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    # Common client exports include time (e.g., "22-12-2025 09:52 AM"). Support both date and datetime.
    # Try full datetime formats first, then fall back to date-only formats, then "first token" parsing.
    for fmt in (
        "%d-%m-%Y %I:%M %p",
        "%d/%m/%Y %I:%M %p",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y-%m-%d",
        "%Y/%m/%d",
    ):
        try:
            return dt.datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    # If a time component exists but doesn't match known formats, take first token as date.
    token = value.split("T", 1)[0].split(" ", 1)[0].strip()
    if token and token != value:
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                return dt.datetime.strptime(token, fmt).date()
            except ValueError:
                continue
    return None


def _parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _normalize_headers(headers: list[str]) -> dict[str, str]:
    # maps normalized -> original
    out: dict[str, str] = {}
    for h in headers:
        out[h.strip().lower().replace(" ", "_")] = h
    return out


def _pick(norm_map: dict[str, str], *candidates: str) -> str | None:
    for c in candidates:
        if c in norm_map:
            return norm_map[c]
    return None


@dataclass(frozen=True)
class UploadResult:
    stored_raw_path: str
    inserted: int
    skipped_duplicates: int


class DataService:
    def __init__(self) -> None:
        _ensure_dirs()

    def store_uploaded_csv(self, tmp_path: str, original_filename: str) -> str:
        _ensure_dirs()
        stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        safe_name = "".join(ch for ch in original_filename if ch.isalnum() or ch in ("-", "_", ".", " ")).strip()
        if not safe_name:
            safe_name = "grievances.csv"
        dest = Path(settings.data_raw_dir) / f"{stamp}_{uuid.uuid4().hex[:8]}_{safe_name}"
        shutil.copyfile(tmp_path, dest)
        return str(dest)

    def ingest_csv_into_db(self, db: Session, csv_path: str) -> UploadResult:
        _ensure_dirs()
        inserted = 0
        skipped = 0

        with open(csv_path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("CSV has no headers")
            norm_map = _normalize_headers(list(reader.fieldnames))

            col_gid = _pick(norm_map, "grievance_id", "complaint_id", "id", "ticket_id")
            # Client exports often use "Subject" as the complaint narrative; accept that as grievance_text.
            col_text = _pick(norm_map, "grievance_text", "complaint_text", "description", "details", "text", "subject", "complaint_subject")
            # Client exports often use "Date" with time.
            col_created = _pick(
                norm_map,
                "created_date",
                "lodged_date",
                "registered_date",
                "date_lodged",
                "date",
                "created_on",
                "created_datetime",
                "created_at",
            )
            col_closed = _pick(
                norm_map,
                "closed_date",
                "resolved_date",
                "date_closed",
                "closed_on",
                "closed_datetime",
                "closed_at",
            )
            col_ward = _pick(norm_map, "ward", "ward_name", "ward_no", "ward_number")
            col_dept = _pick(norm_map, "department", "dept", "service", "category_department")
            col_rating = _pick(norm_map, "feedback_star", "star_rating", "citizen_feedback_rating", "rating", "feedback_rating")

            if not col_gid or not col_text:
                raise ValueError(
                    "CSV must include at least grievance_id and grievance_text columns "
                    "(case-insensitive)."
                )

            for row in reader:
                gid = (row.get(col_gid) or "").strip()
                text = (row.get(col_text) or "").strip()
                if not gid or not text:
                    continue

                try:
                    with db.begin_nested():
                        raw = GrievanceRaw(
                            grievance_id=gid,
                            created_date=_parse_date(row.get(col_created)) if col_created else None,
                            closed_date=_parse_date(row.get(col_closed)) if col_closed else None,
                            ward=(row.get(col_ward) or "").strip() or None if col_ward else None,
                            department=(row.get(col_dept) or "").strip() or None if col_dept else None,
                            feedback_star=_parse_float(row.get(col_rating)) if col_rating else None,
                            grievance_text=text,
                            raw_payload_json=json.dumps(row, ensure_ascii=False),
                        )
                        db.add(raw)
                        db.flush()
                        inserted += 1
                except IntegrityError:
                    skipped += 1
                    continue

        return UploadResult(stored_raw_path=csv_path, inserted=inserted, skipped_duplicates=skipped)

    def has_any_data(self, db: Session) -> bool:
        return db.scalar(select(GrievanceRaw.id).limit(1)) is not None

    def process_pending_structuring(
        self,
        db: Session,
        *,
        batch_size: int = 8,
        max_batches: int = 999,
        reprocess_mock: bool = False,
        reprocess_unknown: bool = False,
    ) -> dict:
        """
        Process unstructured grievances in small Gemini batches (FREE tier friendly).
        - batch_size: 5–10 recommended
        - never crashes pipeline; continues on AI failures
        """
        bs = max(5, min(int(batch_size), 10))
        ai = AIService()
        processed = 0
        batches = 0

        while batches < max_batches:
            if reprocess_mock or reprocess_unknown:
                # Re-run previously structured records that were produced via fallback (is_mock)
                # and/or have an "Unknown" category.
                from sqlalchemy import or_

                conds = []
                if reprocess_mock:
                    conds.append(GrievanceStructured.is_mock.is_(True))
                if reprocess_unknown:
                    conds.append(GrievanceStructured.category.ilike("unknown"))
                raws = (
                    db.execute(
                        select(GrievanceRaw)
                        .join(GrievanceStructured, GrievanceStructured.raw_id == GrievanceRaw.id)
                        .where(or_(*conds))
                        .order_by(GrievanceRaw.id.asc())
                        .limit(bs)
                    )
                    .scalars()
                    .all()
                )
            else:
                raws = (
                    db.execute(
                        select(GrievanceRaw)
                        .outerjoin(GrievanceStructured, GrievanceStructured.raw_id == GrievanceRaw.id)
                        .where(GrievanceStructured.id.is_(None))
                        .order_by(GrievanceRaw.id.asc())
                        .limit(bs)
                    )
                    .scalars()
                    .all()
                )
            if not raws:
                break
            batches += 1

            for raw in raws:
                record = {
                    "grievance_id": raw.grievance_id,
                    "grievance_text": raw.grievance_text,
                    "ward": raw.ward,
                    "department": raw.department,
                    "created_date": raw.created_date.isoformat() if raw.created_date else None,
                    "closed_date": raw.closed_date.isoformat() if raw.closed_date else None,
                    "feedback_star": raw.feedback_star,
                }
                out = ai.structure_grievance(record)
                # Idempotency guard: another worker/request may have already structured this raw_id.
                # Commit per-row to keep SQLite happy and avoid aborting the whole batch on a single duplicate.
                try:
                    with db.begin_nested():
                        existing = db.execute(
                            select(GrievanceStructured).where(GrievanceStructured.raw_id == raw.id).limit(1)
                        ).scalar_one_or_none()
                        if existing:
                            existing.category = out.category
                            existing.sub_issue = out.sub_issue
                            existing.sentiment = out.sentiment
                            existing.severity = out.severity
                            existing.repeat_flag = out.repeat_flag
                            existing.delay_risk = out.delay_risk
                            existing.dissatisfaction_reason = out.dissatisfaction_reason
                            existing.ai_rationale = "Gemini JSON structuring (or fallback Unknown)."
                            existing.ai_provider = out.ai_provider
                            existing.ai_engine = out.ai_engine
                            existing.ai_model = out.ai_model
                            existing.ai_version = "v3"
                            existing.is_mock = not out.raw_ok
                        else:
                            s = GrievanceStructured(
                                raw_id=raw.id,
                                category=out.category,
                                sub_issue=out.sub_issue,
                                sentiment=out.sentiment,
                                severity=out.severity,
                                repeat_flag=out.repeat_flag,
                                delay_risk=out.delay_risk,
                                dissatisfaction_reason=out.dissatisfaction_reason,
                                ai_rationale="Gemini JSON structuring (or fallback Unknown).",
                                ai_provider=out.ai_provider,
                                ai_engine=out.ai_engine,
                                ai_model=out.ai_model,
                                ai_version="v3",
                                is_mock=not out.raw_ok,
                            )
                            db.add(s)
                        db.flush()
                    db.commit()
                    processed += 1
                except IntegrityError:
                    db.rollback()
                    continue

        remaining = (
            db.execute(
                select(GrievanceRaw.id)
                .outerjoin(GrievanceStructured, GrievanceStructured.raw_id == GrievanceRaw.id)
                .where(GrievanceStructured.id.is_(None))
                .limit(1)
            ).first()
            is not None
        )
        remaining_mock = (
            db.execute(
                select(GrievanceStructured.id).where(GrievanceStructured.is_mock.is_(True)).limit(1)
            ).first()
            is not None
        )
        return {
            "processed": processed,
            "batches": batches,
            "remaining": bool(remaining),
            "remaining_mock": bool(remaining_mock),
            "batch_size": bs,
            "mode": (
                "reprocess_mock"
                if reprocess_mock and not reprocess_unknown
                else "reprocess_unknown"
                if reprocess_unknown and not reprocess_mock
                else "reprocess_mixed"
                if (reprocess_mock and reprocess_unknown)
                else "process_new"
            ),
        }


