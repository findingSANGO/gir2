from __future__ import annotations

import datetime as dt
import os
import re
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from config import settings
from models import EnrichmentCheckpoint, GrievanceProcessed, PreprocessRun
from services.enrichment_service import EnrichmentService


IST = ZoneInfo("Asia/Kolkata")


def _strip_cell_newlines(v):
    if isinstance(v, str):
        return re.sub(r"[\r\n]+", " ", v).strip()
    return v


@dataclass(frozen=True)
class PreprocessStatus:
    raw_filename: str
    raw_path: str
    raw_mtime_iso: str
    processed_at: str
    record_count: int
    status: str
    error: str | None


class ProcessedDataService:
    """
    Raw (immutable) Excel/CSV -> normalized DB table grievances_processed for fast filtering queries.
    IMPORTANT: No Gemini calls here. AI fields are read from stored checkpoints.
    """

    def __init__(self) -> None:
        # Reuse the robust reader + column mapping logic already built for the NMMC exports.
        self._raw = EnrichmentService()

    def preprocess_latest(self, db: Session, *, raw_filename: str | None = None) -> dict:
        latest = self._raw.get_raw_file_by_name(raw_filename) if raw_filename else self._raw.detect_latest_raw_file()
        if not latest:
            raise ValueError(f"No raw file found under {settings.data_raw_dir}")

        raw_mtime_iso = latest.mtime_iso
        run = PreprocessRun(raw_filename=latest.filename, raw_path=latest.path, raw_mtime_iso=raw_mtime_iso, status="running")
        db.add(run)
        db.commit()

        try:
            count = self._preprocess_file_into_db(db, raw_path=latest.path)
            run.status = "completed"
            run.record_count = int(count)
            run.error = None
            run.processed_at = dt.datetime.utcnow()
            db.commit()
            return {"status": "completed", "raw_filename": latest.filename, "record_count": int(count)}
        except Exception as e:
            run.status = "failed"
            run.error = f"{type(e).__name__}: {e}"
            run.processed_at = dt.datetime.utcnow()
            db.commit()
            raise

    def latest_status(self, db: Session) -> PreprocessStatus | None:
        r = db.execute(select(PreprocessRun).order_by(PreprocessRun.processed_at.desc()).limit(1)).scalar_one_or_none()
        if not r:
            return None
        return PreprocessStatus(
            raw_filename=r.raw_filename,
            raw_path=r.raw_path,
            raw_mtime_iso=r.raw_mtime_iso,
            processed_at=r.processed_at.isoformat(),
            record_count=int(r.record_count),
            status=r.status,
            error=r.error,
        )

    def _preprocess_file_into_db(self, db: Session, *, raw_path: str) -> int:
        df = self._raw.load_raw_dataframe(raw_path)
        mapping = self._raw._map_columns(df)  # noqa: SLF001 (internal reuse; keeps behavior consistent)

        # Strip embedded newlines so 1 record == 1 row consistently.
        df = df.applymap(_strip_cell_newlines)

        grievance_id = df[mapping["Grievance Id"]].astype(str).str.strip()
        created_raw = df[mapping["Created Date"]]

        created = pd.to_datetime(created_raw, errors="coerce", dayfirst=True)
        # Localize to IST if timezone-naive; then derive created_date/month/week in IST.
        if getattr(created.dt, "tz", None) is None:
            created_ist = created.dt.tz_localize(IST, ambiguous="NaT", nonexistent="shift_forward")
        else:
            created_ist = created.dt.tz_convert(IST)

        created_utc = created_ist.dt.tz_convert("UTC").dt.tz_localize(None)
        created_date = created_ist.dt.date
        created_month = created_ist.dt.strftime("%Y-%m")
        iso = created_ist.dt.isocalendar()
        created_week = (iso["year"].astype(str) + "-W" + iso["week"].astype(int).astype(str).str.zfill(2)).astype(str)

        ward = df[mapping["Ward Name"]].astype(str).str.strip()
        dept = df[mapping["Current Department Name"]].astype(str).str.strip()
        status = df[mapping["Current Status"]].astype(str).str.strip()
        subject = df[mapping["Complaint Subject"]].astype(str)
        desc = df[mapping["Complaint Description"]].astype(str)
        closing = df[mapping["Closing Remark"]].astype(str)

        # Pull AI fields from stored checkpoints (no Gemini).
        cps = db.execute(select(EnrichmentCheckpoint)).scalars().all()
        cp_map = {c.grievance_key: c for c in cps}

        rows = []
        for i in range(len(df)):
            gid = str(grievance_id.iloc[i]).strip()
            if not gid or gid.lower() == "nan":
                continue
            cp = cp_map.get(gid)
            if cp and cp.ai_error:
                cp = None

            rows.append(
                {
                    "grievance_id": gid,
                    "created_at": created_utc.iloc[i].to_pydatetime() if pd.notna(created_utc.iloc[i]) else None,
                    "created_date": created_date.iloc[i] if pd.notna(created_date.iloc[i]) else None,
                    "created_month": str(created_month.iloc[i]) if pd.notna(created_month.iloc[i]) else None,
                    "created_week": str(created_week.iloc[i]) if pd.notna(created_week.iloc[i]) else None,
                    "ward_name": (str(ward.iloc[i]).strip() or None),
                    "department_name": (str(dept.iloc[i]).strip() or None),
                    "status": (str(status.iloc[i]).strip() or None),
                    "subject": (str(subject.iloc[i]).strip() or None),
                    "description": (str(desc.iloc[i]).strip() or None),
                    "closing_remark": (str(closing.iloc[i]).strip() or None),
                    "ai_category": (cp.ai_category if cp else None),
                    "ai_subtopic": (cp.ai_subtopic if cp else None),
                    "ai_confidence": (cp.ai_confidence if cp else None),
                }
            )

        if not rows:
            return 0

        # Idempotent upsert on grievance_id (SQLite).
        # This allows re-run without duplication and updates AI fields if checkpoints improved later.
        for start in range(0, len(rows), 1000):
            chunk = rows[start : start + 1000]
            stmt = sqlite_insert(GrievanceProcessed).values(chunk)
            update_cols = {c.name: getattr(stmt.excluded, c.name) for c in GrievanceProcessed.__table__.columns if c.name != "grievance_id"}
            stmt = stmt.on_conflict_do_update(index_elements=["grievance_id"], set_=update_cols)
            db.execute(stmt)
            db.commit()

        return len(rows)


