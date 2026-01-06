from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from auth import User, require_role
from config import settings
from database import get_db
from models import ReportUpload


router = APIRouter(prefix="/api/reports", tags=["reports_manual"])


def _parse_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def _reports_dir() -> Path:
    base = Path(settings.data_processed_dir) / "reports"
    base.mkdir(parents=True, exist_ok=True)
    return base


@router.post("/upload")
def upload_report(
    user: Annotated[User, Depends(require_role("commissioner", "it_head"))],
    db: Session = Depends(get_db),
    file: UploadFile = File(...),
    period_type: str = Form(...),  # weekly/monthly/quarterly/annual
    period_start: str = Form(...),  # YYYY-MM-DD
    period_end: str = Form(...),  # YYYY-MM-DD
    notes: str | None = Form(None),
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")

    pt = (period_type or "").strip().lower()
    if pt not in ("weekly", "monthly", "quarterly", "annual"):
        raise HTTPException(status_code=400, detail="period_type must be weekly/monthly/quarterly/annual")

    ps = _parse_date(period_start)
    pe = _parse_date(period_end)
    if pe < ps:
        raise HTTPException(status_code=400, detail="period_end must be >= period_start")

    # Store file on disk (manual upload, no GenAI)
    safe_name = f"{pt}_{ps.isoformat()}_{pe.isoformat()}_{dt.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.pdf"
    out_dir = _reports_dir() / pt
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / safe_name

    with out_path.open("wb") as f:
        f.write(file.file.read())

    rec = ReportUpload(
        period_type=pt,
        period_start=ps,
        period_end=pe,
        uploaded_by=user.username,
        file_path=str(out_path),
        notes=notes,
    )
    db.add(rec)
    db.commit()
    db.refresh(rec)

    return {
        "id": rec.id,
        "period_type": rec.period_type,
        "period_start": rec.period_start.isoformat(),
        "period_end": rec.period_end.isoformat(),
        "uploaded_at": rec.uploaded_at.isoformat(),
        "uploaded_by": rec.uploaded_by,
        "notes": rec.notes,
    }


@router.get("/latest")
def latest_report(
    _: Annotated[User, Depends(require_role("commissioner", "it_head"))],
    db: Session = Depends(get_db),
    period_type: str = "weekly",
):
    pt = (period_type or "").strip().lower()
    row = (
        db.execute(
            select(ReportUpload)
            .where(ReportUpload.period_type == pt)
            .order_by(ReportUpload.uploaded_at.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )
    if not row:
        return {"latest": None}
    return {
        "latest": {
            "id": row.id,
            "period_type": row.period_type,
            "period_start": row.period_start.isoformat(),
            "period_end": row.period_end.isoformat(),
            "uploaded_at": row.uploaded_at.isoformat(),
            "uploaded_by": row.uploaded_by,
            "notes": row.notes,
        }
    }


@router.get("")
def list_reports(
    _: Annotated[User, Depends(require_role("commissioner", "it_head"))],
    db: Session = Depends(get_db),
    period_type: str = "monthly",
    limit: int = 30,
):
    pt = (period_type or "").strip().lower()
    limit = max(1, min(int(limit or 30), 200))
    rows = (
        db.execute(
            select(ReportUpload)
            .where(ReportUpload.period_type == pt)
            .order_by(ReportUpload.uploaded_at.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return {
        "rows": [
            {
                "id": r.id,
                "period_type": r.period_type,
                "period_start": r.period_start.isoformat(),
                "period_end": r.period_end.isoformat(),
                "uploaded_at": r.uploaded_at.isoformat(),
                "uploaded_by": r.uploaded_by,
                "notes": r.notes,
            }
            for r in rows
        ]
    }


@router.get("/download/{report_id}")
def download_report(
    _: Annotated[User, Depends(require_role("commissioner", "it_head"))],
    db: Session = Depends(get_db),
    report_id: int = 0,
):
    row = db.execute(select(ReportUpload).where(ReportUpload.id == int(report_id))).scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="Report not found")
    if not row.file_path or not os.path.exists(row.file_path):
        raise HTTPException(status_code=404, detail="File missing on disk")
    return FileResponse(row.file_path, media_type="application/pdf", filename=Path(row.file_path).name)


