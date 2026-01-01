import os
import tempfile
import threading
from typing import Annotated

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from auth import User, require_role
from database import get_db, session_scope
from models import GrievanceRaw, GrievanceStructured
from services.data_service import DataService

router = APIRouter(prefix="/api/grievances", tags=["grievances"])


class UploadResponse(BaseModel):
    stored_raw_path: str
    inserted: int
    skipped_duplicates: int
    processed: int
    batches: int
    batch_size: int
    remaining: bool
    background_processing_started: bool


@router.post("/upload_csv", response_model=UploadResponse)
def upload_csv(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    file: UploadFile = File(...),
) -> UploadResponse:
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    data_svc = DataService()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp_path = tmp.name
        content = file.file.read()
        tmp.write(content)

    try:
        stored_path = data_svc.store_uploaded_csv(tmp_path, file.filename)
        result = data_svc.ingest_csv_into_db(db, stored_path)
        # Free-tier + UX: do not block this request on Gemini calls.
        # Kick off a background pass and return immediately.
        def _bg() -> None:
            try:
                with session_scope() as db2:
                    svc = DataService()
                    svc.process_pending_structuring(db2, batch_size=8, max_batches=10)
            except Exception:
                return

        threading.Thread(target=_bg, name="cgda-upload-structuring", daemon=True).start()

        # Compute whether any unstructured items remain (fast query).
        remaining = (
            db.execute(
                select(GrievanceRaw.id)
                .outerjoin(GrievanceStructured, GrievanceStructured.raw_id == GrievanceRaw.id)
                .where(GrievanceStructured.id.is_(None))
                .limit(1)
            ).first()
            is not None
        )
        return UploadResponse(
            stored_raw_path=result.stored_raw_path,
            inserted=result.inserted,
            skipped_duplicates=result.skipped_duplicates,
            processed=0,
            batches=0,
            batch_size=8,
            remaining=bool(remaining),
            background_processing_started=True,
        )
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


@router.post("/process_pending")
def process_pending(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    batch_size: int = 8,
    max_batches: int = 2,
    reprocess_mock: bool = False,
    reprocess_unknown: bool = False,
):
    """
    On-demand AI structuring (free-tier friendly).
    Used by the UI to bootstrap structured data without blocking server startup.
    """
    # Do not block the request on Gemini calls; run in background and return immediately.
    bs = max(5, min(int(batch_size), 10))
    mb = max(1, min(int(max_batches), 50))

    def _bg() -> None:
        try:
            with session_scope() as db2:
                svc = DataService()
                svc.process_pending_structuring(
                    db2,
                    batch_size=bs,
                    max_batches=mb,
                    reprocess_mock=reprocess_mock,
                    reprocess_unknown=reprocess_unknown,
                )
        except Exception:
            return

    threading.Thread(target=_bg, name="cgda-process-pending", daemon=True).start()
    return {
        "started": True,
        "batch_size": bs,
        "max_batches": mb,
        "reprocess_mock": bool(reprocess_mock),
        "reprocess_unknown": bool(reprocess_unknown),
    }


@router.get("/export_structured_csv")
def export_structured_csv(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
):
    import csv
    import io

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(
        [
            "grievance_id",
            "ward",
            "department",
            "created_date",
            "closed_date",
            "feedback_star",
            "grievance_text",
            "category",
            "sub_issue",
            "sentiment",
            "severity",
            "repeat_flag",
            "delay_risk",
            "dissatisfaction_reason",
            "ai_provider",
            "ai_engine",
            "ai_model",
            "processed_at",
        ]
    )
    rows = db.execute(
        select(GrievanceRaw, GrievanceStructured).join(GrievanceStructured, GrievanceStructured.raw_id == GrievanceRaw.id)
    ).all()
    for raw, s in rows:
        w.writerow(
            [
                raw.grievance_id,
                raw.ward,
                raw.department,
                raw.created_date.isoformat() if raw.created_date else "",
                raw.closed_date.isoformat() if raw.closed_date else "",
                raw.feedback_star if raw.feedback_star is not None else "",
                raw.grievance_text,
                s.category,
                s.sub_issue,
                s.sentiment,
                s.severity,
                "true" if s.repeat_flag else "false",
                s.delay_risk,
                s.dissatisfaction_reason or "",
                s.ai_provider,
                s.ai_engine,
                s.ai_model,
                s.processed_at.isoformat() if s.processed_at else "",
            ]
        )

    return Response(
        content=out.getvalue().encode("utf-8"),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=cgda_structured_export.csv"},
    )


@router.head("/export_structured_csv")
def export_structured_csv_head(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
):
    # Allow HEAD requests (e.g., health checks) for the export endpoint.
    return Response(content=b"", media_type="text/csv")


