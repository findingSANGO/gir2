from __future__ import annotations

import os
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from auth import User, require_role
from config import settings
from database import get_db
from services.enrichment_service import EnrichmentService
from services.processed_data_service import ProcessedDataService

router = APIRouter(prefix="/api/data", tags=["data"])


@router.get("/latest")
def latest_raw(_: Annotated[User, Depends(require_role("admin", "commissioner"))]):
    svc = EnrichmentService()
    latest = svc.detect_latest_raw_file()
    return {"latest": latest.__dict__ if latest else None, "raw_dir": settings.data_raw_dir}


@router.post("/ingest")
def ingest(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    limit_rows: int | None = 100,
    raw_filename: str | None = None,
    reset_analytics: bool = True,
    force_reprocess: bool = False,
):
    """
    Trigger ingestion + enrichment for the latest file under data/raw (csv/xlsx).
    Runs in background; returns run_id immediately.
    """
    try:
        svc = EnrichmentService()
        run_id = svc.start_run_background(
            db,
            row_limit=limit_rows,
            raw_filename=raw_filename,
            reset_analytics=reset_analytics,
            force_reprocess=force_reprocess,
        )
        return {"run_id": run_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/preprocess")
def preprocess(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    raw_filename: str | None = None,
):
    """
    Normalize latest raw Excel/CSV into DB table grievances_processed.
    Idempotent upsert (no duplicates). Raw input remains immutable.
    """
    try:
        svc = ProcessedDataService()
        return svc.preprocess_latest(db, raw_filename=raw_filename)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/preprocess/status")
def preprocess_status(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
):
    svc = ProcessedDataService()
    s = svc.latest_status(db)
    return {"status": s.__dict__ if s else None}


@router.get("/runs")
def list_runs(_: Annotated[User, Depends(require_role("admin", "commissioner"))], db: Session = Depends(get_db)):
    svc = EnrichmentService()
    runs = svc.list_runs(db, limit=30)
    return {
        "runs": [
            {
                "run_id": r.run_id,
                "status": r.status,
                "raw_filename": r.raw_filename,
                "started_at": r.started_at.isoformat(),
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                "total_rows": r.total_rows,
                "processed": r.processed,
                "skipped": r.skipped,
                "failed": r.failed,
                "error": r.error,
                "summary": r.summary_json,
            }
            for r in runs
        ]
    }


@router.get("/runs/{run_id}")
def run_status(
    run_id: str,
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
):
    svc = EnrichmentService()
    r = svc.get_run(db, run_id)
    return {
        "run_id": r.run_id,
        "status": r.status,
        "raw_filename": r.raw_filename,
        "raw_path": r.raw_path,
        "started_at": r.started_at.isoformat(),
        "finished_at": r.finished_at.isoformat() if r.finished_at else None,
        "total_rows": r.total_rows,
        "processed": r.processed,
        "skipped": r.skipped,
        "failed": r.failed,
        "error": r.error,
        "summary": r.summary_json,
    }


@router.get("/enriched/download")
def download_enriched(_: Annotated[User, Depends(require_role("admin", "commissioner"))]):
    path = os.path.join(settings.data_processed_dir, "grievances_enriched.csv")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="No enriched CSV found. Run ingestion first.")
    print(f"[PIPELINE] UI download reads ENRICHED file from: {path}")
    return FileResponse(path, media_type="text/csv", filename="grievances_enriched.csv")


@router.get("/preprocessed/download")
def download_preprocessed(_: Annotated[User, Depends(require_role("admin", "commissioner"))]):
    path = os.path.join(settings.data_processed_dir, "input_dataset_latest.csv")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="No preprocessed file found. Run ingestion first.")
    print(f"[PIPELINE] UI download reads INPUT DATASET file from: {path}")
    return FileResponse(path, media_type="text/csv", filename="input_dataset_latest.csv")


@router.get("/results")
def results(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    limit: int = 50,
    offset: int = 0,
):
    """
    Main results view source: the enriched CSV (derived strictly from preprocessed).
    Returns per-record AI_SubTopic (subcategory) and related fields for UI tables.
    """
    import pandas as pd

    path = os.path.join(settings.data_processed_dir, "grievances_enriched.csv")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="No enriched CSV found. Run enrichment first.")
    print(f"[PIPELINE] UI results read ENRICHED file from: {path}")

    df = pd.read_csv(path)
    total_rows = int(len(df))
    if offset < 0:
        offset = 0
    limit = max(1, min(int(limit or 50), 500))
    sub = df.iloc[offset : offset + limit].fillna("")

    rows = []
    for _, r in sub.iterrows():
        rows.append(
            {
                "grievance_id": str(r.get("Grievance Id", "")).strip(),
                "created_date": str(r.get("Created_Date_ISO", r.get("Created Date", ""))).strip(),
                "ward": str(r.get("Ward Name", "")).strip(),
                "department": str(r.get("Current Department Name", "")).strip(),
                "subject": str(r.get("Complaint Subject", "")).strip(),
                "subcategory": str(r.get("AI_SubTopic", "")).strip(),
                "category": str(r.get("AI_Category", "")).strip(),
                "confidence": str(r.get("AI_Confidence", "")).strip(),
                "error": str(r.get("AI_Error", "")).strip(),
            }
        )

    return {
        "source": "data/processed/grievances_enriched.csv",
        "offset": offset,
        "limit": limit,
        "total_rows": total_rows,
        "rows": rows,
    }


