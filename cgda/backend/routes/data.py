from __future__ import annotations

import os
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy import delete, select

from auth import User, require_role
from config import settings
from database import get_db
from services.enrichment_service import EnrichmentService
from services.processed_data_service import ProcessedDataService

router = APIRouter(prefix="/api/data", tags=["data"])


@router.get("/latest")
def latest_raw(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    raw_dir: str | None = None,
):
    svc = EnrichmentService()
    latest = svc.detect_latest_raw_file(raw_dir=raw_dir)
    base = settings.data_raw_dir if (raw_dir or "raw") in ("raw", "data/raw", "", None) else getattr(settings, "data_raw2_dir", "../data/raw2")
    return {"latest": latest.__dict__ if latest else None, "raw_dir": base}


@router.get("/files")
def list_raw_files(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    raw_dir: str | None = None,
):
    svc = EnrichmentService()
    files = svc.list_raw_files(raw_dir=raw_dir)
    return {"files": [f.__dict__ for f in files]}


@router.post("/ingest")
def ingest(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    limit_rows: int | None = 100,
    raw_filename: str | None = None,
    raw_dir: str | None = None,
    reset_analytics: bool = True,
    force_reprocess: bool = False,
    extra_features: bool = False,
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
            raw_dir=raw_dir,
            reset_analytics=reset_analytics,
            force_reprocess=force_reprocess,
            extra_features=extra_features,
        )
        return {"run_id": run_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/enrich_tickets")
def enrich_tickets(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    source: str = "",
    limit_rows: int | None = None,
    force_reprocess: bool = False,
):
    """
    Run Gemini record-level enrichment on grievances_processed for a selected dataset source.
    Resumable via ticket_enrichment_checkpoints (keyed by grievance_code + ai_input_hash).
    """
    if not source:
        raise HTTPException(status_code=400, detail="source is required")
    try:
        svc = EnrichmentService()
        run_id = svc.start_ticket_enrichment_background(
            db,
            source=source,
            limit_rows=limit_rows,
            force_reprocess=force_reprocess,
        )
        return {"run_id": run_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/run1_expand")
def run1_expand(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    base_source: str = "",
    sample_size: int = 1100,
    force_reprocess: bool = False,
):
    """
    Expand the demo dataset to N tickets and enrich only missing ones.
    - Builds a dataset source: {base_source}__run1_{N}
    - Starts ticket enrichment for N rows; checkpointing skips previously enriched tickets
    """
    if not base_source:
        raise HTTPException(status_code=400, detail="base_source is required")
    try:
        psvc = ProcessedDataService()
        sample_source = psvc.build_run_sample(db, source=base_source, sample_size=int(sample_size))
        svc = EnrichmentService()
        run_id = svc.start_ticket_enrichment_background(
            db,
            source=sample_source,
            limit_rows=int(sample_size),
            force_reprocess=bool(force_reprocess),
        )
        return {"sample_source": sample_source, "run_id": run_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/preprocess")
def preprocess(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    raw_filename: str | None = None,
    raw_dir: str | None = None,
    limit_rows: int | None = None,
    dedupe_mode: str = "ticket",
    output_source: str | None = None,
):
    """
    Normalize latest raw Excel/CSV into DB table grievances_processed.
    Idempotent upsert (no duplicates). Raw input remains immutable.
    """
    try:
        svc = ProcessedDataService()
        return svc.preprocess_latest(
            db,
            raw_filename=raw_filename,
            raw_dir=raw_dir,
            limit_rows=limit_rows,
            dedupe_mode=dedupe_mode,
            output_source=output_source,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/reset_ai_for_source")
def reset_ai_for_source(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    source: str = "",
):
    """
    Delete Gemini ticket-enrichment checkpoints for the grievance_codes present in a processed dataset source.
    This lets you re-run Gemini "from scratch" for that dataset, without deleting raw inputs.
    """
    if not source:
        raise HTTPException(status_code=400, detail="source is required")

    try:
        from models import GrievanceProcessed, RowEnrichmentCheckpoint, TicketEnrichmentCheckpoint

        row_mode = str(source or "").endswith("__id_unique") or "__id_unique__" in str(source or "")
        deleted = 0

        if row_mode:
            gids = [
                g
                for (g,) in db.execute(
                    select(GrievanceProcessed.raw_id)
                    .where(
                        GrievanceProcessed.source_raw_filename == source,
                        GrievanceProcessed.raw_id.is_not(None),
                        func.trim(GrievanceProcessed.raw_id) != "",
                    )
                    .distinct()
                ).all()
                if g
            ]
            if not gids:
                return {"source": source, "deleted_checkpoints": 0, "note": "No raw_id values found for this source."}
            chunk = 500
            for i in range(0, len(gids), chunk):
                part = gids[i : i + chunk]
                res = db.execute(delete(RowEnrichmentCheckpoint).where(RowEnrichmentCheckpoint.raw_id.in_(part)))
                deleted += int(res.rowcount or 0)
                db.commit()
            return {"source": source, "deleted_checkpoints": deleted, "distinct_raw_ids": len(gids)}

        codes = [
            c
            for (c,) in db.execute(
                select(GrievanceProcessed.grievance_code)
                .where(
                    GrievanceProcessed.source_raw_filename == source,
                    GrievanceProcessed.grievance_code.is_not(None),
                    func.trim(GrievanceProcessed.grievance_code) != "",
                )
                .distinct()
            ).all()
            if c
        ]
        if not codes:
            return {"source": source, "deleted_checkpoints": 0, "note": "No grievance_code values found for this source."}
        chunk = 500
        for i in range(0, len(codes), chunk):
            part = codes[i : i + chunk]
            res = db.execute(delete(TicketEnrichmentCheckpoint).where(TicketEnrichmentCheckpoint.grievance_code.in_(part)))
            deleted += int(res.rowcount or 0)
            db.commit()
        return {"source": source, "deleted_checkpoints": deleted, "distinct_grievance_codes": len(codes)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/build_ai_output_dataset")
def build_ai_output_dataset(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    base_source: str = "",
    sample_size: int = 100,
    output_source: str = "ai_output_dataset",
    force_reprocess: bool = True,
):
    """
    Create an output dataset containing N rows cloned from base_source, then run Gemini enrichment on it.
    The resulting dataset is loadable in the UI dropdown and downloadable as CSV.
    """
    if not base_source:
        raise HTTPException(status_code=400, detail="base_source is required")
    try:
        psvc = ProcessedDataService()
        out_source = psvc.clone_sample_source(db, source=base_source, output_source=output_source, sample_size=int(sample_size))
        svc = EnrichmentService()
        run_id = svc.start_ticket_enrichment_background(
            db, source=out_source, limit_rows=int(sample_size), force_reprocess=bool(force_reprocess)
        )
        return {"output_source": out_source, "run_id": run_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/pipeline/stage_and_enrich")
def pipeline_stage_and_enrich(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    raw_filename: str | None = None,
    stage_rows: int = 5,
    force_preprocess: bool = False,
    force_reprocess_ai: bool = True,
):
    """
    Convenience pipeline for debugging:
    - Ensure processed_data_10738 exists (id-dedup, file order) and export to data/preprocess/
    - Create staged dataset processed_data_{stage_rows} and export to data/stage_data/
    - Run Gemini enrichment on staged dataset (exactly stage_rows rows)
    - AI outputs will be written to data/ai_outputs/processed_data_{stage_rows}_ai_outputs.csv after completion
    """
    stage_rows = max(1, min(int(stage_rows or 5), 500))

    try:
        from sqlalchemy import select, func as sa_func
        from models import GrievanceProcessed
        import os, json as pyjson

        raw_dir = "raw2"
        svc = EnrichmentService()
        latest = svc.get_raw_file_by_name(raw_filename, raw_dir=raw_dir) if raw_filename else svc.detect_latest_raw_file(raw_dir=raw_dir)
        if not latest:
            raise HTTPException(status_code=404, detail="No raw file found in data/raw2")

        psvc = ProcessedDataService()

        # Ensure preprocess dataset exists unless forced
        existing_cnt = (
            db.execute(
                select(sa_func.count())
                .select_from(GrievanceProcessed)
                .where(GrievanceProcessed.source_raw_filename == "processed_data_10738")
            ).scalar()
            or 0
        )
        if force_preprocess or int(existing_cnt) <= 0:
            pre = psvc.preprocess_latest(
                db,
                raw_filename=latest.filename,
                raw_dir=raw_dir,
                limit_rows=None,
                dedupe_mode="id",
                output_source="processed_data_10738",
            )
            pre_csv = os.path.join(settings.data_preprocess_dir, "processed_data_10738.csv")
            n_pre = psvc.export_source_to_csv(db, source="processed_data_10738", out_path=pre_csv, limit_rows=None)
        else:
            pre_csv = os.path.join(settings.data_preprocess_dir, "processed_data_10738.csv")
            n_pre = int(existing_cnt)

        # Stage dataset
        stage_source = f"processed_data_{stage_rows}"
        stage_source = psvc.clone_sample_source(
            db,
            source="processed_data_10738",
            output_source=stage_source,
            sample_size=stage_rows,
        )
        stage_csv = os.path.join(settings.data_stage_dir, f"{stage_source}.csv")
        n_stage = psvc.export_source_to_csv(db, source=stage_source, out_path=stage_csv, limit_rows=None)

        # Run Gemini
        enr = EnrichmentService()
        run_id = enr.start_ticket_enrichment_background(
            db,
            source=stage_source,
            limit_rows=stage_rows,
            force_reprocess=bool(force_reprocess_ai),
        )

        # Write a pointer file for easy tracking
        os.makedirs(settings.data_runs_dir, exist_ok=True)
        ptr = os.path.join(settings.data_runs_dir, f"{stage_source}_latest_run.json")
        with open(ptr, "w", encoding="utf-8") as f:
            f.write(pyjson.dumps({"run_id": run_id, "source": stage_source, "raw_filename": latest.filename}, indent=2))

        return {
            "raw_filename": latest.filename,
            "preprocess_source": "processed_data_10738",
            "preprocess_csv": pre_csv,
            "preprocess_rows": int(n_pre),
            "stage_source": stage_source,
            "stage_csv": stage_csv,
            "stage_rows": int(n_stage),
            "run_id": run_id,
            "ai_outputs_csv": os.path.join(settings.data_ai_outputs_dir, f"{stage_source}_ai_outputs.csv"),
            "note": "Track progress via /api/data/runs/latest_public?source=<stage_source> (auth-free only supports processed_data_500) or /api/data/runs/{run_id} with auth.",
        }
    except HTTPException:
        raise
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


@router.get("/runs/latest_public")
def latest_run_public(
    db: Session = Depends(get_db),
    source: str = "processed_data_500",
):
    """
    Public (no-auth) progress endpoint for the file-pipeline staged dataset.
    This MUST be declared BEFORE /runs/{run_id} because Starlette routes are matched in order.
    """
    if source != "processed_data_500":
        raise HTTPException(status_code=403, detail="Only processed_data_500 is allowed on this endpoint")

    from sqlalchemy import select
    from models import EnrichmentRun

    r = (
        db.execute(
            select(EnrichmentRun)
            .where(EnrichmentRun.raw_filename == source)
            .order_by(EnrichmentRun.started_at.desc())
            .limit(1)
        )
        .scalar_one_or_none()
    )
    if not r:
        return {"run": None}
    return {
        "run": {
            "run_id": r.run_id,
            "status": r.status,
            "started_at": r.started_at.isoformat(),
            "finished_at": r.finished_at.isoformat() if r.finished_at else None,
            "total_rows": r.total_rows,
            "processed": r.processed,
            "skipped": r.skipped,
            "failed": r.failed,
            "error": r.error,
        }
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


@router.get("/runs/latest")
def latest_run(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    source: str | None = None,
):
    """
    Convenience endpoint to find the latest enrichment run (optionally filtered by source/dataset name).
    """
    from sqlalchemy import select
    from models import EnrichmentRun

    q = select(EnrichmentRun).order_by(EnrichmentRun.started_at.desc())
    if source:
        q = q.where(EnrichmentRun.raw_filename == source)
    r = db.execute(q.limit(1)).scalar_one_or_none()
    if not r:
        return {"run": None}
    return {
        "run": {
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


@router.get("/processed/download")
def download_processed(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    source: str | None = None,
    limit_rows: int | None = None,
):
    """
    Download the current DB-backed processed dataset as CSV.
    This is the "updated sheet" after preprocessing + ticket-level AI enrichment.

    - source is required and must match grievances_processed.source_raw_filename
    - Example sources:
      - Close Grievances (1).csv            (FULL dataset)
      - Close Grievances (1).csv__run1_3100 (sample subset)
    """
    if not source:
        raise HTTPException(status_code=400, detail="source is required")

    try:
        from sqlalchemy import select
        from models import GrievanceProcessed

        q = select(GrievanceProcessed).where(GrievanceProcessed.source_raw_filename == source)
        # Keep exports stable and useful (latest-first).
        q = q.order_by(GrievanceProcessed.created_date.desc().nullslast())
        if limit_rows is not None:
            lr = int(limit_rows)
            if lr <= 0:
                raise HTTPException(status_code=400, detail="limit_rows must be > 0")
            q = q.limit(lr)

        rows = db.execute(q).scalars().all()
        if not rows:
            raise HTTPException(status_code=404, detail=f"No processed rows found for source={source}")

        # Write to processed dir (safe file name)
        safe = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in str(source))
        out_path = os.path.join(settings.data_processed_dir, f"grievances_processed__{safe}.csv")

        cols = [c.name for c in GrievanceProcessed.__table__.columns]
        import csv

        os.makedirs(settings.data_processed_dir, exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in rows:
                w.writerow({k: getattr(r, k) for k in cols})

        return FileResponse(out_path, media_type="text/csv", filename=os.path.basename(out_path))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/results")
def results(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    limit: int = 50,
    offset: int = 0,
    source: str | None = None,
):
    """
    Results for UI tables.
    - If source is provided: read from grievances_processed (preferred; supports ticket-level + new AI fields).
    - Else: fall back to the legacy enriched CSV.
    """
    limit = max(1, min(int(limit or 50), 500))
    offset = max(0, int(offset or 0))

    if source:
        from sqlalchemy import select
        from models import GrievanceProcessed

        total_rows = (
            db.execute(
                select(func.count())
                .select_from(GrievanceProcessed)
                .where(GrievanceProcessed.source_raw_filename == source)
            ).scalar()
            or 0
        )
        rows = (
            db.execute(
                select(GrievanceProcessed)
                .where(GrievanceProcessed.source_raw_filename == source)
                .order_by(GrievanceProcessed.created_date.desc().nullslast())
                .offset(offset)
                .limit(limit)
            )
            .scalars()
            .all()
        )
        out = []
        for r in rows:
            out.append(
                {
                    "grievance_id": r.grievance_id,
                    "raw_id": r.raw_id,
                    "grievance_code": r.grievance_code,
                    "created_date": r.created_date.isoformat() if r.created_date else None,
                    "closed_date": r.closed_date.isoformat() if r.closed_date else None,
                    "resolution_days": r.resolution_days,
                    "forward_count": int(r.forward_count or 0),
                    "feedback_rating": r.feedback_rating,
                    "ward": r.ward_name,
                    "department": r.department_name,
                    "status": r.status,
                    "subject": r.subject,
                    "description": r.description,
                    "category": r.ai_category,
                    "subcategory": r.ai_subtopic,
                    "confidence": r.ai_confidence,
                    "issue_type": r.ai_issue_type,
                    "entities_json": r.ai_entities_json,
                    "urgency": r.ai_urgency,
                    "sentiment": r.ai_sentiment,
                    "resolution_quality": r.ai_resolution_quality,
                    "reopen_risk": r.ai_reopen_risk,
                    "feedback_driver": r.ai_feedback_driver,
                    "closure_theme": r.ai_closure_theme,
                    "extra_summary": r.ai_extra_summary,
                    "actionable_score": r.actionable_score,
                    "ai_model": r.ai_model,
                    "ai_error": r.ai_error,
                }
            )

        return {
            "source": source,
            "offset": offset,
            "limit": limit,
            "total_rows": int(total_rows),
            "rows": out,
        }

    # Legacy fallback: enriched CSV
    import pandas as pd

    path = os.path.join(settings.data_processed_dir, "grievances_enriched.csv")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="No enriched CSV found. Run enrichment first.")
    print(f"[PIPELINE] UI results read ENRICHED file from: {path}")

    df = pd.read_csv(path)
    total_rows = int(len(df))
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
                # Extra AI features (derived from new raw2 columns when enabled)
                "resolution_quality": str(r.get("AI_ResolutionQuality", "")).strip(),
                "reopen_risk": str(r.get("AI_ReopenRisk", "")).strip(),
                "feedback_driver": str(r.get("AI_FeedbackDriver", "")).strip(),
                "closure_theme": str(r.get("AI_ClosureTheme", "")).strip(),
                "extra_summary": str(r.get("AI_ExtraSummary", "")).strip(),
                "extra_model": str(r.get("AI_ExtraModel", "")).strip(),
                "extra_error": str(r.get("AI_ExtraError", "")).strip(),
            }
        )

    return {
        "source": "data/processed/grievances_enriched.csv",
        "offset": offset,
        "limit": limit,
        "total_rows": total_rows,
        "rows": rows,
    }


