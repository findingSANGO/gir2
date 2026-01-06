from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from auth import User, require_role
from database import get_db
from services.analytics_service import AnalyticsService
from services.enrichment_service import EnrichmentService
from config import settings

router = APIRouter(prefix="/api", tags=["overview"])


def _svc() -> AnalyticsService:
    return AnalyticsService()


def _parse_required_dates(start_date: str | None, end_date: str | None):
    import datetime as dt

    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required (YYYY-MM-DD)")
    s = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
    e = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
    if e < s:
        raise ValueError("end_date must be >= start_date")
    return s, e


@router.get("/debug/pipeline_status")
def pipeline_status(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    source: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
):
    """
    TEMPORARY DEBUG ENDPOINT.
    Returns clear counts showing what is in:
    - raw2 (latest file)
    - preprocessed master dataset (processed_data_10738 in DB)
    - staged dataset (default processed_data_500 in DB)
    - AI-enriched rows (ai_subtopic filled)
    - rows eligible for dashboards under the current date filter
    """
    import datetime as dt
    from sqlalchemy import func, select, case
    from models import GrievanceProcessed

    src = (source or "processed_data_500").strip()

    # Parse dates if provided
    s = e = None
    if start_date and end_date:
        try:
            s = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
            e = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
        except Exception:
            s = e = None

    # Raw file info + row counts (best-effort, read-only)
    raw_info = {"latest_file": None, "raw_rows": None, "raw_unique_ids": None, "raw_path": None}
    try:
        svc = EnrichmentService()
        latest = svc.detect_latest_raw_file(raw_dir="raw2")
        if latest:
            raw_info["latest_file"] = latest.filename
            raw_info["raw_path"] = latest.path
            df = svc.load_raw_dataframe(latest.path)
            raw_info["raw_rows"] = int(len(df))
            # Use id column if present, else grievance_id
            if "id" in df.columns:
                raw_info["raw_unique_ids"] = int(df["id"].astype(str).str.strip().replace("nan", "").replace("None", "").nunique())
            elif "grievance_id" in df.columns:
                raw_info["raw_unique_ids"] = int(
                    df["grievance_id"].astype(str).str.strip().replace("nan", "").replace("None", "").nunique()
                )
    except Exception as ex:
        raw_info["error"] = f"{type(ex).__name__}: {ex}"

    def _cnt(where_clause):
        return int(db.execute(select(func.count()).where(where_clause)).scalar_one() or 0)

    def _minmax(where_clause):
        row = db.execute(
            select(func.min(GrievanceProcessed.created_date), func.max(GrievanceProcessed.created_date)).where(where_clause)
        ).one()
        dmin, dmax = row[0], row[1]
        return (dmin.isoformat() if dmin else None, dmax.isoformat() if dmax else None)

    # DB: preprocess + stage + AI coverage
    pre_source = "processed_data_10738"
    pre_total = _cnt(GrievanceProcessed.source_raw_filename == pre_source)
    stage_total = _cnt(GrievanceProcessed.source_raw_filename == src)

    created_nonnull = _cnt(
        (GrievanceProcessed.source_raw_filename == src) & (GrievanceProcessed.created_date.is_not(None))
    )
    stage_min, stage_max = _minmax(GrievanceProcessed.source_raw_filename == src)

    ai_filled = _cnt(
        (GrievanceProcessed.source_raw_filename == src)
        & (GrievanceProcessed.ai_subtopic.is_not(None))
        & (func.trim(GrievanceProcessed.ai_subtopic) != "")
    )

    # How many rows would dashboards see for the provided date range?
    eligible = None
    if s and e:
        eligible = _cnt(
            (GrievanceProcessed.source_raw_filename == src)
            & (GrievanceProcessed.created_date.is_not(None))
            & (GrievanceProcessed.created_date >= s)
            & (GrievanceProcessed.created_date <= e)
        )

    return {
        "paths": {
            "data_raw2_dir": settings.data_raw2_dir,
            "data_processed_dir": settings.data_processed_dir,
            "data_stage_dir": settings.data_stage_dir,
            "data_ai_outputs_dir": settings.data_ai_outputs_dir,
            "data_outputs_dir": settings.data_outputs_dir,
        },
        "raw2": raw_info,
        "db": {
            "preprocessed_source": pre_source,
            "preprocessed_rows": pre_total,
            "staged_source": src,
            "staged_rows": stage_total,
            "staged_created_date_nonnull": created_nonnull,
            "staged_created_date_min": stage_min,
            "staged_created_date_max": stage_max,
            "staged_ai_subtopic_filled": ai_filled,
            "eligible_rows_for_filter": eligible,
            "filter_start_date": start_date,
            "filter_end_date": end_date,
        },
    }


@router.get("/executive_overview")
def executive_overview_v2(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    category: str | None = None,
    source: str | None = None,
):
    try:
        s, e = _parse_required_dates(start_date, end_date)
    except Exception as ex:
        raise HTTPException(status_code=400, detail=str(ex)) from ex
    ward_list = [w.strip() for w in (wards or "").split(",") if w.strip()] or None
    return _svc().executive_overview_v2(
        db,
        start_date=s,
        end_date=e,
        wards=ward_list,
        department=department or None,
        category=category or None,
        source=source or None,
    )


@router.get("/issue_intelligence")
def issue_intelligence_v2(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    category: str | None = None,
    source: str | None = None,
    ward_focus: str | None = None,
    department_focus: str | None = None,
    subtopic_focus: str | None = None,
    unique_min_priority: int = 0,
    unique_confidence_high_only: bool = False,
):
    try:
        s, e = _parse_required_dates(start_date, end_date)
    except Exception as ex:
        raise HTTPException(status_code=400, detail=str(ex)) from ex
    ward_list = [w.strip() for w in (wards or "").split(",") if w.strip()] or None
    return _svc().issue_intelligence_v2(
        db,
        start_date=s,
        end_date=e,
        wards=ward_list,
        department=department or None,
        category=category or None,
        source=source or None,
        ward_focus=(ward_focus or "").strip() or None,
        department_focus=(department_focus or "").strip() or None,
        subtopic_focus=(subtopic_focus or "").strip() or None,
        unique_min_priority=int(unique_min_priority or 0),
        unique_confidence_high_only=bool(unique_confidence_high_only),
    )


