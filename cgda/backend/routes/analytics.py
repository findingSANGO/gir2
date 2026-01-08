from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from auth import User, require_role
from database import get_db
from services.analytics_service import AnalyticsService
from services.ai_service import AIService

router = APIRouter(prefix="/api/analytics", tags=["analytics"])
reports_router = APIRouter(prefix="/api/reports", tags=["reports"])


def _svc() -> AnalyticsService:
    return AnalyticsService()


def _parse_filters(
    start_date: str | None,
    end_date: str | None,
    wards: str | None,
    department: str | None,
    category: str | None,
    source: str | None,
):
    import datetime as dt
    from services.analytics_service import Filters

    def _d(s: str | None) -> dt.date | None:
        if not s:
            return None
        return dt.datetime.strptime(s, "%Y-%m-%d").date()

    ward_list = [w.strip() for w in (wards or "").split(",") if w.strip()] or None
    return Filters(
        start_date=_d(start_date),
        end_date=_d(end_date),
        wards=ward_list,
        department=department or None,
        category=category or None,
        source=source or None,
    )


@router.get("/dimensions")
def dimensions(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
):
    return _svc().dimensions(db)


@router.get("/dimensions_processed")
def dimensions_processed(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
):
    return _svc().processed_dimensions(db)


@router.get("/datasets_processed")
def datasets_processed(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
):
    return _svc().processed_datasets(db)


@router.get("/dataset_quality")
def dataset_quality(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    source: str | None = None,
):
    """
    Lightweight dataset cross-checks for a single processed dataset source.

    Computes:
    - duplicates vs non-duplicates by raw_id (if raw_id present)
    - deduplicated row count (distinct raw_id, else total)
    - closed_date coverage
    - star rating coverage
    - both closed_date AND star rating
    """
    from sqlalchemy import case, func, select
    from models import GrievanceProcessed

    src = (source or "processed_data_500").strip()

    total = int(
        db.execute(select(func.count()).select_from(GrievanceProcessed).where(GrievanceProcessed.source_raw_filename == src))
        .scalar_one()
        or 0
    )

    # Duplicates are defined by raw_id within the dataset source (the true input PK).
    distinct_raw = int(
        db.execute(
            select(func.count(func.distinct(GrievanceProcessed.raw_id)))
            .select_from(GrievanceProcessed)
            .where(GrievanceProcessed.source_raw_filename == src, GrievanceProcessed.raw_id.is_not(None))
        )
        .scalar_one()
        or 0
    )
    duplicates = max(0, total - distinct_raw) if distinct_raw else 0
    non_duplicates = total - duplicates
    deduped_total = distinct_raw if distinct_raw else total

    closed_known = int(
        db.execute(
            select(func.count())
            .select_from(GrievanceProcessed)
            .where(GrievanceProcessed.source_raw_filename == src, GrievanceProcessed.closed_date.is_not(None))
        )
        .scalar_one()
        or 0
    )

    rating_ok = (
        (GrievanceProcessed.feedback_rating.is_not(None))
        & (GrievanceProcessed.feedback_rating >= 1)
        & (GrievanceProcessed.feedback_rating <= 5)
    )
    rating_known = int(
        db.execute(select(func.count()).select_from(GrievanceProcessed).where(GrievanceProcessed.source_raw_filename == src, rating_ok))
        .scalar_one()
        or 0
    )

    both_known = int(
        db.execute(
            select(func.count())
            .select_from(GrievanceProcessed)
            .where(GrievanceProcessed.source_raw_filename == src, GrievanceProcessed.closed_date.is_not(None), rating_ok)
        )
        .scalar_one()
        or 0
    )

    return {
        "source": src,
        "total_rows": total,
        "duplicates": {"by": "raw_id", "duplicate_rows": duplicates, "non_duplicate_rows": non_duplicates},
        "deduplicated_total_rows": deduped_total,
        "closed_date_rows": closed_known,
        "star_rating_rows": rating_known,
        "closed_date_and_star_rating_rows": both_known,
    }


@router.get("/ai_coverage")
def ai_coverage(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    source: str | None = None,
):
    """
    AI output coverage for a single processed dataset source.

    Returns non-null / non-empty counts per AI output column so the UI can show
    fill-rates for each AI field.
    """
    from sqlalchemy import func, select, case
    from models import GrievanceProcessed

    src = (source or "processed_data_500").strip()

    def _non_empty(col):
        # For text-ish columns: count if not null and trim(col) != ""
        return case(((col.is_not(None)) & (func.trim(col) != ""), 1), else_=0)

    def _non_empty_json(col):
        # For JSON stored as text: treat "[]" / "{}" as empty too.
        t = func.trim(col)
        return case(((col.is_not(None)) & (t != "") & (t != "[]") & (t != "{}") & (t != "null"), 1), else_=0)

    row = db.execute(
        select(
            func.count().label("total_rows"),
            func.sum(_non_empty(GrievanceProcessed.ai_category)).label("ai_category"),
            func.sum(_non_empty(GrievanceProcessed.ai_subtopic)).label("ai_subtopic"),
            func.sum(_non_empty(GrievanceProcessed.ai_confidence)).label("ai_confidence"),
            func.sum(_non_empty(GrievanceProcessed.ai_issue_type)).label("ai_issue_type"),
            func.sum(_non_empty_json(GrievanceProcessed.ai_entities_json)).label("ai_entities"),
            func.sum(_non_empty(GrievanceProcessed.ai_urgency)).label("ai_urgency"),
            func.sum(_non_empty(GrievanceProcessed.ai_sentiment)).label("ai_sentiment"),
            func.sum(_non_empty(GrievanceProcessed.ai_resolution_quality)).label("ai_resolution_quality"),
            func.sum(_non_empty(GrievanceProcessed.ai_reopen_risk)).label("ai_reopen_risk"),
            func.sum(_non_empty(GrievanceProcessed.ai_feedback_driver)).label("ai_feedback_driver"),
            func.sum(_non_empty(GrievanceProcessed.ai_closure_theme)).label("ai_closure_theme"),
            func.sum(_non_empty(GrievanceProcessed.ai_extra_summary)).label("ai_extra_summary"),
            func.sum(_non_empty(GrievanceProcessed.ai_model)).label("ai_model"),
            func.sum(_non_empty(GrievanceProcessed.ai_error)).label("ai_error"),
        ).where(GrievanceProcessed.source_raw_filename == src)
    ).mappings().first()

    if not row:
        return {"source": src, "total_rows": 0, "counts": {}}

    total = int(row.get("total_rows") or 0)
    counts = {k: int(row.get(k) or 0) for k in row.keys() if k != "total_rows"}
    return {"source": src, "total_rows": total, "counts": counts}

@router.get("/retrospective")
def retrospective(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    category: str | None = None,
    source: str | None = None,
):
    f = _parse_filters(start_date, end_date, wards, department, category, source)
    return _svc().retrospective(db, f)


@router.get("/inferential")
def inferential(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    category: str | None = None,
    source: str | None = None,
):
    f = _parse_filters(start_date, end_date, wards, department, category, source)
    return _svc().inferential(db, f)


@router.get("/predictive")
def predictive(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    category: str | None = None,
    source: str | None = None,
):
    f = _parse_filters(start_date, end_date, wards, department, category, source)
    return _svc().predictive(db, f)


@router.get("/feedback")
def feedback(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    category: str | None = None,
    source: str | None = None,
):
    f = _parse_filters(start_date, end_date, wards, department, category, source)
    return _svc().feedback(db, f)


@router.get("/closure")
def closure(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    category: str | None = None,
    source: str | None = None,
):
    f = _parse_filters(start_date, end_date, wards, department, category, source)
    return _svc().closure(db, f)


@router.get("/closure_sla_snapshot")
def closure_sla_snapshot(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    category: str | None = None,
    source: str | None = None,
):
    """
    Dedicated Closure Timeliness (SLA) snapshot for Issue Intelligence 2 → Overview.
    Uses grievances_processed only. No Gemini calls.
    """
    f = _parse_filters(start_date, end_date, wards, department, category, source)
    return _svc().closure_sla_snapshot(db, f)


@router.get("/forwarding_snapshot")
def forwarding_snapshot(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    category: str | None = None,
    source: str | None = None,
):
    """
    Dedicated Forwarding Analytics snapshot for Issue Intelligence 2 → Overview.
    Uses grievances_processed only. No Gemini calls.
    """
    f = _parse_filters(start_date, end_date, wards, department, category, source)
    return _svc().forwarding_snapshot(db, f)


@router.get("/forwarding_impact_resolution")
def forwarding_impact_resolution(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    category: str | None = None,
    source: str | None = None,
):
    """
    Forwarding impact on resolution time ("process tax") comparison.
    Dedicated endpoint for Issue Intelligence 2 → Overview.
    """
    f = _parse_filters(start_date, end_date, wards, department, category, source)
    return _svc().forwarding_impact_resolution(db, f)


@router.get("/wordcloud")
def wordcloud(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    category: str | None = None,
    source: str | None = None,
    top_n: int = 60,
):
    f = _parse_filters(start_date, end_date, wards, department, category, source)
    return _svc().wordcloud(db, f, top_n=top_n)


def _parse_required_dates(start_date: str | None, end_date: str | None):
    import datetime as dt

    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required (YYYY-MM-DD)")
    s = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
    e = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
    if e < s:
        raise ValueError("end_date must be >= start_date")
    return s, e


@router.get("/executive-overview")
def executive_overview(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    ai_category: str | None = None,
    source: str | None = None,
    top_n: int = 10,
):
    try:
        s, e = _parse_required_dates(start_date, end_date)
    except Exception as ex:
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail=str(ex)) from ex
    ward_list = [w.strip() for w in (wards or "").split(",") if w.strip()] or None
    return _svc().executive_overview(
        db,
        start_date=s,
        end_date=e,
        wards=ward_list,
        department=department or None,
        ai_category=ai_category or None,
        source=source or None,
        top_n=top_n,
    )


@router.get("/top-subtopics")
def top_subtopics(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    ai_category: str | None = None,
    source: str | None = None,
    top_n: int = 10,
):
    try:
        s, e = _parse_required_dates(start_date, end_date)
    except Exception as ex:
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail=str(ex)) from ex
    ward_list = [w.strip() for w in (wards or "").split(",") if w.strip()] or None
    return _svc().top_subtopics(
        db,
        start_date=s,
        end_date=e,
        wards=ward_list,
        department=department or None,
        ai_category=ai_category or None,
        source=source or None,
        top_n=top_n,
    )


@router.get("/top-subtopics/by-ward")
def top_subtopics_by_ward(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    ward: str = "",
    start_date: str | None = None,
    end_date: str | None = None,
    department: str | None = None,
    ai_category: str | None = None,
    source: str | None = None,
    top_n: int = 5,
):
    try:
        s, e = _parse_required_dates(start_date, end_date)
    except Exception as ex:
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail=str(ex)) from ex
    return _svc().top_subtopics_by_ward(
        db,
        start_date=s,
        end_date=e,
        ward=ward,
        department=department or None,
        ai_category=ai_category or None,
        source=source or None,
        top_n=top_n,
    )


@router.get("/top-subtopics/by-department")
def top_subtopics_by_department(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    department: str = "",
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    ai_category: str | None = None,
    source: str | None = None,
    top_n: int = 10,
):
    try:
        s, e = _parse_required_dates(start_date, end_date)
    except Exception as ex:
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail=str(ex)) from ex
    ward_list = [w.strip() for w in (wards or "").split(",") if w.strip()] or None
    return _svc().top_subtopics_by_department(
        db,
        start_date=s,
        end_date=e,
        department=department,
        wards=ward_list,
        ai_category=ai_category or None,
        source=source or None,
        top_n=top_n,
    )


@router.get("/subtopic-trend")
def subtopic_trend(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    subtopic: str = "",
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    ai_category: str | None = None,
    source: str | None = None,
):
    try:
        s, e = _parse_required_dates(start_date, end_date)
    except Exception as ex:
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail=str(ex)) from ex
    ward_list = [w.strip() for w in (wards or "").split(",") if w.strip()] or None
    return _svc().subtopic_trend(
        db,
        start_date=s,
        end_date=e,
        subtopic=subtopic,
        wards=ward_list,
        department=department or None,
        ai_category=ai_category or None,
        source=source or None,
    )


@router.get("/one-of-a-kind")
def one_of_a_kind(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    ai_category: str | None = None,
    source: str | None = None,
    limit: int = 25,
):
    """
    One-of-a-kind = sub-topics that appear exactly once in the selected range + filters.
    """
    try:
        s, e = _parse_required_dates(start_date, end_date)
    except Exception as ex:
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail=str(ex)) from ex
    ward_list = [w.strip() for w in (wards or "").split(",") if w.strip()] or None
    return _svc().one_of_a_kind_complaints(
        db,
        start_date=s,
        end_date=e,
        wards=ward_list,
        department=department or None,
        ai_category=ai_category or None,
        source=source or None,
        limit=limit,
    )


# =========================
# Predictive Analytics (Early Warning) — NEW
# =========================


@router.get("/predictive/rising-subtopics")
def predictive_rising_subtopics(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    ai_category: str | None = None,
    source: str | None = None,
    window_days: int = 14,
    min_volume: int = 10,
    growth_threshold: float = 0.5,
    top_n: int = 15,
):
    try:
        s, e = _parse_required_dates(start_date, end_date)
    except Exception as ex:
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail=str(ex)) from ex
    ward_list = [w.strip() for w in (wards or "").split(",") if w.strip()] or None
    return _svc().predictive_rising_subtopics(
        db,
        start_date=s,
        end_date=e,
        wards=ward_list,
        department=department or None,
        ai_category=ai_category or None,
        source=source or None,
        window_days=window_days,
        min_volume=min_volume,
        growth_threshold=growth_threshold,
        top_n=top_n,
    )


@router.get("/predictive/ward-risk")
def predictive_ward_risk(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    ai_category: str | None = None,
    source: str | None = None,
    window_days: int = 14,
    min_ward_volume: int = 30,
):
    try:
        s, e = _parse_required_dates(start_date, end_date)
    except Exception as ex:
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail=str(ex)) from ex
    ward_list = [w.strip() for w in (wards or "").split(",") if w.strip()] or None
    return _svc().predictive_ward_risk(
        db,
        start_date=s,
        end_date=e,
        wards=ward_list,
        department=department or None,
        ai_category=ai_category or None,
        source=source or None,
        window_days=window_days,
        min_ward_volume=min_ward_volume,
    )


@router.get("/predictive/chronic-issues")
def predictive_chronic_issues(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    ai_category: str | None = None,
    source: str | None = None,
    period: str = "week",
    top_n_per_period: int = 5,
    min_periods: int = 4,
    limit: int = 20,
):
    try:
        s, e = _parse_required_dates(start_date, end_date)
    except Exception as ex:
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail=str(ex)) from ex
    ward_list = [w.strip() for w in (wards or "").split(",") if w.strip()] or None
    return _svc().predictive_chronic_issues(
        db,
        start_date=s,
        end_date=e,
        wards=ward_list,
        department=department or None,
        ai_category=ai_category or None,
        source=source or None,
        period=period,
        top_n_per_period=top_n_per_period,
        min_periods=min_periods,
        limit=limit,
    )


@router.post("/predictive/explain")
def predictive_explain(
    payload: dict,
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
):
    # No DB needed; explanation only.
    return _svc().predictive_explain(payload=payload)


@router.get("/subtopics/top")
def subtopics_top(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    category: str | None = None,
    source: str | None = None,
    limit: int = 10,
):
    f = _parse_filters(start_date, end_date, wards, department, category, source)
    return _svc().subtopics_top(db, f, limit=limit)


@router.get("/subtopics/by-ward")
def subtopics_by_ward(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    ward: str = "",
    start_date: str | None = None,
    end_date: str | None = None,
    department: str | None = None,
    category: str | None = None,
    source: str | None = None,
    limit: int = 5,
):
    # ward selector is explicit for this endpoint; other filters still apply.
    f = _parse_filters(start_date, end_date, None, department, category, source)
    return _svc().subtopics_by_ward(db, f, ward=ward, limit=limit)


@router.get("/subtopics/by-department")
def subtopics_by_department(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    department: str = "",
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    category: str | None = None,
    source: str | None = None,
    limit: int = 10,
):
    # department selector is explicit for this endpoint; other filters still apply.
    f = _parse_filters(start_date, end_date, wards, None, category, source)
    return _svc().subtopics_by_department(db, f, department=department, limit=limit)


@router.get("/subtopics/trend")
def subtopics_trend(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
    subtopic: str = "",
    start_date: str | None = None,
    end_date: str | None = None,
    wards: str | None = None,
    department: str | None = None,
    category: str | None = None,
    source: str | None = None,
):
    f = _parse_filters(start_date, end_date, wards, department, category, source)
    return _svc().subtopics_trend(db, f, subtopic=subtopic)


@reports_router.get("/commissioner_pdf")
def commissioner_pdf(
    _: Annotated[User, Depends(require_role("admin", "commissioner"))],
    db: Session = Depends(get_db),
):
    # Minimal PDF summary for demo; generated locally.
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer
    import io
    import datetime as dt

    svc = _svc()
    retro = svc.retrospective(db, _parse_filters(None, None, None, None, None, None))
    infer = svc.inferential(db, _parse_filters(None, None, None, None, None, None))
    pred = svc.predictive(db, _parse_filters(None, None, None, None, None, None))
    bundle = {"retrospective": retro, "inferential": infer, "predictive": pred}
    summary = AIService().commissioner_summary(bundle)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, title="CGDA Commissioner Summary")
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph("CGDA – Commissioner Summary (Analytics Only)", styles["Title"]))
    story.append(Paragraph(f"Generated: {dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}", styles["Normal"]))
    story.append(Spacer(1, 0.4 * cm))

    story.append(Paragraph("<b>Retrospective</b>", styles["Heading2"]))
    t = retro.get("totals", {})
    story.append(Paragraph(f"Total grievances: <b>{t.get('totalGrievances')}</b>", styles["Normal"]))
    story.append(Paragraph(f"Avg closure days: <b>{t.get('avgClosureDays')}</b>", styles["Normal"]))
    story.append(Paragraph(f"Avg feedback: <b>{t.get('avgFeedback')}</b>", styles["Normal"]))
    story.append(Spacer(1, 0.2 * cm))

    story.append(Paragraph("<b>Inferential</b>", styles["Heading2"]))
    story.append(Paragraph(f"Low feedback (≤2) count: <b>{infer.get('lowFeedback', {}).get('count')}</b>", styles["Normal"]))
    story.append(Spacer(1, 0.2 * cm))

    story.append(Paragraph("<b>Predictive</b>", styles["Heading2"]))
    story.append(Paragraph(f"Wards at risk: <b>{len(pred.get('wardRisk', []) or [])}</b>", styles["Normal"]))
    story.append(Spacer(1, 0.3 * cm))

    story.append(Paragraph("<b>AI Summary</b> <font size=8>(Powered by caseA)</font>", styles["Heading2"]))
    for b in (summary.get("summary_bullets") or [])[:5]:
        story.append(Paragraph(f"• {b}", styles["Normal"]))

    doc.build(story)
    pdf = buf.getvalue()
    from fastapi.responses import Response

    return Response(
        content=pdf,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=cgda_commissioner_summary.pdf"},
    )


