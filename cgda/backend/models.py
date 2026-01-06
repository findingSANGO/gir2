from __future__ import annotations

import datetime as dt

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class GrievanceRaw(Base):
    __tablename__ = "grievances_raw"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    grievance_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    created_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)
    closed_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True)

    ward: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    department: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)  # aka service
    feedback_star: Mapped[float | None] = mapped_column(Float, nullable=True)

    grievance_text: Mapped[str] = mapped_column(Text, nullable=False)
    raw_payload_json: Mapped[str] = mapped_column(Text, nullable=False)

    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow(), nullable=False)

    structured: Mapped["GrievanceStructured"] = relationship(
        back_populates="raw", uselist=False, cascade="all, delete-orphan"
    )

    __table_args__ = (UniqueConstraint("grievance_id", name="uq_grievance_id"),)


class GrievanceStructured(Base):
    __tablename__ = "grievances_structured"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    raw_id: Mapped[int] = mapped_column(Integer, ForeignKey("grievances_raw.id"), nullable=False, unique=True)

    # Derived (AI) fields per grievance (structured outputs)
    category: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    sub_issue: Mapped[str] = mapped_column(String(256), nullable=False)
    sentiment: Mapped[str] = mapped_column(String(32), nullable=False, index=True)  # negative/neutral/positive
    severity: Mapped[str] = mapped_column(String(32), nullable=False, index=True)  # low/medium/high
    repeat_flag: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    delay_risk: Mapped[str] = mapped_column(String(32), nullable=False, index=True)  # low/med/high
    dissatisfaction_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    ai_rationale: Mapped[str] = mapped_column(Text, nullable=False)
    ai_provider: Mapped[str] = mapped_column(String(32), nullable=False, default="caseA")
    ai_engine: Mapped[str] = mapped_column(String(32), nullable=False, default="Gemini")
    ai_model: Mapped[str] = mapped_column(String(128), nullable=False)
    ai_version: Mapped[str] = mapped_column(String(32), nullable=False, default="v1")

    processed_at: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow(), nullable=False)
    is_mock: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    raw: Mapped[GrievanceRaw] = relationship(back_populates="structured")


class EnrichmentRun(Base):
    """
    Tracks a single ingestion+enrichment run for NMMC/IES raw Excel/CSV inputs.
    """

    __tablename__ = "enrichment_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    raw_filename: Mapped[str] = mapped_column(String(256), nullable=False)
    raw_path: Mapped[str] = mapped_column(String(512), nullable=False)
    preprocessed_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    enriched_path: Mapped[str | None] = mapped_column(String(512), nullable=True)

    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued", index=True)
    started_at: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow(), nullable=False)
    finished_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True)

    total_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    processed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    skipped: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    summary_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class EnrichmentCheckpoint(Base):
    """
    Resume checkpoint per grievance_key + input_hash.
    This enables incremental processing without duplicating work.
    """

    __tablename__ = "enrichment_checkpoints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    grievance_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)

    ai_input_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    ai_category: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    ai_subtopic: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    ai_confidence: Mapped[str] = mapped_column(String(16), nullable=False, default="Low")
    ai_model: Mapped[str] = mapped_column(String(128), nullable=False, default="gemini-2.0-flash")
    ai_run_timestamp: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow(), nullable=False)

    ai_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (UniqueConstraint("grievance_key", name="uq_enrichment_grievance_key"),)


class EnrichmentExtraCheckpoint(Base):
    """
    Additional Gemini-derived fields that leverage extended columns (e.g., rating/closing remarks/forwarding).
    Kept separate from EnrichmentCheckpoint to avoid breaking existing flows.
    """

    __tablename__ = "enrichment_extra_checkpoints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    grievance_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)

    ai_extra_input_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    # Governance-safe, audit-friendly fields (no free-form sensitive data)
    ai_resolution_quality: Mapped[str] = mapped_column(String(16), nullable=False, default="Unknown")  # High/Medium/Low/Unknown
    ai_reopen_risk: Mapped[str] = mapped_column(String(16), nullable=False, default="Unknown")  # High/Medium/Low/Unknown
    ai_feedback_driver: Mapped[str | None] = mapped_column(String(128), nullable=True)  # short phrase
    ai_closure_theme: Mapped[str | None] = mapped_column(String(128), nullable=True)  # short phrase
    ai_summary: Mapped[str | None] = mapped_column(Text, nullable=True)  # 1–2 sentences max

    ai_model: Mapped[str] = mapped_column(String(128), nullable=False, default="gemini-3-flash")
    ai_run_timestamp: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow(), nullable=False)
    ai_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (UniqueConstraint("grievance_key", name="uq_enrichment_extra_grievance_key"),)


class GrievanceProcessed(Base):
    """
    Normalized dataset for fast date-range analytics (derived from immutable data/raw inputs).
    This table is used by the date-range analytics endpoints and is safe to rebuild idempotently.
    """

    __tablename__ = "grievances_processed"

    grievance_id: Mapped[str] = mapped_column(String(64), primary_key=True)

    # Provenance (helps compare multiple ingested datasets without losing history)
    source_raw_filename: Mapped[str | None] = mapped_column(String(256), nullable=True, index=True)

    # Raw row identity from the input file (Close Grievances 'id'). This is the primary key you care about
    # for id-dedup + Gemini enrichment on the row-unique dataset.
    raw_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    # Original row order index from the raw file (0-based). Used to take the “first 500” deterministically.
    source_row_index: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

    created_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True)
    created_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True, index=True)
    created_month: Mapped[str | None] = mapped_column(String(16), nullable=True)
    created_week: Mapped[str | None] = mapped_column(String(16), nullable=True)

    ward_name: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    department_name: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    status: Mapped[str | None] = mapped_column(String(64), nullable=True)

    subject: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    closing_remark: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Additional fields observed in alternative exports (e.g., "Close Grievances" dumps)
    grievance_code: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    assignee_name: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)

    closed_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True)
    closed_date: Mapped[dt.date | None] = mapped_column(Date, nullable=True, index=True)
    feedback_rating: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Derived operational metrics
    resolution_days: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    forward_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    actionable_score: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)  # 1..100 (deterministic)

    forwarded_at: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True)
    forward_remark: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Bilingual content (if present)
    subject_mr: Mapped[str | None] = mapped_column(Text, nullable=True)
    description_mr: Mapped[str | None] = mapped_column(Text, nullable=True)
    department_name_mr: Mapped[str | None] = mapped_column(Text, nullable=True)
    status_mr: Mapped[str | None] = mapped_column(Text, nullable=True)

    ai_category: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    ai_subtopic: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    ai_confidence: Mapped[str | None] = mapped_column(String(16), nullable=True)

    # Extended AI fields (Gemini record-level enrichment; stored for dashboards)
    ai_issue_type: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    ai_entities_json: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON array
    ai_urgency: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)  # Low/Med/High
    ai_sentiment: Mapped[str | None] = mapped_column(String(8), nullable=True, index=True)  # Neg/Neu/Pos

    ai_resolution_quality: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)  # High/Medium/Low/Unknown
    ai_reopen_risk: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)  # High/Medium/Low/Unknown
    ai_feedback_driver: Mapped[str | None] = mapped_column(String(128), nullable=True)
    ai_closure_theme: Mapped[str | None] = mapped_column(String(128), nullable=True)
    ai_extra_summary: Mapped[str | None] = mapped_column(Text, nullable=True)

    ai_model: Mapped[str | None] = mapped_column(String(128), nullable=True)
    ai_run_timestamp: Mapped[dt.datetime | None] = mapped_column(DateTime, nullable=True)
    ai_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class TicketEnrichmentCheckpoint(Base):
    """
    Checkpointing for record-level enrichment of the processed ticket dataset.
    Keyed by grievance_code (ticket-level), resumable via ai_input_hash.
    """

    __tablename__ = "ticket_enrichment_checkpoints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    grievance_code: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    ai_input_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    ai_category: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    ai_subtopic: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    ai_confidence: Mapped[str | None] = mapped_column(String(16), nullable=True)
    ai_issue_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ai_entities_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    ai_urgency: Mapped[str | None] = mapped_column(String(16), nullable=True)
    ai_sentiment: Mapped[str | None] = mapped_column(String(8), nullable=True)

    ai_resolution_quality: Mapped[str | None] = mapped_column(String(16), nullable=True)
    ai_reopen_risk: Mapped[str | None] = mapped_column(String(16), nullable=True)
    ai_feedback_driver: Mapped[str | None] = mapped_column(String(128), nullable=True)
    ai_closure_theme: Mapped[str | None] = mapped_column(String(128), nullable=True)
    ai_extra_summary: Mapped[str | None] = mapped_column(Text, nullable=True)

    ai_model: Mapped[str | None] = mapped_column(String(128), nullable=True)
    ai_run_timestamp: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow(), nullable=False)
    ai_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (UniqueConstraint("grievance_code", name="uq_ticket_enrich_grievance_code"),)


class RowEnrichmentCheckpoint(Base):
    """
    Checkpointing for record-level enrichment when the dataset is row-unique (e.g. __id_unique).
    Keyed by grievances_processed.raw_id (raw input 'id') so we can enrich 100/100 rows reliably.
    """

    __tablename__ = "row_enrichment_checkpoints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    raw_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    ai_input_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    ai_category: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    ai_subtopic: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    ai_confidence: Mapped[str | None] = mapped_column(String(16), nullable=True)
    ai_issue_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ai_entities_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    ai_urgency: Mapped[str | None] = mapped_column(String(16), nullable=True)
    ai_sentiment: Mapped[str | None] = mapped_column(String(8), nullable=True)

    ai_resolution_quality: Mapped[str | None] = mapped_column(String(16), nullable=True)
    ai_reopen_risk: Mapped[str | None] = mapped_column(String(16), nullable=True)
    ai_feedback_driver: Mapped[str | None] = mapped_column(String(128), nullable=True)
    ai_closure_theme: Mapped[str | None] = mapped_column(String(128), nullable=True)
    ai_extra_summary: Mapped[str | None] = mapped_column(Text, nullable=True)

    ai_model: Mapped[str | None] = mapped_column(String(128), nullable=True)
    ai_run_timestamp: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow(), nullable=False)
    ai_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (UniqueConstraint("raw_id", name="uq_row_enrich_raw_id"),)


class ReportUpload(Base):
    __tablename__ = "report_uploads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    period_type: Mapped[str] = mapped_column(String(16), nullable=False, index=True)  # weekly/monthly/quarterly/annual
    period_start: Mapped[dt.date] = mapped_column(Date, nullable=False, index=True)
    period_end: Mapped[dt.date] = mapped_column(Date, nullable=False, index=True)
    uploaded_at: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow(), nullable=False, index=True)
    uploaded_by: Mapped[str | None] = mapped_column(String(64), nullable=True)

    file_path: Mapped[str] = mapped_column(String(512), nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


class PreprocessRun(Base):
    __tablename__ = "preprocess_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    raw_filename: Mapped[str] = mapped_column(String(256), nullable=False)
    raw_path: Mapped[str] = mapped_column(String(512), nullable=False)
    raw_mtime_iso: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="completed", index=True)
    record_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    processed_at: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow(), nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

