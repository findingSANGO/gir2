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


class GrievanceProcessed(Base):
    """
    Normalized dataset for fast date-range analytics (derived from immutable data/raw inputs).
    This table is used by the date-range analytics endpoints and is safe to rebuild idempotently.
    """

    __tablename__ = "grievances_processed"

    grievance_id: Mapped[str] = mapped_column(String(64), primary_key=True)

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

    ai_category: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    ai_subtopic: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    ai_confidence: Mapped[str | None] = mapped_column(String(16), nullable=True)


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

