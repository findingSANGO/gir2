from __future__ import annotations

import os
import threading
import json

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from config import settings
from database import engine, session_scope
from models import Base
from routes import analytics as analytics_routes
from routes import auth as auth_routes
from routes import data as data_routes
from routes import grievances as grievances_routes
from routes import overview as overview_routes
from routes import reports as reports_routes
from services.data_service import DataService
from services.enrichment_service import EnrichmentService
from services.processed_data_service import ProcessedDataService


def create_app() -> FastAPI:
    app = FastAPI(title=settings.app_name)

    origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins or ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(auth_routes.router)
    app.include_router(grievances_routes.router)
    app.include_router(data_routes.router)
    app.include_router(analytics_routes.router)
    app.include_router(analytics_routes.reports_router)
    app.include_router(overview_routes.router)
    app.include_router(reports_routes.router)

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.on_event("startup")
    def _startup() -> None:
        # Log path config clearly for debugging
        print(f"[CONFIG] DATA_RAW_DIR={settings.data_raw_dir}")
        print(f"[CONFIG] DATA_PROCESSED_DIR={settings.data_processed_dir}")
        print(f"[CONFIG] DATA_RUNS_DIR={settings.data_runs_dir}")
        print(f"[CONFIG] DATABASE_URL={settings.database_url}")
        print(f"[CONFIG] AUTO_PRELOAD_ON_STARTUP={settings.auto_preload_on_startup}")
        print(f"[CONFIG] AUTO_PRELOAD_LIMIT={settings.auto_preload_limit}")
        print(f"[CONFIG] AUTO_STAGE_ROWS={getattr(settings, 'auto_stage_rows', None)}")
        print(f"[CONFIG] GEMINI_API_KEY_CONFIGURED={bool(settings.gemini_api_key)}")

        if settings.recreate_db_on_startup:
            Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)

        # Lightweight SQLite migrations (no Alembic): add missing columns when schema evolves.
        # This keeps localhost stable even if an older cgda.db is present under the mounted data/ volume.
        if str(settings.database_url).startswith("sqlite:"):
            with engine.begin() as conn:
                # enrichment_runs: add new paths columns if missing
                try:
                    cols = [r[1] for r in conn.execute(text("PRAGMA table_info(enrichment_runs)")).fetchall()]
                    if "preprocessed_path" not in cols:
                        conn.execute(text("ALTER TABLE enrichment_runs ADD COLUMN preprocessed_path VARCHAR(512)"))
                    if "enriched_path" not in cols:
                        conn.execute(text("ALTER TABLE enrichment_runs ADD COLUMN enriched_path VARCHAR(512)"))
                except Exception:
                    # Table may not exist yet; ignore.
                    pass

                # grievances_processed: add new analytic fields if missing (backwards compatible)
                try:
                    cols = [r[1] for r in conn.execute(text("PRAGMA table_info(grievances_processed)")).fetchall()]
                    # Provenance
                    if "source_raw_filename" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN source_raw_filename VARCHAR(256)"))
                    if "raw_id" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN raw_id VARCHAR(64)"))
                    if "source_row_index" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN source_row_index INTEGER"))
                    # Extra operational fields
                    if "grievance_code" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN grievance_code VARCHAR(64)"))
                    if "assignee_name" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN assignee_name VARCHAR(128)"))
                    if "closed_at" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN closed_at DATETIME"))
                    if "closed_date" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN closed_date DATE"))
                    if "feedback_rating" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN feedback_rating FLOAT"))
                    if "forwarded_at" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN forwarded_at DATETIME"))
                    if "forward_remark" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN forward_remark TEXT"))
                    # Bilingual fields
                    if "subject_mr" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN subject_mr TEXT"))
                    if "description_mr" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN description_mr TEXT"))
                    if "department_name_mr" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN department_name_mr TEXT"))
                    if "status_mr" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN status_mr TEXT"))
                    # Ticket-level operational metrics
                    if "resolution_days" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN resolution_days INTEGER"))
                    if "forward_count" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN forward_count INTEGER DEFAULT 0"))
                    if "actionable_score" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN actionable_score INTEGER"))
                    # Extended AI fields (stored on processed rows)
                    if "ai_issue_type" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN ai_issue_type VARCHAR(64)"))
                    if "ai_entities_json" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN ai_entities_json TEXT"))
                    if "ai_urgency" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN ai_urgency VARCHAR(16)"))
                    if "ai_sentiment" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN ai_sentiment VARCHAR(8)"))
                    if "ai_resolution_quality" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN ai_resolution_quality VARCHAR(16)"))
                    if "ai_reopen_risk" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN ai_reopen_risk VARCHAR(16)"))
                    if "ai_feedback_driver" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN ai_feedback_driver VARCHAR(128)"))
                    if "ai_closure_theme" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN ai_closure_theme VARCHAR(128)"))
                    if "ai_extra_summary" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN ai_extra_summary TEXT"))
                    if "ai_model" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN ai_model VARCHAR(128)"))
                    if "ai_run_timestamp" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN ai_run_timestamp DATETIME"))
                    if "ai_error" not in cols:
                        conn.execute(text("ALTER TABLE grievances_processed ADD COLUMN ai_error TEXT"))
                except Exception:
                    # Table may not exist yet; ignore.
                    pass

                # enrichment_extra_checkpoints table is created via metadata; no ALTERs needed here.

        # Auto preload (localhost): preprocess + enrich a bounded set (default 100) from the latest raw file.
        # Non-blocking: runs in a daemon thread after startup.
        if settings.auto_preload_on_startup:
            def _auto_preload() -> None:
                try:
                    svc = EnrichmentService()
                    latest = svc.detect_latest_raw_file(raw_dir="raw2")
                    if not latest:
                        print("[AUTO] No raw file found in data/raw2; skipping auto preload.")
                        return

                    with session_scope() as db:
                        psvc = ProcessedDataService()
                        # 1) Build preprocess dataset: processed_data_10738 (id-dedup; file order)
                        pre = psvc.preprocess_latest(
                            db,
                            raw_filename=latest.filename,
                            raw_dir="raw2",
                            limit_rows=None,
                            dedupe_mode="id",
                            output_source="processed_data_10738",
                        )
                        print(f"[PIPELINE] Preprocess completed: processed_data_10738 rows={pre.get('record_count')}")

                        # 1b) Delta ingest: append new grievances from data/raw3/extra_grievances.csv into processed_data_10738
                        delta_added = 0
                        try:
                            raw3_path = os.path.join(getattr(settings, "data_raw3_dir", ""), "extra_grievances.csv")
                            if raw3_path and os.path.exists(raw3_path):
                                delta = psvc.preprocess_delta(
                                    db,
                                    raw_filename="extra_grievances.csv",
                                    raw_dir="raw3",
                                    dedupe_mode="id",
                                    output_source="processed_data_10738",
                                )
                                delta_added = int(delta.get("record_count") or 0)
                                print(
                                    f"[PIPELINE] Delta preprocess completed: raw3/extra_grievances.csv new_rows={delta.get('record_count')}"
                                )
                            else:
                                print("[PIPELINE] No raw3/extra_grievances.csv found; skipping delta ingest.")
                        except Exception as e:
                            print(f"[PIPELINE] Delta ingest failed (continuing): {type(e).__name__}: {e}")

                        # Export preprocess CSV
                        pre_csv = os.path.join(settings.data_preprocess_dir, "processed_data_10738.csv")
                        n_pre = psvc.export_source_to_csv(db, source="processed_data_10738", out_path=pre_csv, limit_rows=None)
                        print(f"[PIPELINE] Wrote preprocess CSV: {pre_csv} rows={n_pre}")

                        # 1c) Optional raw4 ingest: Excel export with "Details" sheet (deduped by id to ~11280).
                        # This is loaded as a separate dataset source so the Datasets tab can browse it directly.
                        try:
                            raw4_name = "NMMC - Grievance Data - 07.01.2026.xlsx"
                            raw4_path = os.path.join(getattr(settings, "data_raw4_dir", ""), raw4_name)
                            if raw4_path and os.path.exists(raw4_path):
                                out_src = "processed_data_raw4"
                                pre4 = psvc.preprocess_latest(
                                    db,
                                    raw_filename=raw4_name,
                                    raw_dir="raw4",
                                    limit_rows=None,
                                    dedupe_mode="id",
                                    output_source=out_src,
                                )
                                print(f"[PIPELINE] Raw4 preprocess completed: {out_src} rows={pre4.get('record_count')}")
                                pre4_csv = os.path.join(settings.data_preprocess_dir, f"{out_src}.csv")
                                n_pre4 = psvc.export_source_to_csv(db, source=out_src, out_path=pre4_csv, limit_rows=None)
                                print(f"[PIPELINE] Wrote raw4 preprocess CSV: {pre4_csv} rows={n_pre4}")

                                # If the staged/master dataset already has AI outputs, backfill them into raw4
                                # so the Datasets tab can show 11280 rows + AI without rerunning Gemini.
                                try:
                                    n_backfill = psvc.backfill_ai_fields_from_source(
                                        db,
                                        target_source=out_src,
                                        from_source="processed_data_500",
                                        join_key="raw_id",
                                    )
                                    print(f"[PIPELINE] Raw4 AI backfill from processed_data_500 by raw_id: updated_rows={int(n_backfill)}")

                                    # Export an explicit ai_outputs snapshot for raw4 too (for auditing/debug).
                                    raw4_ai_csv = os.path.join(settings.data_ai_outputs_dir, f"{out_src}_ai_outputs.csv")
                                    n_raw4_ai = psvc.export_source_to_csv(db, source=out_src, out_path=raw4_ai_csv, limit_rows=None)
                                    print(f"[PIPELINE] Wrote raw4 ai_outputs CSV: {raw4_ai_csv} rows={int(n_raw4_ai)}")

                                    # Enrich ONLY missing AI rows via Gemini to complete the dataset.
                                    # This is resumable/idempotent via checkpoints, and will re-export the ai_outputs CSV on completion.
                                    try:
                                        run4_id = svc.start_ticket_enrichment_background(
                                            db,
                                            source=out_src,
                                            limit_rows=None,
                                            force_reprocess=False,
                                            only_missing=True,
                                        )
                                        print(f"[PIPELINE] Started Gemini enrichment for raw4 missing rows: run_id={run4_id} source={out_src}")
                                    except Exception as e:
                                        print(f"[PIPELINE] Raw4 missing-row Gemini enrich failed to start (continuing): {type(e).__name__}: {e}")
                                except Exception as e:
                                    print(f"[PIPELINE] Raw4 AI backfill/export failed (continuing): {type(e).__name__}: {e}")
                            else:
                                print("[PIPELINE] No raw4 excel found; skipping raw4 ingest.")
                        except Exception as e:
                            print(f"[PIPELINE] Raw4 ingest failed (continuing): {type(e).__name__}: {e}")

                        # 2) Stage dataset: first N rows into processed_data_500 (N controlled by AUTO_STAGE_ROWS)
                        stage_rows = int(getattr(settings, "auto_stage_rows", 500) or 500)
                        # If we appended delta rows, default to staging the full updated master so the app reflects the new records.
                        if delta_added > 0:
                            stage_rows = max(stage_rows, int(n_pre or 0))
                        # Allow larger staged datasets (e.g., 10k) while still keeping a guardrail.
                        # clone_sample_source supports up to 20k, which is plenty for local demo/debug.
                        stage_rows = max(1, min(stage_rows, 20000))
                        stage_source = psvc.clone_sample_source(
                            db,
                            source="processed_data_10738",
                            output_source="processed_data_500",
                            sample_size=stage_rows,
                        )
                        stage_csv = os.path.join(settings.data_stage_dir, "processed_data_500.csv")
                        n_stage = psvc.export_source_to_csv(db, source=stage_source, out_path=stage_csv, limit_rows=None)
                        print(f"[PIPELINE] Wrote stage CSV: {stage_csv} rows={n_stage}")
                        try:
                            from sqlalchemy import func, select
                            from models import GrievanceProcessed

                            staged_cnt = (
                                db.execute(
                                    select(func.count()).where(GrievanceProcessed.source_raw_filename == "processed_data_500")
                                ).scalar_one()
                                or 0
                            )
                            print(f"[PIPELINE] Staged DB rows for processed_data_500: {int(staged_cnt)} (expected {stage_rows})")
                        except Exception as e:
                            print(f"[PIPELINE] Failed to count staged rows: {type(e).__name__}: {e}")

                        # 3) If AI outputs CSV already exists, import it into DB so the app can show outputs immediately.
                        # IMPORTANT: Only do this for the normal 500-row stage. In debug runs (AUTO_STAGE_ROWS < 500),
                        # importing a previously generated 500-row snapshot would override the intended small dataset.
                        if stage_rows == 500:
                            ai_csv = os.path.join(settings.data_ai_outputs_dir, "processed_data_500_ai_outputs.csv")
                            if os.path.exists(ai_csv):
                                try:
                                    n_imp = psvc.import_ai_outputs_csv(db, csv_path=ai_csv, source_override="processed_data_500")
                                    print(f"[PIPELINE] Imported AI outputs CSV into DB: {ai_csv} rows={n_imp}")
                                    # Only short-circuit if the AI outputs CSV actually had rows.
                                    if int(n_imp or 0) > 0:
                                        return
                                except Exception as e:
                                    print(f"[PIPELINE] Failed to import AI outputs CSV (will run Gemini): {type(e).__name__}: {e}")

                        # 4) Run Gemini on staged dataset (N)
                        run_id = svc.start_ticket_enrichment_background(
                            db, source="processed_data_500", limit_rows=stage_rows, force_reprocess=False
                        )
                        print(
                            f"[PIPELINE] Started Gemini enrichment run_id={run_id} source=processed_data_500 rows={stage_rows}"
                        )

                        # Persist run id for easy progress tracking (no log-grepping).
                        try:
                            os.makedirs(settings.data_runs_dir, exist_ok=True)
                            path = os.path.join(settings.data_runs_dir, "processed_data_500_latest_run.json")
                            with open(path, "w", encoding="utf-8") as f:
                                f.write(json.dumps({"run_id": run_id, "source": "processed_data_500"}, indent=2))
                            print(f"[PIPELINE] Wrote run pointer: {path}")
                        except Exception as e:
                            print(f"[PIPELINE] Failed to write run pointer: {type(e).__name__}: {e}")
                except Exception as e:
                    print(f"[AUTO] Preload failed: {type(e).__name__}: {e}")

            threading.Thread(target=_auto_preload, daemon=True, name="auto-preload").start()

        # Seed sample data for demo (only if DB empty)
        if not settings.seed_sample_data:
            return

        data_svc = DataService()
        with session_scope() as db:
            if data_svc.has_any_data(db):
                return
            if not os.path.exists(settings.sample_csv_path):
                return
            data_svc.ingest_csv_into_db(db, settings.sample_csv_path)
            # Ensure seed inserts are committed.
            db.commit()
        # IMPORTANT: Do not auto-run Gemini processing on startup.
        # It can create SQLite write contention (database is locked) during demo usage.
        # AI structuring is triggered on-demand via:
        # - CSV upload (server-side), or
        # - /api/grievances/process_pending (UI bootstrap), free-tier friendly.

    return app


app = create_app()


