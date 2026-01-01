from __future__ import annotations

import os
import threading

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
from services.data_service import DataService
from services.enrichment_service import EnrichmentService


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

        # Auto preload (localhost): preprocess + enrich a bounded set (default 100) from the latest raw file.
        # Non-blocking: runs in a daemon thread after startup.
        if settings.auto_preload_on_startup:
            def _auto_preload() -> None:
                try:
                    svc = EnrichmentService()
                    latest = svc.detect_latest_raw_file()
                    if not latest:
                        print("[AUTO] No raw file found in data/raw; skipping auto preload.")
                        return

                    pre = os.path.join(settings.data_processed_dir, "preprocessed_latest.csv")
                    try:
                        raw_mtime = os.path.getmtime(latest.path)
                        pre_mtime = os.path.getmtime(pre) if os.path.exists(pre) else 0
                    except Exception:
                        raw_mtime = 0
                        pre_mtime = 0
                    if pre_mtime >= raw_mtime:
                        # If up-to-date but previous preprocessed file has fewer rows than the configured preload limit,
                        # regenerate (example: user previously tested with a small file, then changed AUTO_PRELOAD_LIMIT).
                        try:
                            import pandas as pd

                            if os.path.exists(pre):
                                df_check = pd.read_csv(pre, nrows=settings.auto_preload_limit + 1)
                                if len(df_check) >= settings.auto_preload_limit:
                                    print("[AUTO] Preprocessed latest is up-to-date; skipping auto preload.")
                                    return
                                print(
                                    f"[AUTO] Preprocessed latest has only {len(df_check)} rows (<{settings.auto_preload_limit}); regenerating."
                                )
                        except Exception:
                            print("[AUTO] Preprocessed latest is up-to-date; skipping auto preload.")
                            return

                    with session_scope() as db:
                        run_id = svc.start_run_background(db, row_limit=settings.auto_preload_limit)
                        print(
                            f"[AUTO] Started preload run_id={run_id} raw={latest.filename} limit={settings.auto_preload_limit}"
                        )
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


