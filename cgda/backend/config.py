import os
from pathlib import Path

# Repo root is always the parent of /backend (i.e., cgda/)
repo_root = Path(__file__).resolve().parent.parent

# Load local environment variables (do NOT commit secrets).
# This enables developers to provide GEMINI_API_KEY via cgda/.env without exporting in every terminal.
try:
    from dotenv import load_dotenv  # type: ignore

    # Prefer repo root (.env alongside /backend and /frontend)
    load_dotenv(repo_root / ".env", override=False)
except Exception:
    # If python-dotenv isn't installed or file is missing, continue with process env.
    pass
from dataclasses import dataclass


def _get_env(name: str, default: str | None = None) -> str:
    val = os.getenv(name, default)
    if val is None or val == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


@dataclass(frozen=True)
class Settings:
    app_name: str = "CGDA Backend"
    env: str = os.getenv("APP_ENV", os.getenv("ENV", "local"))

    jwt_secret: str = os.getenv("JWT_SECRET", "dev-secret")
    jwt_algorithm: str = os.getenv("JWT_ALGORITHM", "HS256")
    jwt_exp_minutes: int = int(os.getenv("JWT_EXP_MINUTES", "480"))

    cors_origins: str = os.getenv("CORS_ORIGINS", "http://localhost:3000")

    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./cgda.db")

    # Basic users (demo, MVP) — defaults are safe for local demo
    commissioner_username: str = os.getenv("COMMISSIONER_USERNAME", "commissioner")
    commissioner_password: str = os.getenv("COMMISSIONER_PASSWORD", "commissioner123")
    admin_username: str = os.getenv("ADMIN_USER", os.getenv("ADMIN_USERNAME", "admin"))
    admin_password: str = os.getenv("ADMIN_PASS", os.getenv("ADMIN_PASSWORD", "admin123"))
    it_head_username: str = os.getenv("IT_HEAD_USERNAME", "it_head")
    it_head_password: str = os.getenv("IT_HEAD_PASSWORD", "ithead123")

    # Data paths (mounted via docker-compose). Defaults are absolute under repo root,
    # so running scripts from any cwd still works.
    data_raw_dir: str = os.getenv("DATA_RAW_DIR", str((repo_root / "data/raw").resolve()))
    # Optional additional input folder used for new exports with extra columns (e.g., raw2).
    data_raw2_dir: str = os.getenv("DATA_RAW2_DIR", str((repo_root / "data/raw2").resolve()))
    data_processed_dir: str = os.getenv("DATA_PROCESSED_DIR", str((repo_root / "data/processed").resolve()))
    data_runs_dir: str = os.getenv("DATA_RUNS_DIR", str((repo_root / "data/runs").resolve()))
    # Batch pipeline outputs (Gemini results parquet/csv)
    data_outputs_dir: str = os.getenv("DATA_OUTPUTS_DIR", str((repo_root / "data/outputs").resolve()))
    # New file-based pipeline folders
    data_preprocess_dir: str = os.getenv("DATA_PREPROCESS_DIR", str((repo_root / "data/preprocess").resolve()))
    data_stage_dir: str = os.getenv("DATA_STAGE_DIR", str((repo_root / "data/stage_data").resolve()))
    data_ai_outputs_dir: str = os.getenv("DATA_AI_OUTPUTS_DIR", str((repo_root / "data/ai_outputs").resolve()))

    # AI (Gemini) — models/config are fully controlled via env (no hardcoding in callers).
    # New preferred env vars:
    # - GEMINI_MODEL_PRIMARY (default gemini-3-pro)
    # - GEMINI_MODEL_FALLBACK (default gemini-3-flash)
    #
    # Backwards compatibility:
    # - GEMINI_MODEL_DEFAULT maps to PRIMARY if PRIMARY not set
    gemini_api_key: str | None = os.getenv("GEMINI_API_KEY")
    gemini_model_primary: str = os.getenv(
        "GEMINI_MODEL_PRIMARY",
        os.getenv("GEMINI_MODEL_DEFAULT", "gemini-3-pro"),
    )
    gemini_model_fallback: str = os.getenv("GEMINI_MODEL_FALLBACK", "gemini-3-flash")
    gemini_temperature: float = float(os.getenv("GEMINI_TEMPERATURE", "0.1"))
    gemini_max_output_tokens: int = int(os.getenv("GEMINI_MAX_OUTPUT_TOKENS", "256"))
    # Network + reliability tuning for Gemini (keeps UI responsive under bad connectivity).
    gemini_timeout_s: int = int(os.getenv("GEMINI_TIMEOUT_S", "20"))
    # Attempts per model (includes the initial try). Total attempts = attempts_per_model * number_of_models.
    gemini_attempts_per_model: int = int(os.getenv("GEMINI_ATTEMPTS_PER_MODEL", "2"))
    gemini_endpoint: str = os.getenv(
        "GEMINI_ENDPOINT",
        "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
    )

    # Seeding
    seed_sample_data: bool = os.getenv("SEED_SAMPLE_DATA", "true").lower() in ("1", "true", "yes")
    sample_csv_path: str = os.getenv("SAMPLE_CSV_PATH", "../data/raw/sample_grievances.csv")

    recreate_db_on_startup: bool = os.getenv("RECREATE_DB_ON_STARTUP", "false").lower() in ("1", "true", "yes")

    # Auto-preload (localhost UX): preprocess quickly and then enrich a bounded number of rows in background.
    auto_preload_on_startup: bool = os.getenv("AUTO_PRELOAD_ON_STARTUP", "true").lower() in ("1", "true", "yes")
    auto_preload_limit: int = int(os.getenv("AUTO_PRELOAD_LIMIT", "100"))
    # File-pipeline staging: how many rows to clone into the fixed demo source `processed_data_500`.
    # Setting this to 5 is the easiest way to make the app run on only 5 Gemini-enriched records.
    auto_stage_rows: int = int(os.getenv("AUTO_STAGE_ROWS", "500"))


settings = Settings()


