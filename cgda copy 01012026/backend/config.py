import os
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

    # Data paths (mounted via docker-compose)
    data_raw_dir: str = os.getenv("DATA_RAW_DIR", "../data/raw")
    data_processed_dir: str = os.getenv("DATA_PROCESSED_DIR", "../data/processed")
    data_runs_dir: str = os.getenv("DATA_RUNS_DIR", "../data/runs")

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


settings = Settings()


