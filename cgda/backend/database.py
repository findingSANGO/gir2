from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import event
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from config import settings


def _sqlite_connect_args(url: str) -> dict:
    if url.startswith("sqlite:"):
        # timeout is in seconds for sqlite3.connect(); helps transient lock contention.
        return {"check_same_thread": False, "timeout": 60}
    return {}


engine = create_engine(
    settings.database_url,
    connect_args=_sqlite_connect_args(settings.database_url),
    pool_pre_ping=True,
)


# SQLite concurrency tuning:
# - WAL allows concurrent readers while a writer is running (critical for dashboards during enrichment).
# - busy_timeout makes reads/writes wait a bit instead of failing fast with "database is locked".
if str(settings.database_url).startswith("sqlite:"):
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:
        try:
            cur = dbapi_connection.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")
            cur.execute("PRAGMA temp_store=MEMORY;")
            cur.execute("PRAGMA foreign_keys=ON;")
            cur.execute("PRAGMA busy_timeout=60000;")  # ms
            cur.close()
        except Exception:
            # Best-effort; don't prevent startup.
            pass

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)


@contextmanager
def session_scope() -> Session:
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


