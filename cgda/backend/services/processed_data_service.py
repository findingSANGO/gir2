from __future__ import annotations

import datetime as dt
import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import func, select, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from config import settings
from models import EnrichmentCheckpoint, GrievanceProcessed, PreprocessRun
from services.enrichment_service import EnrichmentService


IST = ZoneInfo("Asia/Kolkata")


def _strip_cell_newlines(v):
    if isinstance(v, str):
        return re.sub(r"[\r\n]+", " ", v).strip()
    return v


@dataclass(frozen=True)
class PreprocessStatus:
    raw_filename: str
    raw_path: str
    raw_mtime_iso: str
    processed_at: str
    record_count: int
    status: str
    error: str | None


class ProcessedDataService:
    """
    Raw (immutable) Excel/CSV -> normalized DB table grievances_processed for fast filtering queries.
    IMPORTANT: No Gemini calls here. AI fields are read from stored checkpoints.
    """

    def __init__(self) -> None:
        # Reuse the robust reader + column mapping logic already built for the NMMC exports.
        self._raw = EnrichmentService()

    def preprocess_latest(
        self,
        db: Session,
        *,
        raw_filename: str | None = None,
        raw_dir: str | None = None,
        limit_rows: int | None = None,
        dedupe_mode: str = "ticket",
        output_source: str | None = None,
    ) -> dict:
        latest = (
            self._raw.get_raw_file_by_name(raw_filename, raw_dir=raw_dir)
            if raw_filename
            else self._raw.detect_latest_raw_file(raw_dir=raw_dir)
        )
        if not latest:
            raise ValueError(f"No raw file found under {settings.data_raw_dir}")

        raw_mtime_iso = latest.mtime_iso
        run = PreprocessRun(raw_filename=latest.filename, raw_path=latest.path, raw_mtime_iso=raw_mtime_iso, status="running")
        db.add(run)
        db.commit()

        try:
            count = self._preprocess_file_into_db(
                db,
                raw_path=latest.path,
                limit_rows=limit_rows,
                dedupe_mode=dedupe_mode,
                output_source=output_source,
            )
            run.status = "completed"
            run.record_count = int(count)
            run.error = None
            run.processed_at = dt.datetime.utcnow()
            db.commit()
            return {
                "status": "completed",
                "raw_filename": latest.filename,
                "record_count": int(count),
                "dedupe_mode": dedupe_mode,
                "output_source": output_source or Path(latest.path).name,
            }
        except Exception as e:
            run.status = "failed"
            run.error = f"{type(e).__name__}: {e}"
            run.processed_at = dt.datetime.utcnow()
            db.commit()
            raise

    def preprocess_delta(
        self,
        db: Session,
        *,
        raw_filename: str,
        raw_dir: str,
        output_source: str,
        dedupe_mode: str = "id",
    ) -> dict:
        """
        Append-only preprocessing for "delta" raw files (e.g., data/raw3/extra_grievances.csv).
        - Does NOT delete existing rows for output_source.
        - Inserts ONLY rows whose raw_id is not already present for output_source.
        - Preserves existing file order by appending source_row_index after the current max.
        """
        latest = self._raw.get_raw_file_by_name(raw_filename, raw_dir=raw_dir)
        if not latest:
            raise ValueError(f"Raw file not found: {raw_dir}/{raw_filename}")

        count = self._preprocess_file_into_db(
            db,
            raw_path=latest.path,
            limit_rows=None,
            dedupe_mode=dedupe_mode,
            output_source=str(output_source).strip(),
            mode="append_delta",
        )
        return {
            "status": "completed",
            "raw_filename": latest.filename,
            "record_count": int(count),
            "dedupe_mode": dedupe_mode,
            "output_source": output_source,
        }

    def latest_status(self, db: Session) -> PreprocessStatus | None:
        r = db.execute(select(PreprocessRun).order_by(PreprocessRun.processed_at.desc()).limit(1)).scalar_one_or_none()
        if not r:
            return None
        return PreprocessStatus(
            raw_filename=r.raw_filename,
            raw_path=r.raw_path,
            raw_mtime_iso=r.raw_mtime_iso,
            processed_at=r.processed_at.isoformat(),
            record_count=int(r.record_count),
            status=r.status,
            error=r.error,
        )

    def _preprocess_file_into_db(
        self,
        db: Session,
        *,
        raw_path: str,
        limit_rows: int | None = None,
        dedupe_mode: str = "ticket",
        output_source: str | None = None,
        mode: str = "rebuild",
    ) -> int:
        df = self._raw.load_raw_dataframe(raw_path)
        if limit_rows is not None and int(limit_rows) > 0:
            df = df.head(int(limit_rows))
        mapping = self._raw._map_columns(df)  # noqa: SLF001 (internal reuse; keeps behavior consistent)

        # Strip embedded newlines so 1 record == 1 row consistently.
        df = df.applymap(_strip_cell_newlines)

        # Helpers for optional columns (schema varies by input source)
        norm_to_actual = {re.sub(r"[^a-z0-9]+", " ", str(c).strip().lower()).strip(): c for c in df.columns}

        def opt(*names: str) -> pd.Series:
            for n in names:
                k = re.sub(r"[^a-z0-9]+", " ", str(n).strip().lower()).strip()
                col = norm_to_actual.get(k)
                if col is not None and col in df.columns:
                    return df[col]
            return pd.Series([None] * len(df))

        def clean_opt_str(s: pd.Series) -> pd.Series:
            """
            Convert optional text columns to clean strings, but keep missing values as None.
            Avoids storing literal 'None'/'nan' strings when the source column doesn't exist.
            """
            # Normalize pandas NaN/NA to None first
            s2 = s.where(pd.notna(s), None)
            out = []
            for v in s2.tolist():
                if v is None:
                    out.append(None)
                    continue
                t = str(v).strip()
                if not t:
                    out.append(None)
                    continue
                if t.lower() in ("none", "nan", "<na>"):
                    out.append(None)
                    continue
                out.append(t)
            return pd.Series(out)

        grievance_id_raw = df[mapping["Grievance Id"]].astype(str).str.strip()
        created_raw = df[mapping["Created Date"]]

        # Some exports use NULL/empty sentinels; normalize them before parsing.
        def _normalize_dt_series(s: pd.Series) -> pd.Series:
            s2 = s.where(pd.notna(s), None)
            out = []
            for v in s2.tolist():
                if v is None:
                    out.append(None)
                    continue
                t = str(v).strip()
                if not t or t.lower() in ("null", "none", "nan", "<na>"):
                    out.append(None)
                    continue
                out.append(v)
            return pd.Series(out)

        created_raw_n = _normalize_dt_series(created_raw)
        created = pd.to_datetime(created_raw_n, errors="coerce", dayfirst=True)

        # Robust fallbacks for Close Grievances exports:
        # 1) Fill missing Created Date using raw grievance_date/created_at columns (common in raw2 dumps)
        # 2) If still missing, fall back to close_date (keeps record visible to dashboards instead of dropping it)
        try:
            alt_raw = opt("grievance_date", "grievance date", "created_at", "created at", "created_date", "created date")
            alt = pd.to_datetime(_normalize_dt_series(alt_raw), errors="coerce", dayfirst=True)
            created = created.where(created.notna(), alt)
        except Exception:
            pass

        try:
            close_raw_for_created = opt("close_date", "close date", "closed_date", "closed date", "closedate")
            close_dt_for_created = pd.to_datetime(_normalize_dt_series(close_raw_for_created), errors="coerce", dayfirst=True)
            created = created.where(created.notna(), close_dt_for_created)
        except Exception:
            pass
        # Localize to IST if timezone-naive; then derive created_date/month/week in IST.
        if getattr(created.dt, "tz", None) is None:
            created_ist = created.dt.tz_localize(IST, ambiguous="NaT", nonexistent="shift_forward")
        else:
            created_ist = created.dt.tz_convert(IST)

        created_utc = created_ist.dt.tz_convert("UTC").dt.tz_localize(None)
        created_date = created_ist.dt.date
        created_month = created_ist.dt.strftime("%Y-%m")
        iso = created_ist.dt.isocalendar()
        # isocalendar() returns nullable dtypes when created_ist has NaT; avoid casting NA to int.
        year_s = iso["year"].astype("Int64").astype(str).replace("<NA>", None)
        week_s = iso["week"].astype("Int64").astype(str).str.zfill(2).replace("<NA>", None)
        created_week = (year_s + "-W" + week_s).where(year_s.notna() & week_s.notna(), None).astype(object)

        # Ward is missing in some raw2 exports (Close Grievances). We derive a deterministic "area" proxy
        # from Address (node/sector), and finally fall back to PIN code.
        ward = clean_opt_str(df[mapping["Ward Name"]])

        addr_raw = opt("address")
        addr = addr_raw.where(pd.notna(addr_raw), None).astype(object)
        addr_s = pd.Series([(str(x) if x is not None else "") for x in addr.tolist()])
        addr_l = addr_s.str.lower()

        # Sector extraction: "Sector 12", "Sector-12", etc.
        sector_num = addr_l.str.extract(r"\\bsector\\s*[-:]?\\s*([0-9]{1,2}[a-z]?)\\b", expand=False)
        sector_label = sector_num.map(lambda x: f"Sector {str(x).upper()}" if pd.notna(x) and str(x).strip() else None)

        # Node/locality extraction (common Navi Mumbai nodes)
        node_key = addr_l.str.extract(
            r"(ghansoli|airoli|nerul|vashi|belapur|turbhe|sanpada|seawoods|seawood|koparkhairane|kopar\\s*khairane|juinagar|jui\\s*nagar|kharghar|ulwe|panvel|kamothe|kalamboli|dronagiri)",
            expand=False,
        )
        def _norm_node(x: str | None) -> str | None:
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            t = str(x).strip().lower()
            if not t:
                return None
            if t in ("koparkhairane", "kopar khairane"):
                return "Kopar Khairane"
            if t in ("seawood", "seawoods"):
                return "Seawoods"
            if t in ("jui nagar", "juinagar"):
                return "Juinagar"
            return t.title()

        node_label = node_key.map(_norm_node)
        ward_from_addr = sector_label.where(sector_label.notna(), node_label)

        pin_raw = opt("pin_code", "pin code", "pincode")
        pin = pd.to_numeric(pin_raw, errors="coerce")
        ward_from_pin = pin.map(lambda x: f"PIN {int(x)}" if pd.notna(x) else None)

        # Only backfill when ward is missing/blank.
        ward = ward.where(ward.notna(), ward_from_addr)
        ward = ward.where(ward.notna(), ward_from_pin)

        dept = clean_opt_str(df[mapping["Current Department Name"]])
        status = clean_opt_str(df[mapping["Current Status"]])
        subject = df[mapping["Complaint Subject"]].astype(str)
        desc = df[mapping["Complaint Description"]].astype(str)
        closing = df[mapping["Closing Remark"]].astype(str)

        # Extra fields (best effort; may not exist for NMMC exports)
        base_source = Path(raw_path).name
        source_raw_filename = str(output_source).strip() if output_source else base_source
        if not source_raw_filename:
            source_raw_filename = base_source

        mode = str(mode or "rebuild").strip().lower()
        if mode not in ("rebuild", "append_delta"):
            raise ValueError("mode must be 'rebuild' or 'append_delta'")

        existing_raw_ids: set[str] = set()
        source_row_index_offset = 0
        if mode == "rebuild":
            # Rebuild the processed dataset for this source deterministically.
            db.query(GrievanceProcessed).filter(GrievanceProcessed.source_raw_filename == source_raw_filename).delete()
            db.commit()
        else:
            # Append-only: load existing raw_id keys for this source so we only add true deltas.
            existing_raw_ids = {
                str(x).strip()
                for x in (
                    db.execute(
                        select(GrievanceProcessed.raw_id).where(GrievanceProcessed.source_raw_filename == source_raw_filename)
                    )
                    .scalars()
                    .all()
                )
                if x is not None and str(x).strip()
            }
            max_idx = (
                db.execute(
                    select(func.max(GrievanceProcessed.source_row_index)).where(
                        GrievanceProcessed.source_raw_filename == source_raw_filename
                    )
                ).scalar_one()
                or 0
            )
            try:
                source_row_index_offset = int(max_idx) + 1
            except Exception:
                source_row_index_offset = int(len(existing_raw_ids))
        grievance_code = clean_opt_str(opt("grievance_code", "grievance code"))
        assignee = clean_opt_str(opt("empname", "assign", "current user name"))

        # Close date appears as close_date/closed_date in some exports, and "closedate" in raw2/raw3 dumps.
        closed_raw = opt("close_date", "close date", "closed_date", "closed date", "closedate")
        closed_dt = pd.to_datetime(closed_raw, errors="coerce")
        closed_date = closed_dt.dt.date
        closed_utc = closed_dt.dt.tz_localize(None)

        rating_raw = opt("rating", "feedback_star", "feedback", "star_rating")
        rating = pd.to_numeric(rating_raw, errors="coerce")

        fwd_raw = opt("forwarddate", "forward date", "forwarded_at")
        fwd_dt = pd.to_datetime(fwd_raw, errors="coerce")
        fwd_utc = fwd_dt.dt.tz_localize(None)
        fwd_remark = clean_opt_str(opt("forwardremark", "forward remark"))

        subject_mr = clean_opt_str(opt("subject_mr"))
        description_mr = clean_opt_str(opt("description_mr"))
        department_mr = clean_opt_str(opt("department_mr"))
        status_mr = clean_opt_str(opt("status_mr"))

        # Pull AI fields from stored checkpoints (no Gemini).
        cps = db.execute(select(EnrichmentCheckpoint)).scalars().all()
        cp_map = {c.grievance_key: c for c in cps}

        # Ticket identity:
        # Prefer grievance_code for ticket identity, fallback to raw grievance_id.
        ticket_key = grievance_code.where(
            grievance_code.notna() & (grievance_code.astype(str).str.strip() != ""),
            grievance_id_raw,
        ).astype(str).str.strip()

        dedupe_mode = str(dedupe_mode or "ticket").strip().lower()
        if dedupe_mode not in ("ticket", "id"):
            raise ValueError("dedupe_mode must be 'ticket' or 'id'")

        # Record identity (for this dataset build):
        # - ticket: one row per ticket_key (grievance_code)
        # - id: one row per raw grievance id ("id" column in Close Grievances dumps)
        record_key = ticket_key if dedupe_mode == "ticket" else grievance_id_raw.astype(str).str.strip()

        # Forwarding signal: count forwarddate occurrences per ticket.
        fwd_flag = pd.Series(fwd_dt.notna().astype(int).tolist(), index=df.index)
        forward_count_per_ticket = fwd_flag.groupby(ticket_key).sum().astype(int)
        forward_count = ticket_key.map(forward_count_per_ticket).fillna(0).astype(int)

        # Choose representative row:
        # - ticket mode: keep most "final" row (closed_date then created_at)
        # - id mode: keep FIRST occurrence in file order (matches Sheets “Remove duplicates” expectation)
        if dedupe_mode == "id":
            tmp = pd.DataFrame({"_k": record_key, "_i": df.index})
            tmp = tmp.sort_values(["_i"], ascending=[True])
            keep_idx = tmp.drop_duplicates("_k", keep="first")["_i"].tolist()
        else:
            sort_closed = closed_dt.fillna(pd.NaT)
            sort_created = created_utc
            tmp = pd.DataFrame(
                {
                    "_k": record_key,
                    "_i": df.index,
                    "_closed": sort_closed,
                    "_created": sort_created,
                }
            )
            tmp = tmp.sort_values(["_k", "_closed", "_created"], ascending=[True, True, True], na_position="first")
            keep_idx = tmp.drop_duplicates("_k", keep="last")["_i"].tolist()

        # Namespacing to allow multiple dataset sources to coexist in a table whose PK is grievance_id.
        # Base dataset keeps its natural key; derived/variant datasets get a stable short prefix.
        def _namespaced_id(key: str) -> str:
            if source_raw_filename == base_source:
                return key
            tag = hashlib.sha1(source_raw_filename.encode("utf-8")).hexdigest()[:8]
            candidate = f"{tag}:{key}"
            if len(candidate) <= 64:
                return candidate
            # Fallback: hash the key too (still stable).
            kh = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
            return f"{tag}:{kh}"

        rows = []
        for i in keep_idx:
            key = str(record_key.loc[i]).strip()
            if not key or key.lower() == "nan":
                continue
            if mode == "append_delta":
                rid = str(grievance_id_raw.loc[i]).strip() if pd.notna(grievance_id_raw.loc[i]) else ""
                if rid and rid in existing_raw_ids:
                    continue

            # Prefer checkpoint keyed by ticket key (grievance_code), fallback to raw grievance id.
            # This keeps AI fields populated even for id-dedup datasets where multiple rows share a ticket code.
            tk = str(ticket_key.loc[i]).strip()
            cp = (cp_map.get(tk) if tk else None) or cp_map.get(str(grievance_id_raw.loc[i]).strip())
            if cp and cp.ai_error:
                cp = None

            # Derived: resolution days (close_date - grievance_date)
            rd = None
            try:
                cd = closed_date.loc[i]
                gd = created_date.loc[i]
                if pd.notna(cd) and pd.notna(gd):
                    delta = (cd - gd).days
                    rd = int(delta) if delta >= 0 else None
            except Exception:
                rd = None

            rows.append(
                {
                    # grievance_id is the dataset-level unique key (namespaced for variants)
                    "grievance_id": _namespaced_id(key),
                    "source_raw_filename": source_raw_filename,
                    "raw_id": (str(grievance_id_raw.loc[i]).strip() if pd.notna(grievance_id_raw.loc[i]) else None),
                    "source_row_index": (int(i) + source_row_index_offset) if i is not None else None,
                    "created_at": created_utc.loc[i].to_pydatetime() if pd.notna(created_utc.loc[i]) else None,
                    "created_date": created_date.loc[i] if pd.notna(created_date.loc[i]) else None,
                    "created_month": str(created_month.loc[i]) if pd.notna(created_month.loc[i]) else None,
                    "created_week": str(created_week.loc[i]) if pd.notna(created_week.loc[i]) else None,
                    "ward_name": (ward.loc[i] if ward.loc[i] is not None else None),
                    "department_name": (dept.loc[i] if dept.loc[i] is not None else None),
                    "status": (status.loc[i] if status.loc[i] is not None else None),
                    "subject": (str(subject.loc[i]).strip() or None),
                    "description": (str(desc.loc[i]).strip() or None),
                    "closing_remark": (str(closing.loc[i]).strip() or None),
                    "grievance_code": (grievance_code.loc[i] if grievance_code.loc[i] is not None else None),
                    "assignee_name": (assignee.loc[i] if assignee.loc[i] is not None else None),
                    "closed_at": closed_utc.loc[i].to_pydatetime() if pd.notna(closed_utc.loc[i]) else None,
                    "closed_date": closed_date.loc[i] if pd.notna(closed_date.loc[i]) else None,
                    "feedback_rating": float(rating.loc[i]) if pd.notna(rating.loc[i]) else None,
                    "resolution_days": rd,
                    "forward_count": int(forward_count.loc[i]) if pd.notna(forward_count.loc[i]) else 0,
                    "forwarded_at": fwd_utc.loc[i].to_pydatetime() if pd.notna(fwd_utc.loc[i]) else None,
                    "forward_remark": (fwd_remark.loc[i] if fwd_remark.loc[i] is not None else None),
                    "subject_mr": (subject_mr.loc[i] if subject_mr.loc[i] is not None else None),
                    "description_mr": (description_mr.loc[i] if description_mr.loc[i] is not None else None),
                    "department_name_mr": (department_mr.loc[i] if department_mr.loc[i] is not None else None),
                    "status_mr": (status_mr.loc[i] if status_mr.loc[i] is not None else None),
                    "ai_category": (cp.ai_category if cp else None),
                    "ai_subtopic": (cp.ai_subtopic if cp else None),
                    "ai_confidence": (cp.ai_confidence if cp else None),
                }
            )

        if not rows:
            return 0

        # Idempotent upsert on grievance_id (SQLite).
        # This allows re-run without duplication and updates AI fields if checkpoints improved later.
        for start in range(0, len(rows), 1000):
            chunk = rows[start : start + 1000]
            stmt = sqlite_insert(GrievanceProcessed).values(chunk)
            update_cols = {c.name: getattr(stmt.excluded, c.name) for c in GrievanceProcessed.__table__.columns if c.name != "grievance_id"}
            stmt = stmt.on_conflict_do_update(index_elements=["grievance_id"], set_=update_cols)
            db.execute(stmt)
            db.commit()

        return len(rows)

    def build_run_sample(self, db: Session, *, source: str, sample_size: int = 100) -> str:
        """
        Create a derived dataset source containing ONLY sample_size records from an existing processed dataset.
        This supports "Run 1" behavior: app runs on a small enriched subset.
        """
        # Allow larger demo snapshots (e.g. 10k) while still protecting SQLite from accidental huge clones.
        sample_size = max(1, min(int(sample_size or 100), 20000))
        sample_source = f"{source}__run1_{sample_size}"

        # Delete old sample
        db.query(GrievanceProcessed).filter(GrievanceProcessed.source_raw_filename == sample_source).delete()
        db.commit()

        rows = (
            db.execute(
                select(GrievanceProcessed)
                .where(GrievanceProcessed.source_raw_filename == source)
                .order_by(GrievanceProcessed.created_date.desc().nullslast())
                .limit(sample_size)
            )
            .scalars()
            .all()
        )
        if not rows:
            return sample_source

        payload = []
        tag = hashlib.sha1(sample_source.encode("utf-8")).hexdigest()[:8]
        for r in rows:
            d = {c.name: getattr(r, c.name) for c in GrievanceProcessed.__table__.columns}
            d["source_raw_filename"] = sample_source
            # IMPORTANT: grievance_id is a global PK in grievances_processed, so we must namespace it
            # for derived datasets to coexist with the base dataset.
            base_id = str(d.get("grievance_id") or "").strip()
            if base_id:
                candidate = f"{tag}:{base_id}"
                d["grievance_id"] = candidate if len(candidate) <= 64 else f"{tag}:{hashlib.sha1(base_id.encode('utf-8')).hexdigest()[:16]}"
            payload.append(d)

        # Bulk upsert into sample source in chunks to avoid SQLite variable limits.
        chunk_size = 200
        for i in range(0, len(payload), chunk_size):
            chunk = payload[i : i + chunk_size]
            stmt = sqlite_insert(GrievanceProcessed).values(chunk)
            update_cols = {
                c.name: getattr(stmt.excluded, c.name)
                for c in GrievanceProcessed.__table__.columns
                if c.name != "grievance_id"
            }
            stmt = stmt.on_conflict_do_update(index_elements=["grievance_id"], set_=update_cols)
            db.execute(stmt)
            db.commit()
        return sample_source

    def clone_sample_source(self, db: Session, *, source: str, output_source: str, sample_size: int = 100) -> str:
        """
        Clone sample_size rows from an existing processed dataset into a NEW dataset source name.
        This is used to create stable output datasets like 'ai_output_dataset'.
        """
        output_source = str(output_source or "").strip()
        if not output_source:
            raise ValueError("output_source is required")
        sample_size = max(1, min(int(sample_size or 100), 20000))

        # Delete old output_source
        db.query(GrievanceProcessed).filter(GrievanceProcessed.source_raw_filename == output_source).delete()
        db.commit()

        # IMPORTANT: for the file pipeline we want “first N” = lowest source_row_index.
        rows = (
            db.execute(
                select(GrievanceProcessed)
                .where(GrievanceProcessed.source_raw_filename == source)
                .order_by(GrievanceProcessed.source_row_index.asc().nullslast(), GrievanceProcessed.created_date.asc().nullslast())
                .limit(sample_size)
            )
            .scalars()
            .all()
        )
        if not rows:
            return output_source

        # Reuse same namespacing logic as build_run_sample (global PK safety).
        payload = []
        import hashlib

        tag = hashlib.sha1(output_source.encode("utf-8")).hexdigest()[:8]
        for r in rows:
            d = {c.name: getattr(r, c.name) for c in GrievanceProcessed.__table__.columns}
            d["source_raw_filename"] = output_source
            base_id = str(d.get("grievance_id") or "").strip()
            if base_id:
                candidate = f"{tag}:{base_id}"
                d["grievance_id"] = candidate if len(candidate) <= 64 else f"{tag}:{hashlib.sha1(base_id.encode('utf-8')).hexdigest()[:16]}"
            payload.append(d)

        chunk_size = 200
        for i in range(0, len(payload), chunk_size):
            chunk = payload[i : i + chunk_size]
            stmt = sqlite_insert(GrievanceProcessed).values(chunk)
            update_cols = {
                c.name: getattr(stmt.excluded, c.name)
                for c in GrievanceProcessed.__table__.columns
                if c.name != "grievance_id"
            }
            stmt = stmt.on_conflict_do_update(index_elements=["grievance_id"], set_=update_cols)
            db.execute(stmt)
            db.commit()

        return output_source

    def export_source_to_csv(
        self,
        db: Session,
        *,
        source: str,
        out_path: str,
        limit_rows: int | None = None,
        order_by_row_index: bool = True,
    ) -> int:
        """
        Export a DB-backed processed dataset source to a CSV file.
        """
        if not source:
            raise ValueError("source is required")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)

        q = select(GrievanceProcessed).where(GrievanceProcessed.source_raw_filename == source)
        if order_by_row_index:
            q = q.order_by(GrievanceProcessed.source_row_index.asc().nullslast())
        else:
            q = q.order_by(GrievanceProcessed.created_date.desc().nullslast())
        if limit_rows is not None:
            q = q.limit(int(limit_rows))

        rows = db.execute(q).scalars().all()
        cols = [c.name for c in GrievanceProcessed.__table__.columns]

        import csv

        # Write atomically: avoid leaving an empty/partial file if something goes wrong.
        tmp_path = out_path + ".tmp"
        with open(tmp_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in rows:
                w.writerow({k: getattr(r, k) for k in cols})
        os.replace(tmp_path, out_path)
        return len(rows)

    def import_ai_outputs_csv(
        self,
        db: Session,
        *,
        csv_path: str,
        source_override: str | None = None,
    ) -> int:
        """
        Import a previously exported processed dataset CSV (including AI columns) into grievances_processed.
        This is used to "feed" data/ai_outputs/processed_data_500_ai_outputs.csv back into the app.
        """
        if not os.path.exists(csv_path):
            raise ValueError(f"CSV not found: {csv_path}")

        df = pd.read_csv(csv_path)
        if df.empty:
            return 0

        # Normalize NaN -> None for all fields
        df = df.where(pd.notna(df), None)

        # Type conversions for known columns
        for col in ("created_at", "closed_at", "forwarded_at", "ai_run_timestamp"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None)
                df[col] = df[col].where(pd.notna(df[col]), None)

        for col in ("created_date", "closed_date"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
                df[col] = df[col].where(pd.notna(df[col]), None)

        for col in ("feedback_rating",):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                df[col] = df[col].where(pd.notna(df[col]), None)

        for col in ("resolution_days", "forward_count", "actionable_score", "source_row_index"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                df[col] = df[col].where(pd.notna(df[col]), None)

        cols = [c.name for c in GrievanceProcessed.__table__.columns]
        missing = [c for c in ("grievance_id", "source_raw_filename") if c not in df.columns]
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")

        if source_override:
            df["source_raw_filename"] = str(source_override)

        payload = []
        for _, r in df.iterrows():
            d = {}
            for c in cols:
                if c in df.columns:
                    v = r.get(c)
                    # pandas Timestamp -> python datetime
                    if isinstance(v, pd.Timestamp):
                        v = v.to_pydatetime()
                    # pandas Int64 may be <NA>
                    if v is pd.NA:
                        v = None
                    d[c] = v
            if d.get("grievance_id"):
                payload.append(d)

        if not payload:
            return 0

        # Delete existing rows for this source (so import is an exact “feed” snapshot).
        src = str(source_override) if source_override else str(df["source_raw_filename"].iloc[0])
        if src:
            db.query(GrievanceProcessed).filter(GrievanceProcessed.source_raw_filename == src).delete()
            db.commit()

        # Chunked upsert
        chunk_size = 200
        for i in range(0, len(payload), chunk_size):
            chunk = payload[i : i + chunk_size]
            stmt = sqlite_insert(GrievanceProcessed).values(chunk)
            update_cols = {c.name: getattr(stmt.excluded, c.name) for c in GrievanceProcessed.__table__.columns if c.name != "grievance_id"}
            stmt = stmt.on_conflict_do_update(index_elements=["grievance_id"], set_=update_cols)
            db.execute(stmt)
            db.commit()

        return len(payload)

    def backfill_ai_fields_from_source(
        self,
        db: Session,
        *,
        target_source: str,
        from_source: str,
        join_key: str = "raw_id",
    ) -> int:
        """
        Copy AI output fields from one processed dataset source to another, matching on a stable join key.

        Why: We often ingest the same underlying grievances into multiple dataset sources
        (e.g., a raw4 Excel dump vs a staged dataset). If one source has already been enriched,
        we can backfill AI outputs into the other source without rerunning Gemini.

        Idempotent:
        - Only updates rows where target.ai_subtopic is empty (i.e., not yet enriched).
        - Only updates rows where the source row has at least ai_category or ai_subtopic populated.
        """
        tgt = str(target_source or "").strip()
        src = str(from_source or "").strip()
        key = str(join_key or "").strip()
        if not tgt or not src:
            raise ValueError("target_source and from_source are required")
        if key not in ("raw_id", "grievance_code"):
            raise ValueError("join_key must be 'raw_id' or 'grievance_code'")

        # Use a CTE to keep this fast in SQLite (single statement).
        sql = f"""
WITH src AS (
  SELECT
    {key} AS join_key,
    ai_category,
    ai_subtopic,
    ai_confidence,
    ai_issue_type,
    ai_entities_json,
    ai_urgency,
    ai_sentiment,
    ai_resolution_quality,
    ai_reopen_risk,
    ai_feedback_driver,
    ai_closure_theme,
    ai_extra_summary,
    ai_model,
    ai_run_timestamp,
    ai_error,
    actionable_score
  FROM grievances_processed
  WHERE source_raw_filename = :from_src
    AND {key} IS NOT NULL
)
UPDATE grievances_processed
SET
  ai_category = (SELECT ai_category FROM src WHERE src.join_key = grievances_processed.{key}),
  ai_subtopic = (SELECT ai_subtopic FROM src WHERE src.join_key = grievances_processed.{key}),
  ai_confidence = (SELECT ai_confidence FROM src WHERE src.join_key = grievances_processed.{key}),
  ai_issue_type = (SELECT ai_issue_type FROM src WHERE src.join_key = grievances_processed.{key}),
  ai_entities_json = (SELECT ai_entities_json FROM src WHERE src.join_key = grievances_processed.{key}),
  ai_urgency = (SELECT ai_urgency FROM src WHERE src.join_key = grievances_processed.{key}),
  ai_sentiment = (SELECT ai_sentiment FROM src WHERE src.join_key = grievances_processed.{key}),
  ai_resolution_quality = (SELECT ai_resolution_quality FROM src WHERE src.join_key = grievances_processed.{key}),
  ai_reopen_risk = (SELECT ai_reopen_risk FROM src WHERE src.join_key = grievances_processed.{key}),
  ai_feedback_driver = (SELECT ai_feedback_driver FROM src WHERE src.join_key = grievances_processed.{key}),
  ai_closure_theme = (SELECT ai_closure_theme FROM src WHERE src.join_key = grievances_processed.{key}),
  ai_extra_summary = (SELECT ai_extra_summary FROM src WHERE src.join_key = grievances_processed.{key}),
  ai_model = (SELECT ai_model FROM src WHERE src.join_key = grievances_processed.{key}),
  ai_run_timestamp = (SELECT ai_run_timestamp FROM src WHERE src.join_key = grievances_processed.{key}),
  ai_error = (SELECT ai_error FROM src WHERE src.join_key = grievances_processed.{key}),
  actionable_score = (SELECT actionable_score FROM src WHERE src.join_key = grievances_processed.{key})
WHERE source_raw_filename = :to_src
  AND {key} IS NOT NULL
  AND (ai_subtopic IS NULL OR trim(ai_subtopic) = '')
  AND {key} IN (
    SELECT join_key FROM src
    WHERE (ai_subtopic IS NOT NULL AND trim(ai_subtopic) != '')
       OR (ai_category IS NOT NULL AND trim(ai_category) != '')
  );
"""
        res = db.execute(text(sql), {"from_src": src, "to_src": tgt})
        db.commit()
        try:
            return int(res.rowcount or 0)
        except Exception:
            return 0

    # Backwards-compatible alias (older code paths)
    def build_run1_sample(self, db: Session, *, source: str, sample_size: int = 100) -> str:
        return self.build_run_sample(db, source=source, sample_size=sample_size)


