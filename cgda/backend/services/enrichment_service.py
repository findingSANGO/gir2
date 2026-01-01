from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from config import settings
from models import EnrichmentCheckpoint, EnrichmentRun, GrievanceRaw, GrievanceStructured
from services.gemini_client import GeminiClient


REQUIRED_COLS = [
    "Grievance Id",
    "Created Date",
    "Reported by User Name",
    "Mobile No.",
    "Complaint Location",
    "Complaint Subject",
    "Complaint Description",
    "Current Status",
    "Current Department Name",
    "Ward Name",
    "Current User Name",
    "Closing Remark",
]


ALLOWED_CATEGORIES = [
    "Solid Waste Management",
    "Roads & Footpaths",
    "Water Supply",
    "Sewerage & Drainage",
    "Street Lighting",
    "Public Health & Sanitation",
    "Encroachment & Illegal Construction",
    "Property Tax & Revenue",
    "Parks & Public Spaces",
    "Traffic & Transport",
    "Noise / Nuisance",
    "Animal Control",
    "Other Civic Issues",
]


def _ensure_dirs() -> None:
    Path(settings.data_raw_dir).mkdir(parents=True, exist_ok=True)
    Path(settings.data_processed_dir).mkdir(parents=True, exist_ok=True)
    Path(settings.data_runs_dir).mkdir(parents=True, exist_ok=True)


def _norm_col(c: str) -> str:
    # Normalize aggressively to survive Excel oddities and punctuation differences:
    # "Mobile No." vs "Mobile No" etc.
    s = str(c or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _clean_text(s: str, *, max_chars: int = 2500) -> str:
    s = str(s or "")
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) > max_chars:
        s = s[:max_chars]
    return s


def _strip_cell_newlines(v: Any) -> Any:
    if isinstance(v, str):
        return re.sub(r"[\r\n]+", " ", v).strip()
    return v


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class LatestRawFile:
    path: str
    filename: str
    mtime_iso: str


class EnrichmentService:
    """
    Excel/CSV → AI enrichment → analytics-ready dataset pipeline.
    Resume is handled via EnrichmentCheckpoint (grievance_key + ai_input_hash).
    """

    def __init__(self) -> None:
        _ensure_dirs()
        self.gemini = GeminiClient()

    def detect_latest_raw_file(self) -> LatestRawFile | None:
        raw_dir = Path(settings.data_raw_dir)
        # IMPORTANT: data/raw is "immutable inputs", but users sometimes accidentally copy
        # our generated artifacts into this folder. Ignore anything that looks like an output.
        def _is_valid_input(p: Path) -> bool:
            name = p.name.lower()
            if name.endswith("grievances_enriched.csv"):
                return False
            if name.endswith("preprocessed_latest.csv"):
                return False
            if "_preprocessed" in name:
                return False
            if "grievances_enriched" in name:
                return False
            if "preprocessed" in name:
                return False
            return True

        candidates: list[Path] = []
        for ext in (".xlsx", ".xls", ".csv"):
            candidates += [p for p in raw_dir.glob(f"*{ext}") if _is_valid_input(p)]
        if not candidates:
            return None
        latest = max(candidates, key=lambda p: p.stat().st_mtime)
        return LatestRawFile(
            path=str(latest),
            filename=latest.name,
            mtime_iso=dt.datetime.utcfromtimestamp(latest.stat().st_mtime).isoformat() + "Z",
        )

    def get_raw_file_by_name(self, filename: str) -> LatestRawFile:
        raw_dir = Path(settings.data_raw_dir)
        p = raw_dir / filename
        if not p.exists():
            raise ValueError(f"Raw file not found in data/raw: {filename}")
        if p.suffix.lower() not in (".xlsx", ".xls", ".csv"):
            raise ValueError("Raw file must be .xlsx, .xls, or .csv")
        return LatestRawFile(
            path=str(p),
            filename=p.name,
            mtime_iso=dt.datetime.utcfromtimestamp(p.stat().st_mtime).isoformat() + "Z",
        )

    def load_raw_dataframe(self, path: str) -> pd.DataFrame:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(path)
        if p.suffix.lower() in (".xlsx", ".xls"):
            df = self._read_excel_smart(p)
        else:
            df = pd.read_csv(p)
        return df

    def _read_excel_smart(self, p: Path) -> pd.DataFrame:
        """
        Many NMMC Excel exports have a title row (e.g., 'Grievance Report') and the real header
        appears a few rows below. This function scans for the header row by matching REQUIRED_COLS.
        """
        xl = pd.ExcelFile(p)
        best: tuple[int, str, int] | None = None  # (hits, sheet, header_row)
        for sheet in xl.sheet_names:
            preview = pd.read_excel(p, sheet_name=sheet, header=None, nrows=60)
            for i in range(min(len(preview), 40)):
                cells = [_norm_col(x) for x in preview.iloc[i].tolist()]
                hits = sum(1 for req in REQUIRED_COLS if _norm_col(req) in cells)
                if hits >= 5:
                    if not best or hits > best[0]:
                        best = (hits, sheet, i)
        if best:
            _, sheet, header_row = best
            df = pd.read_excel(p, sheet_name=sheet, header=header_row)
            # Keep metadata for debugging
            df.attrs["__sheet_name__"] = sheet
            df.attrs["__header_row__"] = header_row
            return df
        # Fallback: use default header row
        return pd.read_excel(p)

    def preprocess_raw_to_file(self, *, raw_path: str, run_id: str, row_limit: int | None = None) -> str:
        """
        RAW → Preprocessed CSV (immutable raw file is never modified).
        Downstream steps MUST read ONLY the preprocessed file.
        """
        print(f"[PIPELINE] Reading RAW file from: {raw_path}")
        df = self.load_raw_dataframe(raw_path)
        if row_limit is not None and int(row_limit) > 0:
            df = df.head(int(row_limit))
        mapping = self._map_columns(df)

        grievance_ids = df[mapping["Grievance Id"]].astype(str).str.strip()
        subjects = df[mapping["Complaint Subject"]].astype(str)
        descs = df[mapping["Complaint Description"]].astype(str)

        ai_input_texts = [self._build_ai_input_text(s, d) for s, d in zip(subjects, descs)]
        ai_hashes = [_sha256(t) for t in ai_input_texts]

        # INPUT DATASET contains original columns + stable keys + sanitized AI input.
        # This is the explicit "dataset we pass to the code" for enrichment/analytics.
        out_df = df.copy()
        # Make the CSV “1 record = 1 row” by stripping embedded newlines from ALL string cells.
        out_df = out_df.applymap(_strip_cell_newlines)
        out_df["grievance_key"] = grievance_ids
        out_df["AI_Input_Text"] = ai_input_texts
        out_df["AI_InputHash"] = ai_hashes

        # Normalize created date into a safe ISO string for downstream parsing.
        created_col = mapping["Created Date"]
        created_dt = pd.to_datetime(out_df[created_col], errors="coerce")
        out_df["Created_Date_ISO"] = created_dt.dt.strftime("%Y-%m-%d").fillna("")

        # Write per-run artifact + stable "latest input dataset" pointer
        per_run = Path(settings.data_runs_dir) / f"{run_id}_input_dataset.csv"
        latest = Path(settings.data_processed_dir) / "input_dataset_latest.csv"
        out_df.to_csv(per_run, index=False)
        out_df.to_csv(latest, index=False)

        print(f"[PIPELINE] Wrote INPUT DATASET file to: {per_run}")
        print(f"[PIPELINE] Updated INPUT DATASET latest pointer: {latest}")
        return str(per_run)

    def _map_columns(self, df: pd.DataFrame) -> dict[str, str]:
        # normalized -> original
        norm_map = {_norm_col(c): c for c in df.columns}

        def pick(name: str) -> str:
            key = _norm_col(name)
            if key in norm_map:
                return norm_map[key]
            # Try a loose match (trim)
            for k, v in norm_map.items():
                if k == key:
                    return v
            raise KeyError(name)

        missing: list[str] = []
        out: dict[str, str] = {}
        for col in REQUIRED_COLS:
            try:
                out[col] = pick(col)
            except KeyError:
                missing.append(col)

        if missing:
            found = list(map(str, df.columns))
            raise ValueError(
                "Input file schema mismatch.\n"
                f"Missing required columns: {missing}\n"
                f"Found columns: {found}\n"
                "Please place the NMMC/IES export (with the exact required headers) into data/raw/."
            )

        return out

    def _build_ai_input_text(self, subject: Any, description: Any) -> str:
        subj = str(subject or "").strip()
        desc = str(description or "").strip()
        combined = (subj + "\n" + desc).strip()
        return _clean_text(combined, max_chars=2500)

    def _category_sanitize(self, v: str) -> str:
        v = (v or "").strip()
        if v not in ALLOWED_CATEGORIES:
            return "Other Civic Issues"
        return v

    def _subtopic_sanitize(self, v: str) -> str:
        v = re.sub(r"\s+", " ", (v or "").strip())
        if not v:
            return "General Civic Issue"
        # Enforce 2–4 words preferred; if too long, truncate to 4.
        parts = [p for p in v.split(" ") if p]
        if len(parts) > 4:
            parts = parts[:4]
        # Title Case (simple)
        return " ".join(w[:1].upper() + w[1:] for w in parts)

    def _confidence_sanitize(self, v: str) -> str:
        v = (v or "").strip().capitalize()
        return v if v in ("High", "Medium", "Low") else "Low"

    def _batch_call(
        self, *, prompt_name: str, input_records: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], str, dict[str, int | None]]:
        prompt_tpl = (Path(__file__).resolve().parent.parent / "prompts" / prompt_name).read_text(encoding="utf-8")
        prompt = prompt_tpl.replace("{{INPUT_JSON}}", json.dumps(input_records, ensure_ascii=False))
        # Classification prompts MUST be JSON-only and low temperature.
        # IMPORTANT: batch outputs can exceed small defaults like 256 tokens (would truncate JSON).
        # Size output tokens conservatively based on batch size; still bounded.
        dynamic_max = max(int(settings.gemini_max_output_tokens), len(input_records) * 200, 800)
        dynamic_max = min(dynamic_max, 4096)
        res = self.gemini.generate_json(
            prompt=prompt,
            temperature=min(0.2, settings.gemini_temperature),
            max_output_tokens=dynamic_max,
            expect="list",
        )
        if not res.ok or not isinstance(res.parsed_json, list):
            raise RuntimeError(res.error or "Gemini batch failed")
        usage = {"prompt_tokens": res.prompt_tokens, "output_tokens": res.output_tokens, "total_tokens": res.total_tokens}
        return res.parsed_json, res.model_used, usage

    def _label_batch(self, batch: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str, dict[str, int | None]]:
        """
        Two-step labeling:
        A) category_prompt.txt
        B) subtopic_prompt.txt (takes category as input)
        Returns: list of {category, sub_topic, confidence} aligned to batch order + model used.
        """
        if not settings.gemini_api_key:
            raise RuntimeError("GEMINI_API_KEY is not configured")

        input_a = [{"text": r["ai_input_text"]} for r in batch]
        # Step A: category (GeminiClient handles primary/fallback + retries)
        out_a, model_a, usage_a = self._batch_call(prompt_name="category_prompt.txt", input_records=input_a)
        if len(out_a) != len(batch):
            raise ValueError("Category output length mismatch")
        last_model_used = model_a

        # Step B: subtopic (provide category + text)
        input_b = []
        for i, r in enumerate(batch):
            cat = self._category_sanitize(str(out_a[i].get("category", "")))
            input_b.append({"text": r["ai_input_text"], "category": cat})

        out_b, model_b, usage_b = self._batch_call(prompt_name="subtopic_prompt.txt", input_records=input_b)
        if len(out_b) != len(batch):
            raise ValueError("Subtopic output length mismatch")
        last_model_used = model_b

        usage = {
            "prompt_tokens": (usage_a.get("prompt_tokens") or 0) + (usage_b.get("prompt_tokens") or 0),
            "output_tokens": (usage_a.get("output_tokens") or 0) + (usage_b.get("output_tokens") or 0),
            "total_tokens": (usage_a.get("total_tokens") or 0) + (usage_b.get("total_tokens") or 0),
        }

        merged: list[dict[str, Any]] = []
        for i in range(len(batch)):
            cat = self._category_sanitize(str(out_b[i].get("category", input_b[i]["category"])))
            sub = self._subtopic_sanitize(str(out_b[i].get("sub_topic", "")))
            conf = self._confidence_sanitize(str(out_b[i].get("confidence", "")))
            merged.append({"category": cat, "sub_topic": sub, "confidence": conf})
        return merged, last_model_used, usage

    def start_run_background(
        self,
        db: Session,
        *,
        row_limit: int | None = None,
        raw_filename: str | None = None,
        reset_analytics: bool = False,
        force_reprocess: bool = False,
    ) -> str:
        latest = self.get_raw_file_by_name(raw_filename) if raw_filename else self.detect_latest_raw_file()
        if not latest:
            raise ValueError(f"No .xlsx/.csv files found in {settings.data_raw_dir} (or inputs were filtered out)")

        run_id = f"run_{dt.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
        run = EnrichmentRun(
            run_id=run_id,
            raw_filename=latest.filename,
            raw_path=latest.path,
            status="queued",
            summary_json=json.dumps(
                {
                    "row_limit": int(row_limit) if row_limit else None,
                    "reset_analytics": bool(reset_analytics),
                    "force_reprocess": bool(force_reprocess),
                }
            ),
        )
        db.add(run)
        db.commit()

        t = threading.Thread(
            target=self._run_ingest_and_enrich,
            args=(run_id, row_limit, reset_analytics, force_reprocess),
            daemon=True,
            name=f"enrich-{run_id}",
        )
        t.start()
        return run_id

    def _run_ingest_and_enrich(
        self, run_id: str, row_limit: int | None = None, reset_analytics: bool = False, force_reprocess: bool = False
    ) -> None:
        from database import session_scope

        with session_scope() as db:
            run = db.execute(select(EnrichmentRun).where(EnrichmentRun.run_id == run_id)).scalar_one()
            run.status = "running"
            run.started_at = dt.datetime.utcnow()
            db.commit()

        try:
            with session_scope() as db:
                self._ingest_and_enrich(
                    db, run_id, row_limit=row_limit, reset_analytics=reset_analytics, force_reprocess=force_reprocess
                )
            with session_scope() as db:
                run = db.execute(select(EnrichmentRun).where(EnrichmentRun.run_id == run_id)).scalar_one()
                run.status = "completed"
                run.finished_at = dt.datetime.utcnow()
                db.commit()
        except Exception as e:
            with session_scope() as db:
                run = db.execute(select(EnrichmentRun).where(EnrichmentRun.run_id == run_id)).scalar_one()
                run.status = "failed"
                run.error = f"{type(e).__name__}: {e}"
                run.finished_at = dt.datetime.utcnow()
                db.commit()

    def _ingest_and_enrich(
        self,
        db: Session,
        run_id: str,
        *,
        row_limit: int | None = None,
        reset_analytics: bool = False,
        force_reprocess: bool = False,
    ) -> None:
        run = db.execute(select(EnrichmentRun).where(EnrichmentRun.run_id == run_id)).scalar_one()
        # Step 1: Build an explicit INPUT DATASET (downstream reads ONLY this dataset)
        preprocessed_path = self.preprocess_raw_to_file(raw_path=run.raw_path, run_id=run_id, row_limit=row_limit)
        run.preprocessed_path = preprocessed_path
        db.commit()

        # Step 2: Load INPUT DATASET for enrichment (do NOT read raw again downstream)
        print(f"[PIPELINE] Reading INPUT DATASET for enrichment from: {preprocessed_path}")
        df = pd.read_csv(preprocessed_path)
        # Use exact column headers from the preprocessed file (which still contains the original NMMC columns)
        mapping = self._map_columns(df)

        # Build canonical fields
        grievance_ids = df[mapping["Grievance Id"]].astype(str).str.strip()
        subjects = df[mapping["Complaint Subject"]].astype(str)
        descs = df[mapping["Complaint Description"]].astype(str)
        wards = df[mapping["Ward Name"]].astype(str)
        depts = df[mapping["Current Department Name"]].astype(str)
        created = df.get("Created_Date_ISO", df[mapping["Created Date"]])
        status = df[mapping["Current Status"]].astype(str)

        # Canonical AI input
        # Prefer the already-sanitized preprocessed fields (guarantees downstream reads only preprocessed)
        ai_input_texts = df.get("AI_Input_Text", pd.Series([""] * len(df))).astype(str).tolist()
        ai_hashes = df.get("AI_InputHash", pd.Series([""] * len(df))).astype(str).tolist()
        if not any(ai_hashes):
            ai_input_texts = [self._build_ai_input_text(s, d) for s, d in zip(subjects, descs)]
            ai_hashes = [_sha256(t) for t in ai_input_texts]

        run.total_rows = int(len(df))
        db.commit()

        processed = 0
        skipped = 0
        failed = 0
        token_usage_total = {"prompt_tokens": 0, "output_tokens": 0, "total_tokens": 0}
        token_usage_by_model: dict[str, dict[str, int]] = {}

        # Optionally reset analytics tables so dashboards reflect only this input dataset.
        if reset_analytics:
            print("[PIPELINE] Resetting grievances_raw/grievances_structured for a clean 100-row dashboard dataset.")
            db.query(GrievanceStructured).delete()
            db.query(GrievanceRaw).delete()
            db.commit()

        # Upsert GrievanceRaw for analytics (PII is NOT sent to Gemini; DB storage is local-only).
        # We only store the minimal canonical fields + raw row JSON.
        existing_raw_ids = {
            gid for (gid,) in db.execute(select(GrievanceRaw.grievance_id)).all() if gid
        }
        new_raw = []
        for i in range(len(df)):
            gid = str(grievance_ids.iloc[i]).strip()
            if not gid or gid.lower() == "nan":
                continue
            if gid in existing_raw_ids:
                continue
            try:
                created_dt = pd.to_datetime(created.iloc[i], errors="coerce")
                created_date = created_dt.date() if pd.notna(created_dt) else None
            except Exception:
                created_date = None

            row_payload = df.iloc[i].to_dict()
            new_raw.append(
                GrievanceRaw(
                    grievance_id=gid,
                    created_date=created_date,
                    closed_date=None,
                    ward=(str(wards.iloc[i]).strip() or None),
                    department=(str(depts.iloc[i]).strip() or None),
                    feedback_star=None,
                    grievance_text=ai_input_texts[i],
                    raw_payload_json=json.dumps(row_payload, ensure_ascii=False),
                )
            )
        if new_raw:
            db.bulk_save_objects(new_raw)
            db.commit()

        # Build a fast lookup for checkpoints to support resume without per-row queries.
        existing_cp = {
            c.grievance_key: c
            for c in db.execute(select(EnrichmentCheckpoint)).scalars().all()
        }

        # Prepare per-row work list with resume logic
        work: list[dict[str, Any]] = []
        for i in range(len(df)):
            key = str(grievance_ids.iloc[i]).strip()
            if not key or key.lower() == "nan":
                continue

            ih = ai_hashes[i]
            existing = existing_cp.get(key)
            if (not force_reprocess) and existing and existing.ai_input_hash == ih and not existing.ai_error:
                skipped += 1
                continue

            work.append(
                {
                    "row_index": i,
                    "grievance_key": key,
                    "ai_input_text": ai_input_texts[i],
                    "ai_input_hash": ih,
                }
            )

        # Process in batches of 10
        batch_size = 10
        for start in range(0, len(work), batch_size):
            batch = work[start : start + batch_size]
            # Label batch; if batch fails, mark each item failed and continue
            try:
                labeled, model_used, usage = self._label_batch(batch)
                if usage:
                    for k in ("prompt_tokens", "output_tokens", "total_tokens"):
                        if usage.get(k) is not None:
                            token_usage_total[k] += int(usage.get(k) or 0)
                    if model_used:
                        m = token_usage_by_model.setdefault(model_used, {"prompt_tokens": 0, "output_tokens": 0, "total_tokens": 0})
                        for k in ("prompt_tokens", "output_tokens", "total_tokens"):
                            if usage.get(k) is not None:
                                m[k] += int(usage.get(k) or 0)
            except Exception as e:
                msg = f"{type(e).__name__}: {e}"
                for item in batch:
                    # If we're forcing reprocess but Gemini fails, do NOT overwrite an existing
                    # successful checkpoint (preserve last-known-good labels).
                    prev = existing_cp.get(item["grievance_key"])
                    if force_reprocess and prev and not prev.ai_error:
                        skipped += 1
                        continue
                    cp = EnrichmentCheckpoint(
                        grievance_key=item["grievance_key"],
                        ai_input_hash=item["ai_input_hash"],
                        ai_category="Other Civic Issues",
                        ai_subtopic="General Civic Issue",
                        ai_confidence="Low",
                        ai_model=settings.gemini_model_fallback,
                        ai_run_timestamp=dt.datetime.utcnow(),
                        ai_error=msg,
                    )
                    existing = db.execute(
                        select(EnrichmentCheckpoint).where(EnrichmentCheckpoint.grievance_key == item["grievance_key"])
                    ).scalar_one_or_none()
                    if existing:
                        existing.ai_input_hash = cp.ai_input_hash
                        existing.ai_category = cp.ai_category
                        existing.ai_subtopic = cp.ai_subtopic
                        existing.ai_confidence = cp.ai_confidence
                        existing.ai_model = cp.ai_model
                        existing.ai_run_timestamp = cp.ai_run_timestamp
                        existing.ai_error = cp.ai_error
                    else:
                        db.add(cp)
                    failed += 1
                db.commit()
                self._update_run_progress(db, run_id, processed, skipped, failed)
                continue

            # Write checkpoints + also upsert into grievance_raw/structured for analytics pages
            for i, item in enumerate(batch):
                out = labeled[i]
                cat = self._category_sanitize(out["category"])
                sub = self._subtopic_sanitize(out["sub_topic"])
                conf = self._confidence_sanitize(out["confidence"])

                existing = existing_cp.get(item["grievance_key"])
                if existing:
                    existing.ai_input_hash = item["ai_input_hash"]
                    existing.ai_category = cat
                    existing.ai_subtopic = sub
                    existing.ai_confidence = conf
                    existing.ai_model = model_used
                    existing.ai_run_timestamp = dt.datetime.utcnow()
                    existing.ai_error = None
                else:
                    existing = EnrichmentCheckpoint(
                        grievance_key=item["grievance_key"],
                        ai_input_hash=item["ai_input_hash"],
                        ai_category=cat,
                        ai_subtopic=sub,
                        ai_confidence=conf,
                        ai_model=model_used,
                        ai_run_timestamp=dt.datetime.utcnow(),
                        ai_error=None,
                    )
                    db.add(existing)
                    existing_cp[item["grievance_key"]] = existing

                processed += 1

            db.commit()
            self._update_run_progress(db, run_id, processed, skipped, failed)

        # Regenerate enriched CSV
        self._write_enriched_csv(db, run_id, df, mapping, grievance_ids, ai_input_texts, ai_hashes)
        run.enriched_path = str(Path(settings.data_processed_dir) / "grievances_enriched.csv")
        db.commit()
        print(f"[PIPELINE] Wrote ENRICHED output file to: {run.enriched_path}")

        # Sync checkpointed labels into grievances_structured so existing dashboards use AI_Category/AI_SubTopic.
        self._sync_structured_from_checkpoints(db)

        # Final summary counts
        summary = self._summary_from_checkpoints(db)
        summary["token_usage"] = {
            "prompt_tokens": int(token_usage_total["prompt_tokens"]),
            "output_tokens": int(token_usage_total["output_tokens"]),
            "total_tokens": int(token_usage_total["total_tokens"]),
            "by_model": token_usage_by_model,
            "note": "Token usage counts are reported by Gemini usageMetadata (only for calls made in this run).",
        }
        run.summary_json = json.dumps(summary, ensure_ascii=False)
        run.processed = processed
        run.skipped = skipped
        run.failed = failed
        db.commit()

        # Write a human-friendly run artifact under data/runs/ (optional inspection)
        try:
            Path(settings.data_runs_dir).mkdir(parents=True, exist_ok=True)
            (Path(settings.data_runs_dir) / f"{run_id}_summary.json").write_text(
                json.dumps(
                    {
                        "run_id": run_id,
                        "raw_filename": run.raw_filename,
                        "total_rows": run.total_rows,
                        "processed": run.processed,
                        "skipped": run.skipped,
                        "failed": run.failed,
                        "summary": summary,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        except Exception:
            pass

    def _summary_from_checkpoints(self, db: Session) -> dict[str, Any]:
        rows = db.execute(select(EnrichmentCheckpoint.ai_category, EnrichmentCheckpoint.ai_subtopic)).all()
        cat_counts: dict[str, int] = {}
        sub_counts: dict[str, int] = {}
        for cat, sub in rows:
            cat_counts[cat] = cat_counts.get(cat, 0) + 1
            sub_counts[sub] = sub_counts.get(sub, 0) + 1
        top_cats = sorted(cat_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        top_subs = sorted(sub_counts.items(), key=lambda x: x[1], reverse=True)[:15]
        return {"top_categories": top_cats, "top_subtopics": top_subs, "checkpoint_rows": len(rows)}

    def _update_run_progress(self, db: Session, run_id: str, processed: int, skipped: int, failed: int) -> None:
        run = db.execute(select(EnrichmentRun).where(EnrichmentRun.run_id == run_id)).scalar_one()
        run.processed = processed
        run.skipped = skipped
        run.failed = failed
        db.commit()

    def _write_enriched_csv(
        self,
        db: Session,
        run_id: str,
        df: pd.DataFrame,
        mapping: dict[str, str],
        grievance_ids: pd.Series,
        ai_input_texts: list[str],
        ai_hashes: list[str],
    ) -> None:
        # Merge checkpoint outputs back onto original df without modifying it.
        key_series = grievance_ids.astype(str).str.strip()
        cp_rows = db.execute(select(EnrichmentCheckpoint)).scalars().all()
        cp_map = {c.grievance_key: c for c in cp_rows}

        out_df = df.copy()
        out_df = out_df.applymap(_strip_cell_newlines)
        out_df["grievance_key"] = key_series
        out_df["AI_Input_Text"] = ai_input_texts
        out_df["AI_InputHash"] = ai_hashes

        out_df["AI_Category"] = [cp_map.get(k).ai_category if cp_map.get(k) else "" for k in out_df["grievance_key"]]
        out_df["AI_SubTopic"] = [cp_map.get(k).ai_subtopic if cp_map.get(k) else "" for k in out_df["grievance_key"]]
        out_df["AI_Confidence"] = [cp_map.get(k).ai_confidence if cp_map.get(k) else "" for k in out_df["grievance_key"]]
        out_df["AI_Model"] = [cp_map.get(k).ai_model if cp_map.get(k) else "" for k in out_df["grievance_key"]]
        out_df["AI_RunTimestamp"] = [
            (cp_map.get(k).ai_run_timestamp.isoformat() if cp_map.get(k) else "") for k in out_df["grievance_key"]
        ]
        out_df["AI_Error"] = [cp_map.get(k).ai_error if cp_map.get(k) else "" for k in out_df["grievance_key"]]

        dest = Path(settings.data_processed_dir) / "grievances_enriched.csv"
        out_df.to_csv(dest, index=False)
        print(f"[PIPELINE] Enriched CSV written to: {dest}")

    def _sync_structured_from_checkpoints(self, db: Session) -> None:
        """
        Create/update GrievanceStructured rows using AI_Category and AI_SubTopic.
        Other AI fields are set to "Unknown" (this pipeline focuses on category/subtopic).
        """
        # Map grievance_id -> raw_id
        raw_map = {gid: rid for (gid, rid) in db.execute(select(GrievanceRaw.grievance_id, GrievanceRaw.id)).all()}
        cps = db.execute(select(EnrichmentCheckpoint).where(EnrichmentCheckpoint.ai_error.is_(None))).scalars().all()

        # Existing structured by raw_id
        existing_struct = {
            raw_id: s
            for s in db.execute(select(GrievanceStructured)).scalars().all()
            for raw_id in [s.raw_id]
        }

        to_add = []
        for cp in cps:
            raw_id = raw_map.get(cp.grievance_key)
            if not raw_id:
                continue
            s = existing_struct.get(raw_id)
            if s:
                s.category = cp.ai_category
                s.sub_issue = cp.ai_subtopic
                s.sentiment = "Unknown"
                s.severity = "Unknown"
                s.repeat_flag = False
                s.delay_risk = "Unknown"
                s.dissatisfaction_reason = None
                s.ai_rationale = f"Category/SubTopic enrichment pipeline. Confidence={cp.ai_confidence}."
                s.ai_provider = "caseA"
                s.ai_engine = "Gemini"
                s.ai_model = cp.ai_model
                s.ai_version = "v-enrich-1"
                s.processed_at = cp.ai_run_timestamp
                s.is_mock = False
            else:
                to_add.append(
                    GrievanceStructured(
                        raw_id=raw_id,
                        category=cp.ai_category,
                        sub_issue=cp.ai_subtopic,
                        sentiment="Unknown",
                        severity="Unknown",
                        repeat_flag=False,
                        delay_risk="Unknown",
                        dissatisfaction_reason=None,
                        ai_rationale=f"Category/SubTopic enrichment pipeline. Confidence={cp.ai_confidence}.",
                        ai_provider="caseA",
                        ai_engine="Gemini",
                        ai_model=cp.ai_model,
                        ai_version="v-enrich-1",
                        processed_at=cp.ai_run_timestamp,
                        is_mock=False,
                    )
                )
        if to_add:
            db.bulk_save_objects(to_add)
        db.commit()

    def get_run(self, db: Session, run_id: str) -> EnrichmentRun:
        run = db.execute(select(EnrichmentRun).where(EnrichmentRun.run_id == run_id)).scalar_one_or_none()
        if not run:
            raise ValueError(f"Run not found: {run_id}")
        return run

    def list_runs(self, db: Session, limit: int = 30) -> list[EnrichmentRun]:
        return db.execute(select(EnrichmentRun).order_by(EnrichmentRun.started_at.desc()).limit(limit)).scalars().all()


