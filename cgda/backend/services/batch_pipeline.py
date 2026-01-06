from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from config import settings
from services.enrichment_service import EnrichmentService


def _snake(s: str) -> str:
    s = str(s or "").strip()
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s.lower()


def _clean_text(v: Any) -> Any:
    if v is None:
        return None
    if not isinstance(v, str):
        return v
    s = v.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _load_processed_ids(path: str) -> set[str]:
    if not os.path.exists(path):
        _ensure_dir(os.path.dirname(path))
        with open(path, "w", encoding="utf-8") as f:
            f.write("[]")
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            arr = json.load(f)
        return set(str(x) for x in (arr or []) if str(x).strip())
    except Exception:
        # Fail loudly per spec
        raise RuntimeError(f"processed_ids.json is unreadable: {path}")


def _write_processed_ids(path: str, ids: set[str]) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sorted(ids), f, ensure_ascii=False, indent=2)


def _preprocessed_master_paths() -> tuple[str, str]:
    base = settings.data_processed_dir
    return (
        os.path.join(base, "preprocessed_master.parquet"),
        os.path.join(base, "preprocessed_master.csv"),
    )


def _gemini_results_paths() -> tuple[str, str]:
    base = settings.data_outputs_dir
    return (
        os.path.join(base, "gemini_results.parquet"),
        os.path.join(base, "gemini_results.csv"),
    )


def _read_master() -> pd.DataFrame:
    parquet_path, csv_path = _preprocessed_master_paths()
    if os.path.exists(parquet_path):
        try:
            return pd.read_parquet(parquet_path)
        except Exception:
            # fall back to csv if parquet reader isn't available
            pass
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)
    raise RuntimeError("preprocessed_master not found. Run preprocess_master() first.")


def preprocess_master(
    *,
    raw_dir: str = "raw2",
    raw_filename: str | None = None,
    id_column: str | None = None,
) -> dict:
    """
    STEP 1-2:
    - Load raw file from data/raw2
    - Clean text fields, standardize column names
    - Deduplicate strictly by grievance_id (derived from raw id_column if needed)
    - Save to /data/processed/preprocessed_master.(parquet|csv)
    """
    svc = EnrichmentService()
    latest = svc.get_raw_file_by_name(raw_filename, raw_dir=raw_dir) if raw_filename else svc.detect_latest_raw_file(raw_dir=raw_dir)
    if not latest:
        raise RuntimeError(f"No raw file found in {raw_dir}")

    df = svc.load_raw_dataframe(latest.path)
    if df is None or df.empty:
        raise RuntimeError(f"Raw file is empty or unreadable: {latest.path}")

    # Standardize column names
    df = df.rename(columns={c: _snake(c) for c in df.columns})

    # Clean text columns
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].apply(_clean_text)

    # Determine id column: prefer explicit arg, then env, then grievance_id, then id
    chosen = (id_column or os.getenv("ID_COLUMN") or "").strip().lower() or None
    if chosen and chosen in df.columns and chosen != "grievance_id":
        df = df.rename(columns={chosen: "grievance_id"})

    # Common case: raw has `id` not `grievance_id`
    if "grievance_id" not in df.columns and "id" in df.columns:
        df = df.rename(columns={"id": "grievance_id"})

    # Fail loudly if still missing
    if "grievance_id" not in df.columns:
        raise RuntimeError(
            "Missing required id column. Expected `grievance_id` (or configure ID_COLUMN / pass id_column). "
            f"Found columns: {list(df.columns)[:30]}"
        )

    df["grievance_id"] = df["grievance_id"].astype(str).str.strip()
    df = df[df["grievance_id"].notna() & (df["grievance_id"] != "")]

    before = int(len(df))
    df = df.drop_duplicates(subset=["grievance_id"], keep="first").reset_index(drop=True)
    after = int(len(df))

    # Save master
    parquet_path, csv_path = _preprocessed_master_paths()
    _ensure_dir(os.path.dirname(csv_path))

    # Prefer parquet if available; else CSV.
    wrote = "csv"
    try:
        df.to_parquet(parquet_path, index=False)
        wrote = "parquet"
    except Exception:
        df.to_csv(csv_path, index=False)

    # Ensure processed_ids.json exists
    processed_ids_path = os.path.join(settings.data_processed_dir, "processed_ids.json")
    _load_processed_ids(processed_ids_path)

    return {
        "raw_file": Path(latest.path).name,
        "raw_path": latest.path,
        "rows_loaded": before,
        "rows_deduped": after,
        "written": wrote,
        "preprocessed_master_parquet": parquet_path,
        "preprocessed_master_csv": csv_path,
        "processed_ids_json": processed_ids_path,
    }


@dataclass(frozen=True)
class BatchResult:
    selected: int
    processed: int
    skipped: int
    remaining: int
    output_written: str


def process_batch(batch_size: int, *, raw_dir: str = "raw2", raw_filename: str | None = None) -> BatchResult:
    """
    STEP 3-6:
    - Load preprocessed_master
    - Exclude grievance_id in processed_ids.json
    - Select next batch_size
    - Send batch to Gemini (via existing EnrichmentService logic)
    - Append results to /data/outputs/gemini_results.(parquet|csv)
    - Update processed_ids.json for successfully processed records
    """
    batch_size = int(batch_size or 0)
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    # Ensure master exists
    try:
        master = _read_master()
    except Exception:
        preprocess_master(raw_dir=raw_dir, raw_filename=raw_filename)
        master = _read_master()

    processed_ids_path = os.path.join(settings.data_processed_dir, "processed_ids.json")
    processed_ids = _load_processed_ids(processed_ids_path)

    if "grievance_id" not in master.columns:
        raise RuntimeError("preprocessed_master is missing grievance_id column")

    master["grievance_id"] = master["grievance_id"].astype(str).str.strip()
    master = master[master["grievance_id"].notna() & (master["grievance_id"] != "")]

    # Deterministic order
    master = master.sort_values("grievance_id", kind="stable").reset_index(drop=True)

    unprocessed = master[~master["grievance_id"].isin(processed_ids)]
    remaining = int(len(unprocessed))
    if remaining <= 0:
        return BatchResult(selected=0, processed=0, skipped=0, remaining=0, output_written="(no-op)")

    batch_df = unprocessed.head(batch_size).copy()
    selected = int(len(batch_df))

    # Prepare Gemini inputs (do NOT assume specific columns exist)
    def get_col(*names: str) -> Any:
        for n in names:
            if n in batch_df.columns:
                return batch_df[n]
        return None

    inputs: list[dict[str, Any]] = []
    for _, r in batch_df.iterrows():
        inputs.append(
            {
                "grievance_id": r.get("grievance_id"),
                "department": r.get("department") if "department" in batch_df.columns else r.get("department_name"),
                "status": r.get("status"),
                "subject": r.get("subject"),
                "description": r.get("description"),
                "closing_remark": r.get("closing_remark"),
                "rating": r.get("rating"),
                "resolution_days": r.get("resolution_days"),
                "forward_count": r.get("forward_count"),
            }
        )

    enr = EnrichmentService()
    # Gemini call (existing logic + prompt)
    out, model_used, usage = enr._ticket_enrich_batch(inputs)  # noqa: SLF001 (intentional reuse)

    now_iso = pd.Timestamp.utcnow().isoformat()
    rows_out = []
    ok_ids: set[str] = set()
    for i, r0 in enumerate(out):
        gid = str(inputs[i].get("grievance_id") or "").strip()
        rows_out.append(
            {
                "grievance_id": gid,
                **r0,
                "ai_model": model_used,
                "ai_run_timestamp": now_iso,
                "ai_error": None,
                "prompt_tokens": usage.get("prompt_tokens"),
                "output_tokens": usage.get("output_tokens"),
                "total_tokens": usage.get("total_tokens"),
            }
        )
        if gid:
            ok_ids.add(gid)

    # Persist outputs (append, de-dup by grievance_id)
    out_df = pd.DataFrame(rows_out)
    parquet_out, csv_out = _gemini_results_paths()
    _ensure_dir(os.path.dirname(parquet_out))

    wrote = "csv"
    try:
        if os.path.exists(parquet_out):
            prev = pd.read_parquet(parquet_out)
            merged = pd.concat([prev, out_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["grievance_id"], keep="last")
            merged.to_parquet(parquet_out, index=False)
        else:
            out_df.to_parquet(parquet_out, index=False)
        wrote = parquet_out
    except Exception:
        # fallback to csv
        if os.path.exists(csv_out):
            prev = pd.read_csv(csv_out)
            merged = pd.concat([prev, out_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["grievance_id"], keep="last")
            merged.to_csv(csv_out, index=False)
        else:
            out_df.to_csv(csv_out, index=False)
        wrote = csv_out

    # Update state (only after we successfully wrote outputs)
    processed_ids |= ok_ids
    _write_processed_ids(processed_ids_path, processed_ids)

    remaining_after = int(len(master[~master["grievance_id"].isin(processed_ids)]))
    return BatchResult(selected=selected, processed=len(ok_ids), skipped=0, remaining=remaining_after, output_written=wrote)


