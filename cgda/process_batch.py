#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python cgda/process_batch.py <batch_size> [raw_filename]")
        return 2

    batch_size = int(sys.argv[1])
    raw_filename = sys.argv[2] if len(sys.argv) >= 3 else None

    # Import from backend package (works regardless of current working directory)
    repo_root = Path(__file__).resolve().parent
    sys.path.insert(0, str((repo_root / "backend").resolve()))
    from services.batch_pipeline import process_batch  # type: ignore

    res = process_batch(batch_size, raw_dir="raw2", raw_filename=raw_filename)
    print(
        f"selected={res.selected} processed={res.processed} remaining={res.remaining} output={res.output_written}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


