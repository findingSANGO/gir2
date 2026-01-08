from __future__ import annotations

import datetime as dt
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
import json

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError

from models import GrievanceRaw, GrievanceStructured, GrievanceProcessed
from services.ai_service import AIService
from config import settings
from services.gemini_client import GeminiClient


def _closure_days(created: dt.date | None, closed: dt.date | None) -> int | None:
    if not created or not closed:
        return None
    d = (closed - created).days
    return d if d >= 0 else None


def _bucket(days: int | None) -> str:
    if days is None:
        return "Unknown"
    if days < 7:
        return "<7"
    if days <= 14:
        return "7-14"
    return ">14"


@dataclass(frozen=True)
class Filters:
    start_date: dt.date | None = None
    end_date: dt.date | None = None
    wards: list[str] | None = None
    department: str | None = None
    category: str | None = None
    source: str | None = None


class AnalyticsService:
    def __init__(self) -> None:
        self.ai = AIService()
        self.gemini = GeminiClient()

    def wordcloud(self, db: Session, f: Filters, *, top_n: int = 60) -> dict:
        """
        Computes a simple word-frequency "word cloud" dataset from the *current 100-row input dataset*.
        This powers a visual word cloud on the dashboard.
        """
        # IMPORTANT: Word cloud must reflect the explicit 100-row input dataset file,
        # not any lingering historical rows in SQLite.
        import pandas as pd

        dataset_path = Path(settings.data_processed_dir) / "input_dataset_latest.csv"
        if not dataset_path.exists():
            return {"words": [], "total_docs": 0, "top_n": int(top_n), "source": "missing_input_dataset"}

        df = pd.read_csv(dataset_path)
        # Apply light filtering on columns that exist in the input dataset.
        if f.start_date:
            df = df[df.get("Created_Date_ISO", "").astype(str) >= f.start_date.strftime("%Y-%m-%d")]
        if f.end_date:
            df = df[df.get("Created_Date_ISO", "").astype(str) <= f.end_date.strftime("%Y-%m-%d")]
        if f.wards:
            df = df[df.get("Ward Name", "").astype(str).isin(f.wards)]
        if f.department:
            df = df[df.get("Current Department Name", "").astype(str) == f.department]
        # Category filter: use AI category from structured table if present in dataset (it won't be, so ignore).
        texts = df.get("AI_Input_Text", pd.Series([], dtype=str)).astype(str).tolist()

        # Minimal stopwords list (government-safe; tuned for civic complaints)
        stop = {
            "the",
            "and",
            "to",
            "of",
            "in",
            "on",
            "for",
            "is",
            "are",
            "was",
            "were",
            "be",
            "been",
            "it",
            "this",
            "that",
            "with",
            "from",
            "as",
            "at",
            "by",
            "an",
            "a",
            "or",
            "we",
            "i",
            "you",
            "please",
            "kindly",
            "request",
            "regarding",
            "complaint",
            "issue",
            "problem",
            "urgent",
            "immediately",
            "sir",
            "madam",
            "nmmc",
            "not",
            "has",
            "have",
            "our",
            "your",
            "they",
            "their",
            "there",
        }

        # Tokenize: keep alphabetic words, drop numbers/short tokens
        counter: Counter[str] = Counter()
        for t in texts:
            s = (t or "").lower()
            s = re.sub(r"[\r\n\t]+", " ", s)
            s = re.sub(r"[^a-z\s]", " ", s)
            s = re.sub(r"\s+", " ", s).strip()
            if not s:
                continue
            for w in s.split(" "):
                if len(w) < 3:
                    continue
                if w in stop:
                    continue
                counter[w] += 1

        top_n = max(10, min(int(top_n or 60), 120))
        words = [{"text": k, "count": int(v)} for k, v in counter.most_common(top_n)]
        return {"words": words, "total_docs": len(texts), "top_n": top_n, "source": str(dataset_path)}

    def _ai_meta(self, db: Session) -> dict | None:
        # If structured data exists, expose caseA/Gemini metadata for conditional UI branding.
        row = db.execute(
            select(GrievanceStructured.ai_provider, GrievanceStructured.ai_engine, GrievanceStructured.ai_model)
            .order_by(GrievanceStructured.processed_at.desc())
            .limit(1)
        ).first()
        if not row:
            return None
        provider, engine, model = row
        return {"ai_provider": provider, "ai_engine": engine, "ai_model": model}

    def dimensions(self, db: Session) -> dict:
        wards = [w for (w,) in db.execute(select(GrievanceRaw.ward).distinct()).all() if w]
        depts = [d for (d,) in db.execute(select(GrievanceRaw.department).distinct()).all() if d]
        cats = [c for (c,) in db.execute(select(GrievanceStructured.category).distinct()).all() if c]
        wards.sort()
        depts.sort()
        cats.sort()
        return {"wards": wards, "departments": depts, "categories": cats}

    def processed_dimensions(self, db: Session) -> dict:
        """
        Dimensions sourced from grievances_processed (used for date-range filtering UX).
        """
        wards = [w for (w,) in db.execute(select(GrievanceProcessed.ward_name).distinct()).all() if w]
        depts = [d for (d,) in db.execute(select(GrievanceProcessed.department_name).distinct()).all() if d]
        cats = [c for (c,) in db.execute(select(GrievanceProcessed.ai_category).distinct()).all() if c]
        datasets = [s for (s,) in db.execute(select(GrievanceProcessed.source_raw_filename).distinct()).all() if s]
        wards.sort()
        depts.sort()
        cats.sort()
        datasets.sort()
        return {"wards": wards, "departments": depts, "categories": cats, "datasets": datasets}

    def processed_datasets(self, db: Session) -> dict:
        """
        Dataset inventory for grievances_processed (to support /old vs /new app modes).
        Returns per-source counts and date coverage for safe default selection.
        """
        ai_subtopic_ok = func.sum(
            case(
                (
                    (GrievanceProcessed.ai_subtopic.is_not(None)) & (func.trim(GrievanceProcessed.ai_subtopic) != ""),
                    1,
                ),
                else_=0,
            )
        ).label("ai_subtopic_rows")

        ai_category_ok = func.sum(
            case(
                (
                    (GrievanceProcessed.ai_category.is_not(None)) & (func.trim(GrievanceProcessed.ai_category) != ""),
                    1,
                ),
                else_=0,
            )
        ).label("ai_category_rows")

        # New-file signal: extra columns populated (used to default /new).
        new_cols_ok = (
            func.sum(
                case(
                    (
                        (GrievanceProcessed.grievance_code.is_not(None))
                        & (func.trim(GrievanceProcessed.grievance_code) != ""),
                        1,
                    ),
                    else_=0,
                )
            )
            + func.sum(case(((GrievanceProcessed.feedback_rating.is_not(None)), 1), else_=0))
            + func.sum(case(((GrievanceProcessed.closed_date.is_not(None)), 1), else_=0))
        ).label("new_signal_rows")

        # Coverage signals for UI (helps users pick datasets that power Closure/Feedback tabs).
        feedback_ok = func.sum(case(((GrievanceProcessed.feedback_rating.is_not(None)), 1), else_=0)).label("feedback_rows")
        closed_ok = func.sum(case(((GrievanceProcessed.closed_date.is_not(None)), 1), else_=0)).label("closed_rows")

        rows = db.execute(
            select(
                GrievanceProcessed.source_raw_filename.label("source"),
                func.count().label("count"),
                func.min(GrievanceProcessed.created_date).label("min_created_date"),
                func.max(GrievanceProcessed.created_date).label("max_created_date"),
                ai_subtopic_ok,
                ai_category_ok,
                new_cols_ok,
                feedback_ok,
                closed_ok,
            )
            .where(GrievanceProcessed.source_raw_filename.is_not(None))
            .group_by(GrievanceProcessed.source_raw_filename)
            .order_by(func.count().desc())
        ).all()

        out = []
        for source, cnt, dmin, dmax, ai_sub_n, ai_cat_n, new_sig_n, feedback_n, closed_n in rows:
            total = int(cnt or 0)
            out.append(
                {
                    "source": source,
                    "count": total,
                    "min_created_date": dmin.isoformat() if dmin else None,
                    "max_created_date": dmax.isoformat() if dmax else None,
                    "ai_subtopic_rows": int(ai_sub_n or 0),
                    "ai_category_rows": int(ai_cat_n or 0),
                    "new_signal_rows": int(new_sig_n or 0),
                    "feedback_rows": int(feedback_n or 0),
                    "closed_rows": int(closed_n or 0),
                }
            )

        # Recommended defaults:
        # - old: strongest AI coverage (yesterday's Gemini run)
        # - new: strongest "new columns" signal
        recommended_old = None
        recommended_new = None
        if out:
            recommended_old = sorted(out, key=lambda d: (d.get("ai_subtopic_rows", 0), d.get("count", 0)), reverse=True)[0][
                "source"
            ]
            # New (default):
            # Prefer the FULL dataset if it already has AI coverage (i.e., ticket enrichment completed),
            # otherwise fall back to the largest __run1_ snapshot to keep the UI fast during enrichment.
            def _is_run1(src: str) -> bool:
                return "__run1_" in str(src or "")

            def _run1_n(src: str) -> int:
                try:
                    tail = str(src).split("__run1_", 1)[1]
                    return int("".join(ch for ch in tail if ch.isdigit()) or "0")
                except Exception:
                    return 0

            def _is_id_unique(src: str) -> bool:
                return str(src or "").endswith("__id_unique")

            full_candidates = [
                d
                for d in out
                if (not _is_run1(d.get("source") or ""))
                and int(d.get("count") or 0) >= 500
                and int(d.get("ai_subtopic_rows") or 0) > 0
            ]
            # If an __id_unique dataset exists (row-level unique), prefer it for /new default because it matches
            # the “unique grievances” expectation while keeping all dashboards working.
            id_unique = [d for d in full_candidates if _is_id_unique(d.get("source") or "")]
            if id_unique:
                recommended_new = sorted(
                    id_unique,
                    key=lambda d: (int(d.get("ai_subtopic_rows") or 0), int(d.get("count") or 0), d.get("max_created_date") or ""),
                    reverse=True,
                )[0]["source"]
            elif full_candidates:
                # Pick the largest AI-ready dataset (ties by most recent coverage)
                recommended_new = sorted(
                    full_candidates,
                    key=lambda d: (int(d.get("ai_subtopic_rows") or 0), int(d.get("count") or 0), d.get("max_created_date") or ""),
                    reverse=True,
                )[0]["source"]
            else:
                run1 = [d for d in out if _is_run1(d.get("source") or "")]
                if run1:
                    recommended_new = sorted(
                        run1,
                        key=lambda d: (_run1_n(d.get("source") or ""), d.get("max_created_date") or "", int(d.get("count") or 0)),
                        reverse=True,
                    )[0]["source"]
                else:
                    recommended_new = sorted(
                        out, key=lambda d: (int(d.get("new_signal_rows") or 0), d.get("max_created_date") or ""), reverse=True
                    )[0]["source"]

        return {"datasets": out, "recommended_old_source": recommended_old, "recommended_new_source": recommended_new}

    # =========================
    # Predictive analytics (rule-based early warning) — uses grievances_processed only
    # =========================

    def _processed_base(
        self,
        *,
        start_date: dt.date,
        end_date: dt.date,
        wards: list[str] | None = None,
        department: str | None = None,
        ai_category: str | None = None,
        source: str | None = None,
    ):
        q = select(GrievanceProcessed).where(
            GrievanceProcessed.created_date.is_not(None),
            GrievanceProcessed.created_date >= start_date,
            GrievanceProcessed.created_date <= end_date,
        )
        if wards:
            q = q.where(GrievanceProcessed.ward_name.in_(wards))
        if department:
            q = q.where(GrievanceProcessed.department_name == department)
        if ai_category:
            q = q.where(GrievanceProcessed.ai_category == ai_category)
        if source:
            q = q.where(GrievanceProcessed.source_raw_filename == source)
        return q

    def predictive_rising_subtopics(
        self,
        db: Session,
        *,
        start_date: dt.date,
        end_date: dt.date,
        wards: list[str] | None = None,
        department: str | None = None,
        ai_category: str | None = None,
        source: str | None = None,
        window_days: int = 14,
        min_volume: int = 10,
        growth_threshold: float = 0.5,
        top_n: int = 15,
    ) -> dict:
        """
        Compare two rolling windows (N days each) ending at end_date.
        recent: [end-N+1, end]
        previous: [end-2N+1, end-N]
        """
        window_days = max(3, min(int(window_days or 14), 60))
        min_volume = max(1, min(int(min_volume or 10), 2000))
        top_n = max(5, min(int(top_n or 15), 50))

        recent_start = end_date - dt.timedelta(days=window_days - 1)
        prev_start = end_date - dt.timedelta(days=2 * window_days - 1)
        prev_end = recent_start - dt.timedelta(days=1)

        # Require enough history inside selected range.
        if prev_start < start_date:
            return {
                "window_days": window_days,
                "recent_start": recent_start.isoformat(),
                "recent_end": end_date.isoformat(),
                "previous_start": prev_start.isoformat(),
                "previous_end": prev_end.isoformat(),
                "rows": [],
                "note": "Selected date range is too short for two windows; expand the range.",
            }

        base = self._processed_base(
            start_date=prev_start,
            end_date=end_date,
            wards=wards,
            department=department,
            ai_category=ai_category,
            source=source,
        ).subquery()
        sub_expr = func.coalesce(func.nullif(func.trim(base.c.ai_subtopic), ""), "General Civic Issue")

        recent_expr = func.sum(case((base.c.created_date >= recent_start, 1), else_=0))
        prev_expr = func.sum(case((base.c.created_date <= prev_end, 1), else_=0))
        recent_count = recent_expr.label("recent_count")
        prev_count = prev_expr.label("previous_count")

        denom = case((prev_expr > 0, prev_expr), else_=1)
        growth = ((recent_expr - prev_expr) * 1.0 / denom).label("growth_rate")

        rows = db.execute(
            select(sub_expr.label("subTopic"), prev_count, recent_count, growth)
            .group_by(sub_expr)
            .having(recent_count >= min_volume)
            .order_by(growth.desc(), recent_count.desc())
            .limit(top_n)
        ).all()

        out = []
        for sub, prev_n, recent_n, gr in rows:
            gr = float(gr or 0.0)
            out.append(
                {
                    "subTopic": sub,
                    "previous": int(prev_n or 0),
                    "recent": int(recent_n or 0),
                    "pct_change": round(gr * 100.0, 1),
                    "status": "Rising Issue" if gr > float(growth_threshold) else "Watch",
                }
            )

        return {
            "window_days": window_days,
            "recent_start": recent_start.isoformat(),
            "recent_end": end_date.isoformat(),
            "previous_start": prev_start.isoformat(),
            "previous_end": prev_end.isoformat(),
            "min_volume": min_volume,
            "growth_threshold": growth_threshold,
            "rows": out,
        }

    def predictive_ward_risk(
        self,
        db: Session,
        *,
        start_date: dt.date,
        end_date: dt.date,
        wards: list[str] | None = None,
        department: str | None = None,
        ai_category: str | None = None,
        source: str | None = None,
        window_days: int = 14,
        min_ward_volume: int = 30,
    ) -> dict:
        window_days = max(3, min(int(window_days or 14), 60))
        min_ward_volume = max(5, min(int(min_ward_volume or 30), 20000))

        recent_start = end_date - dt.timedelta(days=window_days - 1)
        prev_start = end_date - dt.timedelta(days=2 * window_days - 1)
        prev_end = recent_start - dt.timedelta(days=1)
        if prev_start < start_date:
            return {"window_days": window_days, "rows": [], "note": "Selected date range is too short for two windows; expand the range."}

        base = self._processed_base(
            start_date=prev_start,
            end_date=end_date,
            wards=wards,
            department=department,
            ai_category=ai_category,
            source=source,
        ).subquery()
        ward_expr = func.coalesce(func.nullif(func.trim(base.c.ward_name), ""), "Unknown")
        sub_expr = func.coalesce(func.nullif(func.trim(base.c.ai_subtopic), ""), "General Civic Issue")

        recent_expr = func.sum(case((base.c.created_date >= recent_start, 1), else_=0))
        prev_expr = func.sum(case((base.c.created_date <= prev_end, 1), else_=0))
        recent_total = recent_expr.label("recent_total")
        prev_total = prev_expr.label("previous_total")

        denom = case((prev_expr > 0, prev_expr), else_=1)
        growth = ((recent_expr - prev_expr) * 1.0 / denom).label("growth_rate")

        # distinct subtopics in recent window
        distinct_recent = func.count(func.distinct(case((base.c.created_date >= recent_start, sub_expr), else_=None))).label(
            "distinct_subtopics_recent"
        )

        rows = db.execute(
            select(ward_expr.label("ward"), prev_total, recent_total, growth, distinct_recent)
            .group_by(ward_expr)
            .having(recent_total >= min_ward_volume)
            .order_by(growth.desc(), recent_total.desc())
            .limit(30)
        ).all()

        # Repeat density: max subtopic share in recent window (compute with a second aggregation, still SQL)
        ward_list = [w for (w, *_rest) in rows]
        if not ward_list:
            return {"window_days": window_days, "rows": []}

        ward_sub = db.execute(
            select(
                ward_expr.label("ward"),
                sub_expr.label("subTopic"),
                func.count().label("cnt"),
            )
            .where(ward_expr.in_(ward_list))
            .where(base.c.created_date >= recent_start)
            .group_by(ward_expr, sub_expr)
        ).all()

        # small python post-processing is fine here; aggregation already done in SQL.
        by_ward_tot = {w: int(rn or 0) for (w, _p, rn, _g, _d) in rows}
        by_ward_max = {}
        for w, _s, c in ward_sub:
            by_ward_max[w] = max(int(c or 0), by_ward_max.get(w, 0))

        out = []
        for w, pn, rn, gr, ds in rows:
            pn = int(pn or 0)
            rn = int(rn or 0)
            grf = float(gr or 0.0)
            distinct = int(ds or 0)
            max_sub = by_ward_max.get(w, 0)
            repeat_density = (max_sub / rn) if rn else 0.0

            # Rule-based risk score (audit-friendly, no ML).
            if rn >= min_ward_volume and grf > 0.25 and distinct >= 8:
                risk = "HIGH"
            elif grf > 0.15 or repeat_density > 0.35:
                risk = "MEDIUM"
            else:
                risk = "LOW"

            out.append(
                {
                    "ward": w,
                    "risk": risk,
                    "previous": pn,
                    "recent": rn,
                    "pct_change": round(grf * 100.0, 1),
                    "distinct_subtopics_recent": distinct,
                    "repeat_density": round(repeat_density, 2),
                }
            )

        # rank: HIGH then MEDIUM then LOW, then by recent volume
        rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        out.sort(key=lambda x: (rank.get(x["risk"], 9), -x["recent"], -x["pct_change"]))

        return {"window_days": window_days, "rows": out}

    def predictive_chronic_issues(
        self,
        db: Session,
        *,
        start_date: dt.date,
        end_date: dt.date,
        wards: list[str] | None = None,
        department: str | None = None,
        ai_category: str | None = None,
        source: str | None = None,
        period: str = "week",
        top_n_per_period: int = 5,
        min_periods: int = 4,
        limit: int = 20,
    ) -> dict:
        """
        Chronic = appears in top N subtopics for >= K distinct periods (weeks or months).
        Uses SQL window ranking (SQLite supports window functions).
        """
        period = (period or "week").strip().lower()
        if period not in ("week", "month"):
            period = "week"
        top_n_per_period = max(3, min(int(top_n_per_period or 5), 20))
        min_periods = max(2, min(int(min_periods or 4), 52))
        limit = max(5, min(int(limit or 20), 50))

        base_q = self._processed_base(
            start_date=start_date,
            end_date=end_date,
            wards=wards,
            department=department,
            ai_category=ai_category,
            source=source,
        ).subquery()
        sub_expr = func.coalesce(func.nullif(func.trim(base_q.c.ai_subtopic), ""), "General Civic Issue")
        ward_expr = func.coalesce(func.nullif(func.trim(base_q.c.ward_name), ""), "Unknown")
        period_col = base_q.c.created_week if period == "week" else base_q.c.created_month

        # counts per period/subtopic
        counts = (
            select(
                period_col.label("period"),
                sub_expr.label("subTopic"),
                func.count().label("cnt"),
            )
            .group_by(period_col, sub_expr)
            .cte("counts")
        )

        ranked = select(
            counts.c.period,
            counts.c.subTopic,
            counts.c.cnt,
            func.dense_rank().over(partition_by=counts.c.period, order_by=counts.c.cnt.desc()).label("rnk"),
        ).cte("ranked")

        top = select(ranked.c.period, ranked.c.subTopic, ranked.c.cnt).where(ranked.c.rnk <= top_n_per_period).cte("topn")

        chronic = (
            select(
                top.c.subTopic,
                func.count(func.distinct(top.c.period)).label("periods_active"),
                func.sum(top.c.cnt).label("total_count"),
            )
            .group_by(top.c.subTopic)
            .having(func.count(func.distinct(top.c.period)) >= min_periods)
            .order_by(func.count(func.distinct(top.c.period)).desc(), func.sum(top.c.cnt).desc())
            .limit(limit)
        ).cte("chronic")

        # join back to get affected wards
        ward_counts = (
            select(
                sub_expr.label("subTopic"),
                ward_expr.label("ward"),
                func.count().label("cnt"),
            )
            .where(sub_expr.in_(select(chronic.c.subTopic)))
            .group_by(sub_expr, ward_expr)
            .cte("ward_counts")
        )

        rows = db.execute(
            select(
                chronic.c.subTopic,
                chronic.c.periods_active,
                chronic.c.total_count,
                func.group_concat(ward_counts.c.ward, ", ").label("wards"),
            )
            .join(ward_counts, ward_counts.c.subTopic == chronic.c.subTopic)
            .group_by(chronic.c.subTopic, chronic.c.periods_active, chronic.c.total_count)
            .order_by(chronic.c.periods_active.desc(), chronic.c.total_count.desc())
        ).all()

        out = []
        for sub, pa, total, wards_str in rows:
            wards_list = [w.strip() for w in (wards_str or "").split(",") if w.strip()]
            out.append(
                {
                    "subTopic": sub,
                    "periods_active": int(pa or 0),
                    "total_count": int(total or 0),
                    "affected_wards": wards_list[:10],
                }
            )

        return {"period": period, "top_n_per_period": top_n_per_period, "min_periods": min_periods, "rows": out}

    def predictive_explain(self, *, payload: dict) -> dict:
        """
        Gemini must ONLY explain computed metrics; never predict numbers.
        Returns a short governance-safe narrative.
        """
        if not settings.gemini_api_key:
            return {"explanation": "AI explanation unavailable (GEMINI_API_KEY not configured).", "ai_provider": "caseA"}

        prompt_path = Path(__file__).resolve().parent.parent / "prompts" / "predictive_explain_prompt.txt"
        tpl = prompt_path.read_text(encoding="utf-8")
        import json as _json

        prompt = tpl.replace("{{INPUT_JSON}}", _json.dumps(payload, ensure_ascii=False))
        res = self.gemini.generate_json(prompt=prompt, temperature=0.2, max_output_tokens=256, expect="dict")
        if not res.ok or not isinstance(res.parsed_json, dict):
            return {"explanation": "AI explanation unavailable due to an AI service error.", "ai_provider": "caseA"}

        explanation = str(res.parsed_json.get("explanation", "")).strip()
        if not explanation:
            explanation = "AI explanation unavailable."
        return {
            "explanation": explanation,
            "ai_provider": "caseA",
            "ai_engine": "Gemini",
            "ai_model": res.model_used,
        }

    def retrospective(self, db: Session, f: Filters) -> dict:
        base = self._base(db, f).subquery()
        total = db.scalar(select(func.count()).select_from(base)) or 0
        ai_meta = self._ai_meta(db)

        # feedback avg + distribution (filtered)
        ratings = (
            db.execute(
                select(GrievanceRaw.feedback_star)
                .where(GrievanceRaw.id.in_(select(base.c.id)))
                .where(GrievanceRaw.feedback_star.is_not(None))
            )
            .scalars()
            .all()
        )
        avg_feedback = (sum(ratings) / len(ratings)) if ratings else None
        dist = Counter(int(round(max(1.0, min(5.0, float(r))))) for r in ratings)
        feedback_dist = [{"star": s, "count": dist.get(s, 0)} for s in range(1, 6)]

        # closure distribution (filtered)
        rows = db.execute(
            select(GrievanceRaw.created_date, GrievanceRaw.closed_date).where(GrievanceRaw.id.in_(select(base.c.id)))
        ).all()
        days = [_closure_days(a, b) for (a, b) in rows]
        bucket_counts = Counter(_bucket(d) for d in days)
        closure_buckets = [{"bucket": b, "count": bucket_counts.get(b, 0)} for b in ["<7", "7-14", ">14", "Unknown"]]
        known = [d for d in days if d is not None]
        avg_closure = (sum(known) / len(known)) if known else None

        # category distribution (filtered)
        cat_rows = db.execute(
            select(GrievanceStructured.category, func.count(GrievanceStructured.id))
            .join(GrievanceRaw, GrievanceRaw.id == GrievanceStructured.raw_id)
            .where(GrievanceRaw.id.in_(select(base.c.id)))
            .group_by(GrievanceStructured.category)
            .order_by(func.count(GrievanceStructured.id).desc())
        ).all()
        categories = [{"category": c, "count": n} for (c, n) in cat_rows]

        # subcategory/subtopic distribution (filtered) — derived from AI sub_issue
        sub_rows = db.execute(
            select(GrievanceStructured.sub_issue, func.count(GrievanceStructured.id))
            .join(GrievanceRaw, GrievanceRaw.id == GrievanceStructured.raw_id)
            .where(GrievanceRaw.id.in_(select(base.c.id)))
            .group_by(GrievanceStructured.sub_issue)
            .order_by(func.count(GrievanceStructured.id).desc())
        ).all()
        subcategories = [{"subTopic": s, "count": n} for (s, n) in sub_rows]

        # ward heatmap (filtered)
        ward_rows = db.execute(
            select(GrievanceRaw.ward, func.count(GrievanceRaw.id))
            .where(GrievanceRaw.id.in_(select(base.c.id)))
            .group_by(GrievanceRaw.ward)
            .order_by(func.count(GrievanceRaw.id).desc())
        ).all()
        ward_heat = [{"ward": (w or "Unknown"), "count": n} for (w, n) in ward_rows]

        # trend (weekly by created_date) (filtered)
        created_dates = (
            db.execute(select(GrievanceRaw.created_date).where(GrievanceRaw.id.in_(select(base.c.id))))
            .scalars()
            .all()
        )
        by_week: dict[str, int] = defaultdict(int)
        for d in created_dates:
            if not d:
                continue
            y, w, _ = d.isocalendar()
            key = f"{y}-W{w:02d}"
            by_week[key] += 1
        trend = [{"week": k, "count": by_week[k]} for k in sorted(by_week.keys())]

        insights = []
        if categories:
            insights.append(f"Top category: {categories[0]['category']} ({categories[0]['count']} grievances).")
        if subcategories:
            insights.append(f"Top subcategory: {subcategories[0]['subTopic']} ({subcategories[0]['count']} grievances).")
        if avg_closure is not None:
            insights.append(f"Average closure time is {round(avg_closure, 1)} days with {bucket_counts.get('>14', 0)} in >14 days.")
        if avg_feedback is not None:
            insights.append(f"Average feedback is {round(avg_feedback, 2)}/5.0; low feedback (≤2) count: {sum(1 for r in ratings if r <= 2)}.")

        return {
            "ai_meta": ai_meta,
            "totals": {
                "totalGrievances": int(total),
                "avgClosureDays": round(avg_closure, 2) if avg_closure is not None else None,
                "avgFeedback": round(avg_feedback, 2) if avg_feedback is not None else None,
            },
            "trend": trend,
            "categoryDistribution": categories,
            "subCategoryDistribution": subcategories,
            "wardHeatmap": ward_heat,
            "closureBuckets": closure_buckets,
            "feedbackDistribution": feedback_dist,
            "insights": insights[:5],
        }

    def inferential(self, db: Session, f: Filters) -> dict:
        base = self._base(db, f).subquery()
        ai_meta = self._ai_meta(db)
        # Low feedback subset (filtered)
        low_ids = (
            db.execute(
                select(GrievanceRaw.id)
                .where(GrievanceRaw.id.in_(select(base.c.id)))
                .where(GrievanceRaw.feedback_star <= 2.0)
            )
            .scalars()
            .all()
        )
        if not low_ids:
            return {
                "ai_meta": ai_meta,
                "lowFeedback": {"count": 0},
                "drivers": {"byCategory": [], "bySubIssue": [], "byWard": [], "byDepartment": [], "byClosureBucket": []},
                "delayDrivers": {"byCategory": [], "byWard": []},
                "silentDissatisfaction": {"count": 0, "definition": "feedback<=2 AND repeat_flag==true (proxy)"},
                "insights": ["No low-feedback grievances found for current filters."],
            }

        # correlate low feedback
        by_cat = db.execute(
            select(GrievanceStructured.category, func.count(GrievanceStructured.id))
            .join(GrievanceRaw, GrievanceRaw.id == GrievanceStructured.raw_id)
            .where(GrievanceRaw.id.in_(low_ids))
            .group_by(GrievanceStructured.category)
            .order_by(func.count(GrievanceStructured.id).desc())
        ).all()
        by_sub = db.execute(
            select(GrievanceStructured.sub_issue, func.count(GrievanceStructured.id))
            .join(GrievanceRaw, GrievanceRaw.id == GrievanceStructured.raw_id)
            .where(GrievanceRaw.id.in_(low_ids))
            .group_by(GrievanceStructured.sub_issue)
            .order_by(func.count(GrievanceStructured.id).desc())
        ).all()
        by_ward = db.execute(
            select(GrievanceRaw.ward, func.count(GrievanceRaw.id))
            .where(GrievanceRaw.id.in_(low_ids))
            .group_by(GrievanceRaw.ward)
            .order_by(func.count(GrievanceRaw.id).desc())
        ).all()
        by_dept = db.execute(
            select(GrievanceRaw.department, func.count(GrievanceRaw.id))
            .where(GrievanceRaw.id.in_(low_ids))
            .group_by(GrievanceRaw.department)
            .order_by(func.count(GrievanceRaw.id).desc())
        ).all()

        # closure bucket correlation (python)
        rows = db.execute(select(GrievanceRaw.created_date, GrievanceRaw.closed_date).where(GrievanceRaw.id.in_(low_ids))).all()
        buckets = Counter(_bucket(_closure_days(a, b)) for (a, b) in rows)
        by_closure_bucket = [{"bucket": b, "count": buckets.get(b, 0)} for b in ["<7", "7-14", ">14", "Unknown"]]

        # AI dissatisfaction reasons top
        reasons = db.execute(
            select(GrievanceStructured.dissatisfaction_reason).join(GrievanceRaw, GrievanceRaw.id == GrievanceStructured.raw_id).where(GrievanceRaw.id.in_(low_ids))
        ).scalars().all()
        reason_counts = Counter((r or "Unspecified") for r in reasons)
        top_reasons = [{"reason": k, "count": v} for k, v in reason_counts.most_common(8)]

        # delay drivers: avg closure by category / ward
        joined = db.execute(
            select(GrievanceStructured.category, GrievanceRaw.ward, GrievanceRaw.created_date, GrievanceRaw.closed_date)
            .join(GrievanceRaw, GrievanceRaw.id == GrievanceStructured.raw_id)
        ).all()
        by_cat_days: dict[str, list[int]] = defaultdict(list)
        by_ward_days: dict[str, list[int]] = defaultdict(list)
        for cat, ward, a, b in joined:
            d = _closure_days(a, b)
            if d is None:
                continue
            by_cat_days[cat].append(d)
            by_ward_days[ward or "Unknown"].append(d)
        delay_by_cat = [{"category": k, "avgDays": round(sum(v)/len(v), 2), "count": len(v)} for k, v in by_cat_days.items() if v]
        delay_by_ward = [{"ward": k, "avgDays": round(sum(v)/len(v), 2), "count": len(v)} for k, v in by_ward_days.items() if v]
        delay_by_cat.sort(key=lambda x: x["avgDays"], reverse=True)
        delay_by_ward.sort(key=lambda x: x["avgDays"], reverse=True)

        silent = db.scalar(
            select(func.count(GrievanceStructured.id))
            .join(GrievanceRaw, GrievanceRaw.id == GrievanceStructured.raw_id)
            .where(GrievanceRaw.id.in_(low_ids))
            .where(GrievanceStructured.repeat_flag.is_(True))
        ) or 0

        insights = []
        if by_cat:
            insights.append(f"Low feedback clusters most in: {by_cat[0][0]} ({by_cat[0][1]} cases).")
        if top_reasons:
            insights.append(f"Top dissatisfaction reason: {top_reasons[0]['reason']} ({top_reasons[0]['count']}).")
        if buckets.get(">14", 0) > 0:
            insights.append(f"{buckets.get('>14', 0)} low-feedback cases also had >14 day closures.")
        if silent:
            insights.append(f"Silent dissatisfaction proxy (low feedback + repeat) count: {silent}.")

        return {
            "ai_meta": ai_meta,
            "lowFeedback": {"count": len(low_ids)},
            "drivers": {
                "byCategory": [{"category": c, "count": n} for (c, n) in by_cat],
                "bySubIssue": [{"subIssue": s, "count": n} for (s, n) in by_sub[:12]],
                "byWard": [{"ward": (w or "Unknown"), "count": n} for (w, n) in by_ward[:12]],
                "byDepartment": [{"department": (d or "Unknown"), "count": n} for (d, n) in by_dept[:12]],
                "byClosureBucket": by_closure_bucket,
                "topDissatisfactionReasons": top_reasons,
            },
            "delayDrivers": {"byCategory": delay_by_cat[:12], "byWard": delay_by_ward[:12]},
            "silentDissatisfaction": {"count": int(silent), "definition": "feedback<=2 AND repeat_flag==true (proxy)"},
            "insights": insights[:5],
        }

    def predictive(self, db: Session, f: Filters) -> dict:
        base = self._base(db, f).subquery()
        ai_meta = self._ai_meta(db)
        anchor = db.scalar(select(func.max(GrievanceRaw.created_date)).where(GrievanceRaw.id.in_(select(base.c.id))))
        today = anchor or dt.date.today()
        d30 = today - dt.timedelta(days=30)
        d60 = today - dt.timedelta(days=60)

        rows = db.execute(
            select(GrievanceRaw.ward, GrievanceRaw.created_date).where(GrievanceRaw.id.in_(select(base.c.id)))
        ).all()
        ward_last = Counter()
        ward_prev = Counter()
        for ward, created in rows:
            if not created:
                continue
            w = ward or "Unknown"
            if created >= d30:
                ward_last[w] += 1
            elif created >= d60:
                ward_prev[w] += 1

        ward_risk = []
        for w in set(ward_last) | set(ward_prev):
            last = ward_last[w]
            prev = ward_prev[w]
            if last >= max(6, prev * 2) and last > prev:
                ward_risk.append({"ward": w, "risk": "HIGH", "last30": last, "prev30": prev})
            elif last >= max(4, int(prev * 1.5)) and last > prev:
                ward_risk.append({"ward": w, "risk": "MEDIUM", "last30": last, "prev30": prev})
        ward_risk.sort(key=lambda x: (x["risk"], x["last30"]), reverse=True)

        # category rising
        rows2 = db.execute(
            select(GrievanceStructured.category, GrievanceRaw.created_date)
            .join(GrievanceRaw, GrievanceRaw.id == GrievanceStructured.raw_id)
            .where(GrievanceRaw.id.in_(select(base.c.id)))
        ).all()
        cat_last = Counter()
        cat_prev = Counter()
        for cat, created in rows2:
            if not created:
                continue
            if created >= d30:
                cat_last[cat] += 1
            elif created >= d60:
                cat_prev[cat] += 1

        cat_risk = []
        for cat in set(cat_last) | set(cat_prev):
            last = cat_last[cat]
            prev = cat_prev[cat]
            if last >= max(8, prev * 2) and last > prev:
                cat_risk.append({"category": cat, "risk": "HIGH", "last30": last, "prev30": prev})
            elif last >= max(5, int(prev * 1.5)) and last > prev:
                cat_risk.append({"category": cat, "risk": "MEDIUM", "last30": last, "prev30": prev})
        cat_risk.sort(key=lambda x: (x["risk"], x["last30"]), reverse=True)

        # alerts: rising volume + negative sentiment (by ward)
        ward_sent = db.execute(
            select(GrievanceRaw.ward, GrievanceStructured.sentiment, GrievanceRaw.created_date)
            .join(GrievanceStructured, GrievanceStructured.raw_id == GrievanceRaw.id)
            .where(GrievanceRaw.id.in_(select(base.c.id)))
        ).all()
        neg_last = Counter()
        neg_prev = Counter()
        total_last = Counter()
        total_prev = Counter()
        for ward, sent, created in ward_sent:
            if not created:
                continue
            w = ward or "Unknown"
            if created >= d30:
                total_last[w] += 1
                if sent == "negative":
                    neg_last[w] += 1
            elif created >= d60:
                total_prev[w] += 1
                if sent == "negative":
                    neg_prev[w] += 1
        alerts = []
        for w in ward_risk[:20]:
            ward = w["ward"]
            last_ratio = (neg_last[ward] / total_last[ward]) if total_last[ward] else 0.0
            prev_ratio = (neg_prev[ward] / total_prev[ward]) if total_prev[ward] else 0.0
            if last_ratio > prev_ratio + 0.15 and total_last[ward] >= 4:
                alerts.append({"type": "WARD_SENTIMENT", "ward": ward, "negativeRatioLast30": round(last_ratio, 2), "negativeRatioPrev30": round(prev_ratio, 2)})

        insights = []
        if ward_risk:
            insights.append(f"Top ward at risk: {ward_risk[0]['ward']} (last30={ward_risk[0]['last30']}, prev30={ward_risk[0]['prev30']}).")
        if cat_risk:
            insights.append(f"Rising category: {cat_risk[0]['category']} (last30={cat_risk[0]['last30']}, prev30={cat_risk[0]['prev30']}).")
        if alerts:
            insights.append(f"{len(alerts)} ward(s) show both rising volume and rising negative sentiment.")

        return {
            "ai_meta": ai_meta,
            "wardRisk": ward_risk[:5],
            "categoryRisk": cat_risk[:5],
            "alerts": alerts[:8],
            "insights": insights[:5],
        }

    def feedback(self, db: Session, f: Filters) -> dict:
        """
        Feedback analytics using grievances_processed (supports raw2 ratings).
        NOTE: This endpoint intentionally does NOT call Gemini. It only aggregates stored fields.
        """
        import datetime as dt
        from sqlalchemy import Integer, and_, cast

        # Resolve date range (UI normally supplies these)
        start = f.start_date or dt.date(1900, 1, 1)
        end = f.end_date or dt.date.today()

        base = self._processed_base(
            start_date=start,
            end_date=end,
            wards=f.wards,
            department=f.department,
            ai_category=f.category,  # category filter == ai_category in processed world
            source=f.source,
        ).subquery()

        # Normalize star rating (1..5) from float
        star = cast(func.round(base.c.feedback_rating), Integer)
        star_norm = case(
            (and_(star >= 1, star <= 5), star),
            else_=None,
        ).label("star")

        dist_rows = db.execute(
            select(star_norm, func.count().label("count"))
            .where(star_norm.is_not(None))
            .group_by(star_norm)
            .order_by(star_norm.asc())
        ).all()
        feedback_distribution = [{"star": int(s), "count": int(c)} for (s, c) in dist_rows if s is not None]

        avg_feedback = db.execute(select(func.avg(base.c.feedback_rating)).where(star_norm.is_not(None))).scalar()
        avg_feedback = round(float(avg_feedback), 2) if avg_feedback is not None else None

        # Low feedback (<=2)
        is_low = star_norm <= 2
        cat_expr = func.coalesce(func.nullif(func.trim(base.c.ai_category), ""), func.nullif(func.trim(base.c.department_name), ""), "Unknown").label("category")
        ward_expr = func.coalesce(func.nullif(func.trim(base.c.ward_name), ""), "Unknown").label("ward")

        by_cat_rows = db.execute(
            select(cat_expr, func.count().label("count"))
            .where(star_norm.is_not(None), is_low)
            .group_by(cat_expr)
            .order_by(func.count().desc())
        ).all()
        by_ward_rows = db.execute(
            select(ward_expr, func.count().label("count"))
            .where(star_norm.is_not(None), is_low)
            .group_by(ward_expr)
            .order_by(func.count().desc())
        ).all()

        # Closure bucket for low feedback correlation (only if closed_date present)
        days = cast(func.julianday(base.c.closed_date) - func.julianday(base.c.created_date), Integer)
        days_ok = case(((base.c.closed_date.is_not(None)) & (base.c.created_date.is_not(None)) & (days >= 0), days), else_=None)
        bucket = case(
            (days_ok.is_(None), "Unknown"),
            (days_ok < 7, "<7"),
            (days_ok <= 14, "7-14"),
            else_=">14",
        ).label("bucket")
        by_bucket_rows = db.execute(
            select(bucket, func.count().label("count"))
            .where(star_norm.is_not(None), is_low)
            .group_by(bucket)
            .order_by(func.count().desc())
        ).all()

        # We intentionally avoid grouping by free-text remarks here (too noisy for charts).
        top_reasons: list[dict] = []

        ai_rows = db.execute(
            select(
                func.sum(
                    case(
                        (((base.c.ai_category.is_not(None)) & (func.trim(base.c.ai_category) != "")), 1),
                        else_=0,
                    )
                )
            )
        ).scalar()
        ai_meta = {"ai_provider": "caseA"} if int(ai_rows or 0) > 0 else {"ai_provider": "none"}

        insights = []
        if avg_feedback is not None:
            insights.append(f"Average feedback rating: {avg_feedback} / 5.")
        if feedback_distribution:
            total = sum(r["count"] for r in feedback_distribution)
            low = sum(r["count"] for r in feedback_distribution if int(r["star"]) <= 2)
            if total:
                insights.append(f"Low feedback (≤2): {low} / {total} ({round(100.0*low/total, 1)}%).")

        return {
            "ai_meta": ai_meta,
            "feedbackDistribution": feedback_distribution,
            "avgFeedback": avg_feedback,
            "lowFeedbackDrivers": {
                "topDissatisfactionReasons": top_reasons,
                "byCategory": [{"category": c, "count": int(n)} for (c, n) in by_cat_rows if c],
                "byWard": [{"ward": w, "count": int(n)} for (w, n) in by_ward_rows if w],
                "byClosureBucket": [{"bucket": b, "count": int(n)} for (b, n) in by_bucket_rows if b],
            },
            "insights": insights[:5],
        }

    def closure(self, db: Session, f: Filters) -> dict:
        """
        Closure analytics using grievances_processed (supports raw2 close_date).
        """
        import datetime as dt
        from sqlalchemy import Integer, and_, cast

        start = f.start_date or dt.date(1900, 1, 1)
        end = f.end_date or dt.date.today()

        base = self._processed_base(
            start_date=start,
            end_date=end,
            wards=f.wards,
            department=f.department,
            ai_category=f.category,  # category filter == ai_category in processed world
            source=f.source,
        ).subquery()

        days = cast(func.julianday(base.c.closed_date) - func.julianday(base.c.created_date), Integer)
        days_ok = case(((base.c.closed_date.is_not(None)) & (base.c.created_date.is_not(None)) & (days >= 0), days), else_=None).label(
            "closure_days"
        )
        bucket = case(
            (days_ok.is_(None), "Unknown"),
            (days_ok < 7, "<7"),
            (days_ok <= 14, "7-14"),
            else_=">14",
        ).label("bucket")

        bucket_rows = db.execute(select(bucket, func.count().label("count")).group_by(bucket)).all()
        bucket_counts = {b: int(c) for (b, c) in bucket_rows if b}
        buckets = [{"bucket": b, "count": bucket_counts.get(b, 0)} for b in ["<7", "7-14", ">14", "Unknown"]]

        cat_expr = func.coalesce(func.nullif(func.trim(base.c.ai_category), ""), func.nullif(func.trim(base.c.department_name), ""), "Unknown").label("category")
        ward_expr = func.coalesce(func.nullif(func.trim(base.c.ward_name), ""), "Unknown").label("ward")

        avg_by_cat_rows = db.execute(
            select(cat_expr, func.avg(days_ok).label("avgDays"), func.count(days_ok).label("count"))
            .where(days_ok.is_not(None))
            .group_by(cat_expr)
            .order_by(func.avg(days_ok).desc())
        ).all()
        avg_by_ward_rows = db.execute(
            select(ward_expr, func.avg(days_ok).label("avgDays"), func.count(days_ok).label("count"))
            .where(days_ok.is_not(None))
            .group_by(ward_expr)
            .order_by(func.avg(days_ok).desc())
        ).all()

        avg_by_cat = [
            {"category": c, "avgDays": round(float(a), 2), "count": int(n)}
            for (c, a, n) in avg_by_cat_rows
            if c and a is not None and n
        ]
        avg_by_ward = [
            {"ward": w, "avgDays": round(float(a), 2), "count": int(n)} for (w, a, n) in avg_by_ward_rows if w and a is not None and n
        ]

        ai_rows = db.execute(
            select(
                func.sum(
                    case(
                        (((base.c.ai_category.is_not(None)) & (func.trim(base.c.ai_category) != "")), 1),
                        else_=0,
                    )
                )
            )
        ).scalar()
        ai_meta = {"ai_provider": "caseA"} if int(ai_rows or 0) > 0 else {"ai_provider": "none"}

        insights = []
        if avg_by_cat:
            insights.append(f"Longest average closures: {avg_by_cat[0]['category']} ({avg_by_cat[0]['avgDays']} days).")
        if bucket_counts.get(">14", 0):
            insights.append(f"{bucket_counts.get('>14', 0)} grievances took more than 14 days to close.")
        if avg_by_ward:
            insights.append(f"Ward with highest average closure: {avg_by_ward[0]['ward']} ({avg_by_ward[0]['avgDays']} days).")

        return {
            "ai_meta": ai_meta,
            "closureBuckets": buckets,
            "avgClosureByCategory": avg_by_cat[:12],
            "avgClosureByWard": avg_by_ward[:12],
            "insights": insights[:5],
        }

    def closure_sla_snapshot(self, db: Session, f: Filters) -> dict:
        """
        Dedicated Closure Timeliness (SLA) snapshot for embedding in Issue Intelligence 2 → Overview.

        Returns:
        - KPIs (median, p90, within 1 day, within 7 days, >30 days)
        - Bucket distribution in days for closed grievances
        """
        import datetime as dt
        from sqlalchemy import Integer, cast

        start = f.start_date or dt.date(1900, 1, 1)
        end = f.end_date or dt.date.today()

        base = self._processed_filter_subquery(
            db,
            start_date=start,
            end_date=end,
            wards=f.wards,
            department=f.department,
            category=f.category,
            source=f.source,
        )

        # Prefer precomputed resolution_days; fallback to date diff.
        jd_days = func.julianday(base.c.closed_date) - func.julianday(base.c.created_date)
        closed_days = case(
            (
                (base.c.closed_date.is_not(None)) & (base.c.created_date.is_not(None)) & (jd_days >= 0),
                jd_days,
            ),
            else_=None,
        )
        closure_ok = case(
            (
                (base.c.resolution_days.is_not(None)) & (base.c.resolution_days >= 0),
                base.c.resolution_days,
            ),
            else_=closed_days,
        )

        vals = db.execute(select(closure_ok).where(closure_ok.is_not(None)).select_from(base)).scalars().all()
        xs = [float(x) for x in vals if x is not None]
        xs.sort()
        n = len(xs)

        median = self._median(xs) if xs else None
        p90 = None
        if xs:
            idx = int(round(0.9 * (len(xs) - 1)))
            idx = max(0, min(idx, len(xs) - 1))
            p90 = float(xs[idx])

        # Bucket distribution (days)
        buckets = [
            ("0-1 Day", 0, 1, "standard"),
            ("1-3 Days", 1, 3, "standard"),
            ("3-7 Days", 3, 7, "standard"),
            ("7-14 Days", 7, 14, "standard"),
            ("14-30 Days", 14, 30, "standard"),
            ("30-60 Days", 30, 60, "long_tail"),
            ("60+ Days", 60, None, "long_tail"),
        ]

        counts: dict[str, int] = {b[0]: 0 for b in buckets}
        within_1 = 0
        within_7 = 0
        over_30 = 0
        for v in xs:
            if v <= 1:
                within_1 += 1
            if v <= 7:
                within_7 += 1
            if v > 30:
                over_30 += 1
            # bucket assignment (exclusive lower bound, inclusive upper bound)
            placed = False
            for label, lo, hi, _band in buckets:
                if hi is None:
                    if v > lo:
                        counts[label] += 1
                        placed = True
                        break
                else:
                    if v > lo and v <= hi:
                        counts[label] += 1
                        placed = True
                        break
            if not placed:
                # For exact 0 days, put into 0-1 Day.
                if v == 0:
                    counts["0-1 Day"] += 1

        def pct(x: int) -> float | None:
            if not n:
                return None
            return round((100.0 * float(x)) / float(n), 2)

        dist_rows = []
        for label, lo, hi, band in buckets:
            c = int(counts.get(label, 0))
            dist_rows.append(
                {
                    "bucket": label,
                    "count": c,
                    "pct": pct(c),
                    "band": band,
                    "lo": lo,
                    "hi": hi,
                }
            )

        return {
            "filters": {
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "wards": f.wards,
                "department": f.department,
                "category": f.category,
                "source": f.source,
            },
            "as_of": end.isoformat(),
            "based_on": {"closed_n": int(n)},
            "kpis": {
                "median_days": round(float(median), 2) if median is not None else None,
                "p90_days": round(float(p90), 2) if p90 is not None else None,
                "within_1d_pct": pct(within_1),
                "within_7d_pct": pct(within_7),
                "over_30d_pct": pct(over_30),
            },
            "distribution": {"rows": dist_rows},
        }

    def forwarding_snapshot(self, db: Session, f: Filters) -> dict:
        """
        Dedicated Forwarding Analytics snapshot for embedding in Issue Intelligence 2 → Overview.

        Uses grievances_processed:
        - forward_count (number of forwards)
        - forwarded_at (timestamp of first forward)
        - created_at/created_date (for forward delay)
        """
        import datetime as dt

        start = f.start_date or dt.date(1900, 1, 1)
        end = f.end_date or dt.date.today()

        base = self._processed_filter_subquery(
            db,
            start_date=start,
            end_date=end,
            wards=f.wards,
            department=f.department,
            category=f.category,
            source=f.source,
        )

        total = int(db.scalar(select(func.count()).select_from(base)) or 0)

        forwarded_flag = case(((base.c.forward_count.is_not(None)) & (base.c.forward_count > 0), 1), else_=0)
        forwarded_n = int(db.scalar(select(func.sum(forwarded_flag)).select_from(base)) or 0)
        forwarded_pct = round((100.0 * forwarded_n / total), 2) if total else 0.0

        # Forward delay in days (created_at -> forwarded_at). If timestamps are missing, fall back to date-level.
        jd_delay_dt = func.julianday(base.c.forwarded_at) - func.julianday(base.c.created_at)
        jd_delay_date = func.julianday(base.c.forwarded_at) - func.julianday(base.c.created_date)
        delay = case(
            (
                (base.c.forwarded_at.is_not(None)) & (base.c.created_at.is_not(None)) & (jd_delay_dt >= 0),
                jd_delay_dt,
            ),
            else_=case(
                (
                    (base.c.forwarded_at.is_not(None)) & (base.c.created_date.is_not(None)) & (jd_delay_date >= 0),
                    jd_delay_date,
                ),
                else_=None,
            ),
        )

        dvals = db.execute(select(delay).where(delay.is_not(None)).select_from(base)).scalars().all()
        xs = [float(x) for x in dvals if x is not None]
        xs.sort()
        n_delay = len(xs)
        med_delay = self._median(xs) if xs else None
        p90_delay = None
        if xs:
            idx = int(round(0.9 * (len(xs) - 1)))
            idx = max(0, min(idx, len(xs) - 1))
            p90_delay = float(xs[idx])

        # Hop distribution among forwarded tickets:
        # - 1 Hop = forward_count == 1
        # - 2 Hops = forward_count == 2
        # - 3+ Hops = forward_count >= 3
        hop_1 = int(
            db.scalar(select(func.count()).where(base.c.forward_count == 1).select_from(base)) or 0
        )
        hop_2 = int(
            db.scalar(select(func.count()).where(base.c.forward_count == 2).select_from(base)) or 0
        )
        hop_3p = int(
            db.scalar(select(func.count()).where(base.c.forward_count >= 3).select_from(base)) or 0
        )

        # “Multiple hops” counters (match screenshot semantics)
        refwd_ge2 = int(db.scalar(select(func.count()).where(base.c.forward_count >= 2).select_from(base)) or 0)
        chronic_ge3 = int(db.scalar(select(func.count()).where(base.c.forward_count >= 3).select_from(base)) or 0)

        return {
            "filters": {
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "wards": f.wards,
                "department": f.department,
                "category": f.category,
                "source": f.source,
            },
            "as_of": end.isoformat(),
            "based_on": {"total_n": total, "forwarded_n": forwarded_n, "delay_n": int(n_delay)},
            "kpis": {
                "forwarded_pct": float(forwarded_pct),
                "median_forward_delay_days": round(float(med_delay), 2) if med_delay is not None else None,
                "p90_forward_delay_days": round(float(p90_delay), 2) if p90_delay is not None else None,
            },
            "distribution": {
                "among_forwarded_n": forwarded_n,
                "hops": [
                    {"bucket": "1 Hop (Standard)", "count": hop_1, "band": "standard"},
                    {"bucket": "2 Hops (Correction)", "count": hop_2, "band": "correction"},
                    {"bucket": "3+ Hops (Confusion)", "count": hop_3p, "band": "confusion"},
                ],
            },
            "multiple_hops": {
                "reforwarded_ge2": refwd_ge2,
                "chronic_ge3": chronic_ge3,
            },
            "insight": (
                f"Insight: Every forward event resets the \"clock\" for the citizen but adds "
                f"{round(float(med_delay), 2) if med_delay is not None else '—'} days of hidden wait time."
            ),
        }

    def forwarding_impact_resolution(self, db: Session, f: Filters) -> dict:
        """
        Forwarding Impact on Resolution Time ("process tax") analysis.
        Compares closure time distribution for:
        - Direct closure (not forwarded): forward_count == 0
        - Re-routed (forwarded): forward_count > 0

        Uses closed tickets only (needs valid closure time).
        """
        import datetime as dt

        start = f.start_date or dt.date(1900, 1, 1)
        end = f.end_date or dt.date.today()

        base = self._processed_filter_subquery(
            db,
            start_date=start,
            end_date=end,
            wards=f.wards,
            department=f.department,
            category=f.category,
            source=f.source,
        )

        # Closure time in days: prefer resolution_days else closed_date-created_date
        jd_days = func.julianday(base.c.closed_date) - func.julianday(base.c.created_date)
        closed_days = case(
            (
                (base.c.closed_date.is_not(None)) & (base.c.created_date.is_not(None)) & (jd_days >= 0),
                jd_days,
            ),
            else_=None,
        )
        closure_ok = case(
            (
                (base.c.resolution_days.is_not(None)) & (base.c.resolution_days >= 0),
                base.c.resolution_days,
            ),
            else_=closed_days,
        )

        # Only rows with valid closure time
        closed_base = select(
            base.c.forward_count.label("forward_count"),
            closure_ok.label("closure_days"),
        ).where(closure_ok.is_not(None)).subquery()

        direct_q = select(closed_base.c.closure_days).where(
            (closed_base.c.forward_count.is_(None)) | (closed_base.c.forward_count <= 0)
        )
        fwd_q = select(closed_base.c.closure_days).where(closed_base.c.forward_count > 0)

        direct_vals = [float(x) for x in db.execute(direct_q).scalars().all() if x is not None]
        fwd_vals = [float(x) for x in db.execute(fwd_q).scalars().all() if x is not None]
        direct_vals.sort()
        fwd_vals.sort()

        def _mean(xs: list[float]) -> float | None:
            if not xs:
                return None
            return float(sum(xs) / len(xs))

        def _bucket_counts(xs: list[float]) -> list[dict]:
            # Match screenshot buckets (coarser; focuses on tail)
            buckets = [
                ("0-1d", 0, 1),
                ("1-3d", 1, 3),
                ("3-7d", 3, 7),
                ("7-14d", 7, 14),
                ("14-30d", 14, 30),
                ("30d+", 30, None),
            ]
            n = len(xs)
            counts = {b[0]: 0 for b in buckets}
            for v in xs:
                placed = False
                for label, lo, hi in buckets:
                    if hi is None:
                        if v > lo:
                            counts[label] += 1
                            placed = True
                            break
                    else:
                        if v > lo and v <= hi:
                            counts[label] += 1
                            placed = True
                            break
                if not placed and v == 0:
                    counts["0-1d"] += 1
            rows = []
            for label, lo, hi in buckets:
                c = int(counts.get(label, 0))
                pct = round((100.0 * c / n), 2) if n else None
                rows.append({"bucket": label, "count": c, "pct": pct, "lo": lo, "hi": hi})
            return rows

        direct_n = len(direct_vals)
        fwd_n = len(fwd_vals)

        direct_median = self._median(direct_vals) if direct_vals else None
        fwd_median = self._median(fwd_vals) if fwd_vals else None
        direct_mean = _mean(direct_vals)
        fwd_mean = _mean(fwd_vals)

        uplift_median_pct = None
        if direct_median and direct_median > 0 and fwd_median is not None:
            uplift_median_pct = round(((float(fwd_median) - float(direct_median)) / float(direct_median)) * 100.0, 0)

        heavy_tail = False
        if fwd_mean is not None and direct_mean is not None and fwd_mean > direct_mean * 2:
            heavy_tail = True

        return {
            "filters": {
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "wards": f.wards,
                "department": f.department,
                "category": f.category,
                "source": f.source,
            },
            "as_of": end.isoformat(),
            "based_on": {"closed_n": int(direct_n + fwd_n), "direct_n": int(direct_n), "forwarded_n": int(fwd_n)},
            "direct": {
                "median_days": round(float(direct_median), 2) if direct_median is not None else None,
                "mean_days": round(float(direct_mean), 2) if direct_mean is not None else None,
                "distribution": _bucket_counts(direct_vals),
            },
            "forwarded": {
                "median_days": round(float(fwd_median), 2) if fwd_median is not None else None,
                "mean_days": round(float(fwd_mean), 2) if fwd_mean is not None else None,
                "distribution": _bucket_counts(fwd_vals),
            },
            "comparison": {
                "median_uplift_pct": uplift_median_pct,
                "heavy_tail": bool(heavy_tail),
            },
        }

    def _base(self, db: Session, f: Filters):
        q = select(GrievanceRaw.id)
        if f.start_date:
            q = q.where(GrievanceRaw.created_date >= f.start_date)
        if f.end_date:
            q = q.where(GrievanceRaw.created_date <= f.end_date)
        if f.wards:
            q = q.where(GrievanceRaw.ward.in_(f.wards))
        if f.department:
            q = q.where(GrievanceRaw.department == f.department)
        if f.category:
            q = q.join(GrievanceStructured, GrievanceStructured.raw_id == GrievanceRaw.id).where(
                GrievanceStructured.category == f.category
            )
        return q

    # =========================
    # Date-range analytics (NEW) — uses grievances_processed only
    # =========================
    def _median(self, xs: list[float]) -> float | None:
        xs = [float(x) for x in xs if x is not None]
        if not xs:
            return None
        xs.sort()
        n = len(xs)
        mid = n // 2
        if n % 2 == 1:
            return float(xs[mid])
        return float((xs[mid - 1] + xs[mid]) / 2.0)

    def _parse_entities(self, s: str | None) -> list[str]:
        if not s:
            return []
        try:
            v = json.loads(s)
            if isinstance(v, list):
                out = []
                for x in v:
                    if x is None:
                        continue
                    t = str(x).strip()
                    if t:
                        out.append(t)
                return out
        except Exception:
            return []
        return []

    def _processed_filter_subquery(
        self,
        db: Session,
        *,
        start_date: dt.date,
        end_date: dt.date,
        wards: list[str] | None = None,
        department: str | None = None,
        category: str | None = None,
        source: str | None = None,
    ):
        """
        Returns a subquery over grievances_processed for a given filter set.

        Key behavior:
        - For normal (partial) date ranges, we filter to rows with created_date within [start_date, end_date].
        - If the requested range covers the *entire* dated span for the selected filters (i.e., "All"),
          we include rows with NULL created_date too, so totals match the full dataset and users don't
          perceive "missing" records purely due to missing dates.
        """
        # IMPORTANT: v2 endpoints must be robust to whitespace in dimension values.
        # We use trimmed comparisons here to avoid "No data" when DB values contain stray spaces.
        conds = []
        if wards:
            ward_list = [w.strip() for w in wards if str(w or "").strip()]
            if ward_list:
                conds.append(func.trim(GrievanceProcessed.ward_name).in_(ward_list))
        if department:
            conds.append(func.trim(GrievanceProcessed.department_name) == str(department).strip())
        if category:
            conds.append(func.trim(GrievanceProcessed.ai_category) == str(category).strip())
        if source:
            conds.append(GrievanceProcessed.source_raw_filename == source)

        # Determine the full dated span for this filter set (ignoring undated rows).
        min_d, max_d = db.execute(
            select(func.min(GrievanceProcessed.created_date), func.max(GrievanceProcessed.created_date)).where(
                GrievanceProcessed.created_date.is_not(None),
                *conds,
            )
        ).one()

        include_undated = False
        if min_d is None or max_d is None:
            # No dated rows at all: include undated so the dataset isn't empty.
            include_undated = True
        else:
            include_undated = bool(start_date <= min_d and end_date >= max_d)

        q = select(GrievanceProcessed).where(*conds)
        if not include_undated:
            q = q.where(
                GrievanceProcessed.created_date.is_not(None),
                GrievanceProcessed.created_date >= start_date,
                GrievanceProcessed.created_date <= end_date,
            )
        return q.subquery()

    def executive_overview_v2(
        self,
        db: Session,
        *,
        start_date: dt.date,
        end_date: dt.date,
        wards: list[str] | None = None,
        department: str | None = None,
        category: str | None = None,
        source: str | None = None,
        top_n: int = 10,
        closed_series_min_coverage: float = 0.25,
    ) -> dict:
        """
        Executive Overview v2: same visuals + small toggles/cards.
        Reads grievances_processed only. No Gemini calls.
        """
        top_n = max(3, min(int(top_n or 10), 15))
        base = self._processed_filter_subquery(
            db,
            start_date=start_date,
            end_date=end_date,
            wards=wards,
            department=department,
            category=category,
            source=source,
        )

        def _with_retry(fn):
            import time

            for delay in (0.15, 0.4, 0.9):
                try:
                    return fn()
                except OperationalError as e:
                    if "database is locked" not in str(e).lower():
                        raise
                    time.sleep(delay)
            # last attempt
            return fn()

        total = int(_with_retry(lambda: db.scalar(select(func.count()).select_from(base)) or 0))

        # closure days (prefer resolution_days, fallback to closed_date-created_date)
        jd_days = func.julianday(base.c.closed_date) - func.julianday(base.c.created_date)
        closed_days = case(
            (
                (base.c.closed_date.is_not(None)) & (base.c.created_date.is_not(None)) & (jd_days >= 0),
                jd_days,
            ),
            else_=None,
        )
        closure_ok = case(
            (
                (base.c.resolution_days.is_not(None)) & (base.c.resolution_days >= 0),
                base.c.resolution_days,
            ),
            else_=closed_days,
        )
        closure_known = int(
            _with_retry(lambda: db.scalar(select(func.count()).where(closure_ok.is_not(None)).select_from(base)) or 0)
        )
        avg_closure = _with_retry(lambda: db.scalar(select(func.avg(closure_ok)).select_from(base)))
        avg_closure = round(float(avg_closure), 2) if avg_closure is not None else None
        # median / p90 (python; N ~ 10k is fine)
        closure_vals = _with_retry(
            lambda: db.execute(select(closure_ok).where(closure_ok.is_not(None)).select_from(base)).scalars().all()
        )
        closure_vals_f = [float(x) for x in closure_vals if x is not None]
        closure_vals_f.sort()
        median_closure = self._median(closure_vals_f) if closure_vals_f else None
        p90_closure = None
        if closure_vals_f:
            idx = int(round(0.9 * (len(closure_vals_f) - 1)))
            idx = max(0, min(idx, len(closure_vals_f) - 1))
            p90_closure = float(closure_vals_f[idx])

        # rating (1..5)
        rating_ok = case(
            (
                (base.c.feedback_rating.is_not(None)) & (base.c.feedback_rating >= 1) & (base.c.feedback_rating <= 5),
                base.c.feedback_rating,
            ),
            else_=None,
        )
        rating_known = int(
            _with_retry(lambda: db.scalar(select(func.count()).where(rating_ok.is_not(None)).select_from(base)) or 0)
        )
        avg_rating = _with_retry(lambda: db.scalar(select(func.avg(rating_ok)).select_from(base)))
        avg_rating = round(float(avg_rating), 2) if avg_rating is not None else None

        # status breakdown + backlog (best-effort)
        status_rows = _with_retry(
            lambda: db.execute(
                select(base.c.status, func.count().label("cnt"))
                .group_by(base.c.status)
                .order_by(func.count().desc())
            ).all()
        )
        status_breakdown = [{"status": (s or "Unknown"), "count": int(n)} for (s, n) in status_rows]
        closed_like = sum(
            int(r["count"])
            for r in status_breakdown
            if "closed" in str(r["status"]).lower() or "resolved" in str(r["status"]).lower()
        )
        open_backlog = max(0, total - closed_like) if total else 0

        # operational risk snapshot
        within_3d = int(
            _with_retry(
                lambda: db.scalar(select(func.count()).where(closure_ok.is_not(None), closure_ok <= 3).select_from(base)) or 0
            )
        )
        over_30d = int(
            _with_retry(
                lambda: db.scalar(select(func.count()).where(closure_ok.is_not(None), closure_ok > 30).select_from(base)) or 0
            )
        )
        forwarded = int(
            _with_retry(
                lambda: db.scalar(
                    select(func.count())
                    .where((base.c.forward_count.is_not(None)) & (base.c.forward_count > 0))
                    .select_from(base)
                )
                or 0
            )
        )
        low_rating = int(
            _with_retry(lambda: db.scalar(select(func.count()).where(rating_ok.is_not(None), rating_ok <= 2).select_from(base)) or 0)
        )
        escalation_rate = round(100.0 * forwarded / total, 1) if total else 0.0
        risk = {
            "within_3d": {"count": within_3d, "pct": round(100.0 * within_3d / total, 1) if total else 0.0},
            "over_30d": {"count": over_30d, "pct": round(100.0 * over_30d / total, 1) if total else 0.0},
            "forwarded": {"count": forwarded, "pct": round(100.0 * forwarded / total, 1) if total else 0.0},
            "low_rating_1_2": {"count": low_rating, "pct": round(100.0 * low_rating / total, 1) if total else 0.0},
        }

        # totals for priority mode
        total_priority = db.scalar(select(func.sum(func.coalesce(base.c.actionable_score, 0))).select_from(base))
        total_priority = int(total_priority or 0)

        # top categories/subtopics (count + priority_sum)
        cat_expr = func.coalesce(func.nullif(func.trim(base.c.ai_category), ""), "Other Civic Issues")
        sub_expr = func.coalesce(func.nullif(func.trim(base.c.ai_subtopic), ""), "General Civic Issue")
        pr = func.sum(func.coalesce(base.c.actionable_score, 0)).label("priority_sum")

        cat_rows = db.execute(
            select(cat_expr.label("category"), func.count().label("count"), pr)
            .group_by(cat_expr)
            .order_by(func.count().desc())
            .limit(top_n)
        ).all()
        top_categories = [{"category": c, "count": int(n), "priority_sum": int(p or 0)} for (c, n, p) in cat_rows]

        sub_rows = db.execute(
            select(sub_expr.label("subTopic"), func.count().label("count"), pr)
            .group_by(sub_expr)
            .order_by(func.count().desc())
            .limit(top_n)
        ).all()
        top_subtopics = [{"subTopic": s, "count": int(n), "priority_sum": int(p or 0)} for (s, n, p) in sub_rows]

        # daily time series: created always, closed optional
        created_rows = db.execute(
            select(base.c.created_date.label("d"), func.count().label("cnt"))
            .where(base.c.created_date.is_not(None))
            .group_by(base.c.created_date)
            .order_by(base.c.created_date.asc())
        ).all()
        created_daily = {d.strftime("%Y-%m-%d"): int(n) for (d, n) in created_rows if d}

        closed_rows = db.execute(
            select(base.c.closed_date.label("d"), func.count().label("cnt"))
            .where(base.c.closed_date.is_not(None))
            .group_by(base.c.closed_date)
            .order_by(base.c.closed_date.asc())
        ).all()
        closed_daily = {d.strftime("%Y-%m-%d"): int(n) for (d, n) in closed_rows if d}

        closed_coverage = int(db.scalar(select(func.count()).where(base.c.closed_date.is_not(None)).select_from(base)) or 0)
        closed_coverage_pct = (float(closed_coverage) / float(total)) if total else 0.0
        show_closed = bool(closed_coverage_pct >= float(closed_series_min_coverage))

        all_days = sorted(set(created_daily.keys()) | set(closed_daily.keys()))
        series = [{"day": day, "created": created_daily.get(day, 0), "closed": closed_daily.get(day, 0)} for day in all_days]

        # insights (no extra AI calls)
        insights = []
        insights.append(f"Total grievances: {total}. Open backlog (best-effort): {open_backlog}.")
        if avg_closure is not None:
            insights.append(
                f"Average closure time: {avg_closure} days (coverage {closure_known}/{total})."
            )
        else:
            insights.append(f"Average closure time unavailable (coverage {closure_known}/{total}).")
        if avg_rating is not None:
            insights.append(f"Average rating: {avg_rating}/5 (coverage {rating_known}/{total}).")
        else:
            insights.append(f"Average rating unavailable (coverage {rating_known}/{total}).")
        insights.append(
            f"Operational risk: {risk['over_30d']['pct']}% >30d, {risk['forwarded']['pct']}% forwarded, {risk['low_rating_1_2']['pct']}% low rating (1–2)."
        )

        return {
            "generated_at": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "filters": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "wards": wards or [],
                "department": department,
                "category": category,
                "source": source,
            },
            "totals": {
                "total_grievances": total,
                "open_backlog": int(open_backlog),
                "avg_closure_time_days": avg_closure,
                "avg_closure_coverage": {"known": closure_known, "total": total},
                "median_closure_time_days": round(float(median_closure), 2) if median_closure is not None else None,
                "p90_closure_time_days": round(float(p90_closure), 2) if p90_closure is not None else None,
                "avg_rating": avg_rating,
                "avg_rating_coverage": {"known": rating_known, "total": total},
                "total_priority_sum": int(total_priority),
                "closed_coverage": {"known": int(closed_coverage), "total": total, "pct": round(100.0 * closed_coverage_pct, 1) if total else 0.0},
            },
            "time_series_daily": {
                "show_closed": show_closed,
                "rows": series,
            },
            "top": {
                "categories": top_categories,
                "subtopics": top_subtopics,
            },
            "operational_risk_snapshot": risk,
            "escalation": {
                "enabled": True,
                "escalated_count": int(forwarded),
                "rate_pct": escalation_rate,
            },
            "analytics": {
                "ai_coverage_known": int(db.scalar(select(func.count()).where(sub_expr != "General Civic Issue").select_from(base)) or 0),
                "ai_coverage_total": int(total),
            },
            "status_breakdown": status_breakdown,
            "insights": insights[:6],
        }

    def issue_intelligence_v2(
        self,
        db: Session,
        *,
        start_date: dt.date,
        end_date: dt.date,
        wards: list[str] | None = None,
        department: str | None = None,
        category: str | None = None,
        source: str | None = None,
        ward_focus: str | None = None,
        department_focus: str | None = None,
        subtopic_focus: str | None = None,
        unique_min_priority: int = 0,
        unique_confidence_high_only: bool = False,
        top_n: int = 10,
        by_ward_top_n: int = 5,
        by_dept_top_n: int = 10,
        entities_top_n: int = 10,
    ) -> dict:
        """
        Issue Intelligence v2: richer metrics for operational prioritization.
        Reads grievances_processed only. No Gemini calls.
        """
        top_n = max(3, min(int(top_n or 10), 15))
        base = self._processed_filter_subquery(
            db,
            start_date=start_date,
            end_date=end_date,
            wards=wards,
            department=department,
            category=category,
            source=source,
        )

        total = int(db.scalar(select(func.count()).select_from(base)) or 0)

        # =========================
        # Data readiness (coverage)
        # =========================
        ai_known = int(
            db.scalar(
                select(func.count())
                .where((base.c.ai_subtopic.is_not(None)) & (func.trim(base.c.ai_subtopic) != ""))
                .select_from(base)
            )
            or 0
        )
        close_known = int(db.scalar(select(func.count()).where(base.c.closed_date.is_not(None)).select_from(base)) or 0)
        rating_ok = (
            (base.c.feedback_rating.is_not(None)) & (base.c.feedback_rating >= 1) & (base.c.feedback_rating <= 5)
        )
        rating_known = int(db.scalar(select(func.count()).where(rating_ok).select_from(base)) or 0)
        readiness = {
            "ai": {"pct": round(100.0 * ai_known / total, 1) if total else 0.0, "known": ai_known, "total": total},
            "close_date": {
                "pct": round(100.0 * close_known / total, 1) if total else 0.0,
                "known": close_known,
                "total": total,
            },
            "rating": {"pct": round(100.0 * rating_known / total, 1) if total else 0.0, "known": rating_known, "total": total},
        }

        sub_expr = func.coalesce(func.nullif(func.trim(base.c.ai_subtopic), ""), "General Civic Issue")
        pr = func.sum(func.coalesce(base.c.actionable_score, 0)).label("priority_sum")

        # Top subtopics (count + priority_sum)
        top_rows = db.execute(
            select(
                sub_expr.label("subTopic"),
                func.count().label("count"),
                pr,
                func.sum(case((func.trim(func.coalesce(base.c.ai_urgency, "")) == "High", 1), else_=0)).label("high_urgency_count"),
                func.sum(case((func.trim(func.coalesce(base.c.ai_urgency, "")) == "Medium", 1), else_=0)).label("med_urgency_count"),
                func.sum(case((func.trim(func.coalesce(base.c.ai_urgency, "")) == "Low", 1), else_=0)).label("low_urgency_count"),
                func.sum(case((rating_ok & (base.c.feedback_rating <= 2), 1), else_=0)).label("low_rating_count"),
                func.sum(case((rating_ok, 1), else_=0)).label("rated_count"),
            )
            .group_by(sub_expr)
            .order_by(func.count().desc())
            .limit(top_n)
        ).all()
        top_list = []
        for s, n, p, hu, mu, lu, lr, rated in top_rows:
            n = int(n or 0)
            rated = int(rated or 0)
            hu = int(hu or 0)
            mu = int(mu or 0)
            lu = int(lu or 0)
            lr = int(lr or 0)
            urgency = "Low"
            if hu >= mu and hu >= lu and hu > 0:
                urgency = "High"
            elif mu >= hu and mu >= lu and mu > 0:
                urgency = "Med"
            top_list.append(
                {
                    "subTopic": s,
                    "count": n,
                    "priority_sum": int(p or 0),
                    "high_urgency_pct": round(100.0 * hu / n, 1) if n else 0.0,
                    "low_rating_pct": round(100.0 * lr / rated, 1) if rated else None,
                    "urgency": urgency,
                    "urgency_counts": {"high": hu, "med": mu, "low": lu},
                }
            )
        top_subtopics = [r["subTopic"] for r in top_list]
        if not subtopic_focus and top_subtopics:
            subtopic_focus = top_subtopics[0]

        # Compute per-subtopic median SLA + avg rating for top list
        jd_days = func.julianday(base.c.closed_date) - func.julianday(base.c.created_date)
        closed_days = case(
            (
                (base.c.closed_date.is_not(None)) & (base.c.created_date.is_not(None)) & (jd_days >= 0),
                jd_days,
            ),
            else_=None,
        )
        closure_ok = case(
            ((base.c.resolution_days.is_not(None)) & (base.c.resolution_days >= 0), base.c.resolution_days),
            else_=closed_days,
        ).label("closure_days")
        rating_ok = case(
            (
                (base.c.feedback_rating.is_not(None)) & (base.c.feedback_rating >= 1) & (base.c.feedback_rating <= 5),
                base.c.feedback_rating,
            ),
            else_=None,
        ).label("rating")

        metrics_map = {s: {"closure": [], "rating": []} for s in top_subtopics}
        if top_subtopics:
            rows = db.execute(
                select(sub_expr.label("subTopic"), closure_ok, rating_ok)
                .where(sub_expr.in_(top_subtopics))
            ).all()
            for s, cd, rt in rows:
                if s not in metrics_map:
                    continue
                if cd is not None:
                    try:
                        metrics_map[s]["closure"].append(float(cd))
                    except Exception:
                        pass
                if rt is not None:
                    try:
                        metrics_map[s]["rating"].append(float(rt))
                    except Exception:
                        pass

        for r in top_list:
            m = metrics_map.get(r["subTopic"]) or {"closure": [], "rating": []}
            med = self._median(m["closure"])
            avg = (sum(m["rating"]) / len(m["rating"])) if m["rating"] else None
            pct_over_30 = (sum(1 for x in m["closure"] if x > 30) / len(m["closure"]) * 100.0) if m["closure"] else None
            r["median_sla_days"] = round(float(med), 2) if med is not None else None
            r["avg_rating"] = round(float(avg), 2) if avg is not None else None
            r["pct_over_30d"] = round(float(pct_over_30), 1) if pct_over_30 is not None else None
            # tooltip lines for richer UI
            r["tooltip_lines"] = [
                {"label": "Volume", "value": r["count"]},
                {"label": "Priority (sum)", "value": r["priority_sum"]},
                {"label": "Median SLA (days)", "value": r["median_sla_days"] if r["median_sla_days"] is not None else "—"},
                {"label": "Avg rating", "value": r["avg_rating"] if r["avg_rating"] is not None else "—"},
                {"label": "Tail >30d", "value": f'{r["pct_over_30d"]}%' if r["pct_over_30d"] is not None else "—"},
            ]

        # Callouts for slide 2 (volume / priority / urgency)
        volume_leader = max(top_list, key=lambda x: x.get("count", 0), default=None)
        priority_leader = max(top_list, key=lambda x: x.get("priority_sum", 0), default=None)
        # urgency leader: among subtopics with some urgency signal
        urgency_leader = max(top_list, key=lambda x: x.get("high_urgency_pct", 0), default=None)

        # =========================
        # Slide 3: Pain Matrix
        # =========================
        pain_points = []
        for r in top_list:
            x = r.get("median_sla_days")
            y = r.get("low_rating_pct")
            if x is None or y is None:
                continue
            if (r.get("count") or 0) <= 0:
                continue
            pain_points.append(
                {
                    "subTopic": r.get("subTopic") or "",
                    "count": int(r.get("count") or 0),
                    "median_sla_days": float(x),
                    "low_rating_pct": float(y),
                    "pct_over_30d": r.get("pct_over_30d"),
                    "urgency": r.get("urgency") or "Low",
                }
            )

        x_thr = self._median([p["median_sla_days"] for p in pain_points]) if pain_points else None
        y_thr = self._median([p["low_rating_pct"] for p in pain_points]) if pain_points else None
        x_thr = round(float(x_thr), 1) if x_thr is not None else 15.0
        y_thr = round(float(y_thr), 1) if y_thr is not None else 25.0

        def _pain_index(p: dict) -> float:
            # Composite pain = delay + dissatisfaction (+ SLA tail)
            x = float(p.get("median_sla_days") or 0)
            y = float(p.get("low_rating_pct") or 0)
            t = float(p.get("pct_over_30d") or 0)
            return x * 1.0 + y * 0.6 + t * 0.4

        top_painful = sorted(pain_points, key=_pain_index, reverse=True)[:5]
        top_painful_out = []
        for idx, p in enumerate(top_painful, start=1):
            status = "WATCH"
            if float(p.get("median_sla_days") or 0) >= x_thr and float(p.get("low_rating_pct") or 0) >= y_thr:
                status = "ACTION REQ"
            elif (p.get("urgency") or "").lower().startswith("high"):
                status = "CRITICAL"
            top_painful_out.append(
                {
                    "rank": idx,
                    "subTopic": p.get("subTopic") or "",
                    "status": status,
                    "count": int(p.get("count") or 0),
                    "median_sla_days": round(float(p.get("median_sla_days") or 0), 1),
                    "low_rating_pct": round(float(p.get("low_rating_pct") or 0), 1),
                    "pct_over_30d": p.get("pct_over_30d"),
                }
            )

        # One-of-a-kind complaints (unique subtopics)
        unique_min_priority = max(0, min(int(unique_min_priority or 0), 100))
        u_q = self._processed_filter_subquery(
            db,
            start_date=start_date,
            end_date=end_date,
            wards=wards,
            department=department,
            category=category,
            source=source,
        )
        u_sub = func.coalesce(func.nullif(func.trim(u_q.c.ai_subtopic), ""), "General Civic Issue")
        u_counts = (
            select(u_sub.label("subTopic"), func.count().label("cnt"))
            .group_by(u_sub)
            .having(func.count() == 1)
            .cte("unique_counts")
        )
        u_where = []
        if unique_min_priority > 0:
            u_where.append(func.coalesce(u_q.c.actionable_score, 0) >= unique_min_priority)
        if unique_confidence_high_only:
            u_where.append(func.coalesce(func.trim(u_q.c.ai_confidence), "") == "High")

        sel_unique = (
            select(
                u_q.c.grievance_id,
                u_q.c.created_date,
                u_q.c.ward_name,
                u_q.c.department_name,
                u_q.c.subject,
                u_sub.label("subTopic"),
                u_q.c.actionable_score.label("actionable_score"),
                u_q.c.ai_urgency.label("ai_urgency"),
                u_q.c.ai_sentiment.label("ai_sentiment"),
                u_q.c.ai_confidence,
                u_q.c.ai_entities_json,
            )
            .join(u_counts, u_counts.c.subTopic == u_sub)
            .order_by(u_q.c.created_date.desc())
        )
        if u_where:
            sel_unique = sel_unique.where(*u_where)
        unique_rows = db.execute(sel_unique).all()

        unique_out = []
        for gid, cd, wardn, deptn, subj, sub, score, urg, sent, conf, ent_json in unique_rows[:50]:
            ents = self._parse_entities(ent_json)
            unique_out.append(
                {
                    "grievance_id": gid,
                    "created_date": cd.strftime("%Y-%m-%d") if cd else "",
                    "ward": wardn or "",
                    "department": deptn or "",
                    "subTopic": sub or "",
                    "subject": (subj or "")[:180],
                    "actionable_score": int(score) if score is not None else None,
                    "urgency": urg or "",
                    "sentiment": sent or "",
                    "ai_confidence": conf or "",
                    "top_entity": (ents[0] if ents else ""),
                }
            )

        # Ward focus: derive from this dataset scope (trimmed) and default to TOP ward by volume.
        ward_expr = func.trim(base.c.ward_name)
        ward_opts = [w for (w,) in db.execute(select(func.distinct(ward_expr)).where(ward_expr.is_not(None), ward_expr != "")).all() if w]
        ward_opts.sort()
        if not ward_focus:
            top_ward = db.execute(
                select(ward_expr.label("ward"), func.count().label("cnt"))
                .where(ward_expr.is_not(None), ward_expr != "")
                .group_by(ward_expr)
                .order_by(func.count().desc())
                .limit(1)
            ).first()
            ward_focus = (top_ward[0] if top_ward else None) or (ward_opts[0] if ward_opts else None)

        ward_rows = []
        ward_entities = []
        ward_entities_coverage = {"known": 0, "total": 0, "pct": 0.0}
        if ward_focus:
            wq = self._processed_filter_subquery(
                db,
                start_date=start_date,
                end_date=end_date,
                wards=[ward_focus],
                department=department,
                category=category,
                source=source,
            )
            w_sub = func.coalesce(func.nullif(func.trim(wq.c.ai_subtopic), ""), "General Civic Issue")
            w_pr = func.sum(func.coalesce(wq.c.actionable_score, 0)).label("priority_sum")
            ward_rows_raw = db.execute(
                select(w_sub.label("subTopic"), func.count().label("count"), w_pr)
                .group_by(w_sub)
                .order_by(func.count().desc())
                .limit(by_ward_top_n)
            ).all()
            ward_rows = [{"subTopic": s, "count": int(n), "priority_sum": int(p or 0)} for (s, n, p) in ward_rows_raw]

            # Entities in ward (python explode)
            ward_total = int(db.scalar(select(func.count()).select_from(wq)) or 0)
            ward_known = int(
                db.scalar(
                    select(func.count())
                    .where((wq.c.ai_entities_json.is_not(None)) & (func.trim(wq.c.ai_entities_json) != ""))
                    .select_from(wq)
                )
                or 0
            )
            ward_entities_coverage = {
                "known": ward_known,
                "total": ward_total,
                "pct": round(100.0 * ward_known / ward_total, 1) if ward_total else 0.0,
            }
            ent_vals = db.execute(
                select(wq.c.ai_entities_json).where((wq.c.ai_entities_json.is_not(None)) & (func.trim(wq.c.ai_entities_json) != ""))
            ).scalars().all()
            ctr: Counter[str] = Counter()
            for s in ent_vals:
                for e in self._parse_entities(s):
                    ctr[e] += 1
            ward_entities = [{"entity": k, "count": int(v)} for k, v in ctr.most_common(entities_top_n)]

        # Department focus: derive from this dataset scope (trimmed) and default to TOP department by volume.
        dept_expr = func.trim(base.c.department_name)
        dept_opts = [d for (d,) in db.execute(select(func.distinct(dept_expr)).where(dept_expr.is_not(None), dept_expr != "")).all() if d]
        dept_opts.sort()
        if not department_focus:
            top_dept = db.execute(
                select(dept_expr.label("dept"), func.count().label("cnt"))
                .where(dept_expr.is_not(None), dept_expr != "")
                .group_by(dept_expr)
                .order_by(func.count().desc())
                .limit(1)
            ).first()
            department_focus = (top_dept[0] if top_dept else None) or (dept_opts[0] if dept_opts else None)

        dept_table = []
        if department_focus:
            dq = self._processed_filter_subquery(
                db,
                start_date=start_date,
                end_date=end_date,
                wards=wards,
                department=department_focus,
                category=category,
                source=source,
            )
            d_sub = func.coalesce(func.nullif(func.trim(dq.c.ai_subtopic), ""), "General Civic Issue")
            d_pr = func.sum(func.coalesce(dq.c.actionable_score, 0)).label("priority_sum")
            d_rows = db.execute(
                select(d_sub.label("subTopic"), func.count().label("count"), d_pr)
                .group_by(d_sub)
                .order_by(func.count().desc())
                .limit(by_dept_top_n)
            ).all()
            subs = [s for (s, _n, _p) in d_rows]
            d_map = {s: {"closure": [], "rating": []} for s in subs}
            if subs:
                d_jd = func.julianday(dq.c.closed_date) - func.julianday(dq.c.created_date)
                d_closed_days = case(
                    (
                        (dq.c.closed_date.is_not(None)) & (dq.c.created_date.is_not(None)) & (d_jd >= 0),
                        d_jd,
                    ),
                    else_=None,
                )
                d_closure_ok = case(
                    ((dq.c.resolution_days.is_not(None)) & (dq.c.resolution_days >= 0), dq.c.resolution_days),
                    else_=d_closed_days,
                ).label("closure_days")
                d_rating_ok = case(
                    (
                        (dq.c.feedback_rating.is_not(None)) & (dq.c.feedback_rating >= 1) & (dq.c.feedback_rating <= 5),
                        dq.c.feedback_rating,
                    ),
                    else_=None,
                ).label("rating")
                d_rows2 = db.execute(select(d_sub.label("subTopic"), d_closure_ok, d_rating_ok).where(d_sub.in_(subs))).all()
                for s, cd, rt in d_rows2:
                    if s not in d_map:
                        continue
                    if cd is not None:
                        try:
                            d_map[s]["closure"].append(float(cd))
                        except Exception:
                            pass
                    if rt is not None:
                        try:
                            d_map[s]["rating"].append(float(rt))
                        except Exception:
                            pass

            for s, n, p in d_rows:
                clos = d_map.get(s, {}).get("closure", [])
                rats = d_map.get(s, {}).get("rating", [])
                med = self._median(clos)
                pct_over_30 = (sum(1 for x in clos if x > 30) / len(clos) * 100.0) if clos else None
                avg_rt = (sum(rats) / len(rats)) if rats else None
                low_rt_pct = (sum(1 for x in rats if x <= 2) / len(rats) * 100.0) if rats else None
                dept_table.append(
                    {
                        "subTopic": s,
                        "count": int(n),
                        "priority_sum": int(p or 0),
                        "median_resolution_days": round(float(med), 2) if med is not None else None,
                        "pct_over_30d": round(float(pct_over_30), 1) if pct_over_30 is not None else None,
                        "avg_rating": round(float(avg_rt), 2) if avg_rt is not None else None,
                        "low_rating_pct": round(float(low_rt_pct), 1) if low_rt_pct is not None else None,
                    }
                )

        # Monthly trend for selected subtopic (count + avg actionable score)
        trend_months = []
        if subtopic_focus:
            tq = self._processed_filter_subquery(
                db,
                start_date=start_date,
                end_date=end_date,
                wards=wards,
                department=department,
                category=category,
                source=source,
            )
            t_sub = func.coalesce(func.nullif(func.trim(tq.c.ai_subtopic), ""), "General Civic Issue")
            rows = db.execute(
                select(
                    tq.c.created_month.label("month"),
                    func.count().label("count"),
                    func.avg(func.coalesce(tq.c.actionable_score, 0)).label("avg_actionable_score"),
                    func.sum(func.coalesce(tq.c.actionable_score, 0)).label("priority_sum"),
                )
                .where(t_sub == subtopic_focus)
                .group_by(tq.c.created_month)
                .order_by(tq.c.created_month.asc())
            ).all()
            for m, c, avg_a, ps in rows:
                if not m:
                    continue
                trend_months.append(
                    {
                        "month": m,
                        "count": int(c or 0),
                        "avg_actionable_score": round(float(avg_a), 2) if avg_a is not None else None,
                        "priority_sum": int(ps or 0),
                    }
                )

        return {
            "filters": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "wards": wards or [],
                "department": department,
                "category": category,
                "source": source,
            },
            "readiness": readiness,
            "load_view": {
                "metric_default": "volume",
                "total_records_analyzed": total,
                "source": source or "",
                "top_subtopics": top_list,
                "callouts": {
                    "volume_leader": volume_leader,
                    "priority_leader": priority_leader,
                    "urgent_leader": urgency_leader,
                },
            },
            "focus": {
                "ward": ward_focus or "",
                "department": department_focus or "",
                "subtopic": subtopic_focus or "",
            },
            "options": {
                "wards": ward_opts,
                "departments": dept_opts,
                "subtopics": top_subtopics,
            },
            "top_subtopics": top_list,
            "one_of_a_kind": {
                "definition": "Sub-Topics with exactly 1 complaint in the selected filters.",
                "filters": {"min_priority": unique_min_priority, "confidence_high_only": unique_confidence_high_only},
                "rows": unique_out[:25],
            },
            "by_ward": {"ward": ward_focus or "", "rows": ward_rows},
            "ward_entities": ward_entities,
            "ward_entities_coverage": ward_entities_coverage,
            "by_department": {"department": department_focus or "", "rows": dept_table[:10]},
            "trend": {"subTopic": subtopic_focus or "", "months": trend_months},
            "pain_matrix": {
                "x_threshold_days": x_thr,
                "y_threshold_low_rating_pct": y_thr,
                "points": pain_points,
                "top_painful": top_painful_out,
                "definition": "Bubble chart: X=Median Resolution Days, Y=Low-rating% (1-2★), Bubble size=Volume, Color=Urgency.",
            },
        }

    def executive_overview(
        self,
        db: Session,
        *,
        start_date: dt.date,
        end_date: dt.date,
        wards: list[str] | None = None,
        department: str | None = None,
        ai_category: str | None = None,
        source: str | None = None,
        top_n: int = 10,
    ) -> dict:
        top_n = max(3, min(int(top_n or 10), 15))

        q = select(GrievanceProcessed).where(
            GrievanceProcessed.created_date.is_not(None),
            GrievanceProcessed.created_date >= start_date,
            GrievanceProcessed.created_date <= end_date,
        )
        if wards:
            q = q.where(GrievanceProcessed.ward_name.in_(wards))
        if department:
            q = q.where(GrievanceProcessed.department_name == department)
        if ai_category:
            q = q.where(GrievanceProcessed.ai_category == ai_category)
        if source:
            q = q.where(GrievanceProcessed.source_raw_filename == source)

        base = q.subquery()

        total = db.scalar(select(func.count()).select_from(base)) or 0

        # status breakdown
        status_rows = db.execute(
            select(base.c.status, func.count())
            .group_by(base.c.status)
            .order_by(func.count().desc())
        ).all()
        status_breakdown = [{"status": (s or "Unknown"), "count": int(n)} for (s, n) in status_rows]

        # AI fields: treat NULL/"" as General Civic Issue for subtopics
        sub_expr = func.coalesce(func.nullif(func.trim(base.c.ai_subtopic), ""), "General Civic Issue")
        cat_expr = func.coalesce(func.nullif(func.trim(base.c.ai_category), ""), "Other Civic Issues")

        cat_rows = db.execute(
            select(cat_expr.label("ai_category"), func.count())
            .group_by(cat_expr)
            .order_by(func.count().desc())
            .limit(top_n)
        ).all()
        top_categories = [{"category": c, "count": int(n)} for (c, n) in cat_rows]

        sub_rows = db.execute(
            select(sub_expr.label("ai_subtopic"), func.count())
            .group_by(sub_expr)
            .order_by(func.count().desc())
            .limit(top_n)
        ).all()
        top_subtopics = [{"subTopic": s, "count": int(n)} for (s, n) in sub_rows]

        # daily trend
        trend_rows = db.execute(
            select(base.c.created_date, func.count())
            .where(base.c.created_date.is_not(None))
            .group_by(base.c.created_date)
            .order_by(base.c.created_date.asc())
        ).all()
        grievances_over_time = [
            {"date": d.strftime("%Y-%m-%d"), "count": int(n)} for (d, n) in trend_rows if d
        ]

        # avg feedback rating (1..5)
        rating_ok = case(
            (
                (base.c.feedback_rating.is_not(None))
                & (base.c.feedback_rating >= 1)
                & (base.c.feedback_rating <= 5),
                base.c.feedback_rating,
            ),
            else_=None,
        )
        avg_feedback = db.execute(select(func.avg(rating_ok))).scalar()
        avg_feedback = round(float(avg_feedback), 2) if avg_feedback is not None else None

        # avg closure time days
        # Prefer precomputed resolution_days; fall back to closed_date-created_date if needed.
        jd_days = func.julianday(base.c.closed_date) - func.julianday(base.c.created_date)
        closed_days = case(
            (
                (base.c.closed_date.is_not(None))
                & (base.c.created_date.is_not(None))
                & (jd_days >= 0),
                jd_days,
            ),
            else_=None,
        )
        closure_ok = case(
            (
                (base.c.resolution_days.is_not(None)) & (base.c.resolution_days >= 0),
                base.c.resolution_days,
            ),
            else_=closed_days,
        )
        avg_closure = db.execute(select(func.avg(closure_ok))).scalar()
        avg_closure = round(float(avg_closure), 2) if avg_closure is not None else None

        return {
            "total_grievances": int(total),
            "avg_feedback_rating": avg_feedback,
            "avg_closure_time_days": avg_closure,
            "status_breakdown": status_breakdown,
            "top_categories": top_categories,
            "top_subtopics": top_subtopics,
            "grievances_over_time": grievances_over_time,
            "powered_by_caseA_flags": {
                "top_categories": True,
                "top_subtopics": True,
                "status_breakdown": False,
                "grievances_over_time": False,
                "avg_feedback_rating": False,
                "avg_closure_time_days": False,
            },
        }

    def top_subtopics(
        self,
        db: Session,
        *,
        start_date: dt.date,
        end_date: dt.date,
        wards: list[str] | None = None,
        department: str | None = None,
        ai_category: str | None = None,
        source: str | None = None,
        top_n: int = 10,
    ) -> dict:
        top_n = max(1, min(int(top_n or 10), 25))
        q = select(GrievanceProcessed.ai_subtopic).where(
            GrievanceProcessed.created_date.is_not(None),
            GrievanceProcessed.created_date >= start_date,
            GrievanceProcessed.created_date <= end_date,
        )
        if wards:
            q = q.where(GrievanceProcessed.ward_name.in_(wards))
        if department:
            q = q.where(GrievanceProcessed.department_name == department)
        if ai_category:
            q = q.where(GrievanceProcessed.ai_category == ai_category)
        if source:
            q = q.where(GrievanceProcessed.source_raw_filename == source)
        base = q.subquery()
        sub_expr = func.coalesce(func.nullif(func.trim(base.c.ai_subtopic), ""), "General Civic Issue")
        rows = db.execute(
            select(sub_expr.label("subTopic"), func.count())
            .group_by(sub_expr)
            .order_by(func.count().desc())
            .limit(top_n)
        ).all()
        return {"rows": [{"subTopic": s, "count": int(n)} for (s, n) in rows], "top_n": top_n}

    def top_subtopics_by_ward(
        self,
        db: Session,
        *,
        start_date: dt.date,
        end_date: dt.date,
        ward: str,
        department: str | None = None,
        ai_category: str | None = None,
        source: str | None = None,
        top_n: int = 5,
    ) -> dict:
        ward = (ward or "").strip()
        if not ward:
            return {"ward": "", "rows": [], "top_n": int(top_n or 5)}
        top_n = max(1, min(int(top_n or 5), 15))
        q = select(GrievanceProcessed.ai_subtopic).where(
            GrievanceProcessed.created_date.is_not(None),
            GrievanceProcessed.created_date >= start_date,
            GrievanceProcessed.created_date <= end_date,
            GrievanceProcessed.ward_name == ward,
        )
        if department:
            q = q.where(GrievanceProcessed.department_name == department)
        if ai_category:
            q = q.where(GrievanceProcessed.ai_category == ai_category)
        if source:
            q = q.where(GrievanceProcessed.source_raw_filename == source)
        base = q.subquery()
        sub_expr = func.coalesce(func.nullif(func.trim(base.c.ai_subtopic), ""), "General Civic Issue")
        rows = db.execute(
            select(sub_expr.label("subTopic"), func.count())
            .group_by(sub_expr)
            .order_by(func.count().desc())
            .limit(top_n)
        ).all()
        return {"ward": ward, "rows": [{"subTopic": s, "count": int(n)} for (s, n) in rows], "top_n": top_n}

    def top_subtopics_by_department(
        self,
        db: Session,
        *,
        start_date: dt.date,
        end_date: dt.date,
        department: str,
        wards: list[str] | None = None,
        ai_category: str | None = None,
        source: str | None = None,
        top_n: int = 10,
    ) -> dict:
        department = (department or "").strip()
        if not department:
            return {"department": "", "rows": [], "top_n": int(top_n or 10)}
        top_n = max(1, min(int(top_n or 10), 25))
        q = select(GrievanceProcessed.ai_subtopic).where(
            GrievanceProcessed.created_date.is_not(None),
            GrievanceProcessed.created_date >= start_date,
            GrievanceProcessed.created_date <= end_date,
            GrievanceProcessed.department_name == department,
        )
        if wards:
            q = q.where(GrievanceProcessed.ward_name.in_(wards))
        if ai_category:
            q = q.where(GrievanceProcessed.ai_category == ai_category)
        if source:
            q = q.where(GrievanceProcessed.source_raw_filename == source)
        base = q.subquery()
        sub_expr = func.coalesce(func.nullif(func.trim(base.c.ai_subtopic), ""), "General Civic Issue")
        rows = db.execute(
            select(sub_expr.label("subTopic"), func.count())
            .group_by(sub_expr)
            .order_by(func.count().desc())
            .limit(top_n)
        ).all()
        return {
            "department": department,
            "rows": [{"subTopic": s, "count": int(n)} for (s, n) in rows],
            "top_n": top_n,
        }

    def subtopic_trend(
        self,
        db: Session,
        *,
        start_date: dt.date,
        end_date: dt.date,
        subtopic: str,
        wards: list[str] | None = None,
        department: str | None = None,
        ai_category: str | None = None,
        source: str | None = None,
    ) -> dict:
        subtopic = (subtopic or "").strip()
        if not subtopic:
            return {"subTopic": "", "months": []}
        # Month-wise: uses created_month derived during preprocessing (fast).
        q = select(GrievanceProcessed.created_month).where(
            GrievanceProcessed.created_date.is_not(None),
            GrievanceProcessed.created_date >= start_date,
            GrievanceProcessed.created_date <= end_date,
            func.coalesce(func.nullif(func.trim(GrievanceProcessed.ai_subtopic), ""), "General Civic Issue") == subtopic,
        )
        if wards:
            q = q.where(GrievanceProcessed.ward_name.in_(wards))
        if department:
            q = q.where(GrievanceProcessed.department_name == department)
        if ai_category:
            q = q.where(GrievanceProcessed.ai_category == ai_category)
        if source:
            q = q.where(GrievanceProcessed.source_raw_filename == source)
        base = q.subquery()
        rows = db.execute(
            select(base.c.created_month, func.count())
            .group_by(base.c.created_month)
            .order_by(base.c.created_month.asc())
        ).all()
        months = [{"month": (m or ""), "count": int(n)} for (m, n) in rows if m]
        return {"subTopic": subtopic, "months": months}

    def one_of_a_kind_complaints(
        self,
        db: Session,
        *,
        start_date: dt.date,
        end_date: dt.date,
        wards: list[str] | None = None,
        department: str | None = None,
        ai_category: str | None = None,
        source: str | None = None,
        limit: int = 25,
    ) -> dict:
        """
        "One-of-a-kind" = sub-topics that appear exactly once in the selected date range + filters.
        Returns the single underlying complaint row for each such sub-topic.
        """
        limit = max(5, min(int(limit or 25), 100))

        q = self._processed_base(
            start_date=start_date,
            end_date=end_date,
            wards=wards,
            department=department,
            ai_category=ai_category,
            source=source,
        ).subquery()
        sub_expr = func.coalesce(func.nullif(func.trim(q.c.ai_subtopic), ""), "General Civic Issue")

        counts = (
            select(sub_expr.label("subTopic"), func.count().label("cnt"))
            .group_by(sub_expr)
            .having(func.count() == 1)
            .cte("one_counts")
        )

        rows = db.execute(
            select(
                q.c.grievance_id,
                q.c.created_date,
                q.c.ward_name,
                q.c.department_name,
                q.c.ai_category,
                sub_expr.label("subTopic"),
                q.c.subject,
            )
            .join(counts, counts.c.subTopic == sub_expr)
            .order_by(q.c.created_date.desc())
            .limit(limit)
        ).all()

        out = []
        for gid, cd, ward, dept, cat, sub, subj in rows:
            out.append(
                {
                    "grievance_id": gid,
                    "created_date": cd.strftime("%Y-%m-%d") if cd else "",
                    "ward": ward or "",
                    "department": dept or "",
                    "ai_category": cat or "",
                    "ai_subtopic": sub or "",
                    "subject": (subj or "")[:180],
                }
            )

        return {"definition": "Sub-Topics with exactly 1 complaint in the selected filters.", "rows": out, "limit": limit}

    def subtopics_top(
        self,
        db: Session,
        f: Filters,
        *,
        limit: int = 10,
        include_general_min_pct: float = 0.2,
    ) -> dict:
        """
        Top AI sub-topics overall (uses stored GrievanceStructured.sub_issue).
        Excludes empty values. Excludes "General Civic Issue" unless it exceeds a threshold.
        """
        base = self._base(db, f).subquery()
        total = db.scalar(select(func.count()).select_from(base)) or 0

        limit = max(1, min(int(limit or 10), 25))

        rows = db.execute(
            select(GrievanceStructured.sub_issue, func.count(GrievanceStructured.id))
            .join(GrievanceRaw, GrievanceRaw.id == GrievanceStructured.raw_id)
            .where(GrievanceRaw.id.in_(select(base.c.id)))
            .where(GrievanceStructured.sub_issue.is_not(None))
            .where(func.trim(GrievanceStructured.sub_issue) != "")
            .group_by(GrievanceStructured.sub_issue)
            .order_by(func.count(GrievanceStructured.id).desc())
        ).all()

        out = []
        for sub, n in rows:
            sub = str(sub or "").strip()
            if not sub:
                continue
            pct = (float(n) / float(total)) if total else 0.0
            if sub == "General Civic Issue" and pct < float(include_general_min_pct):
                continue
            out.append({"subTopic": sub, "count": int(n), "pct": round(pct, 4)})
            if len(out) >= limit:
                break

        return {"total": int(total), "limit": limit, "rows": out, "ai_meta": self._ai_meta(db)}

    def subtopics_by_ward(self, db: Session, f: Filters, *, ward: str, limit: int = 5) -> dict:
        """
        Top AI sub-topics for a specific ward (stored sub_issue).
        """
        ward = (ward or "").strip()
        if not ward:
            return {"ward": "", "total": 0, "limit": int(limit or 5), "rows": [], "ai_meta": self._ai_meta(db)}

        f2 = Filters(start_date=f.start_date, end_date=f.end_date, wards=[ward], department=f.department, category=f.category)
        base = self._base(db, f2).subquery()
        total = db.scalar(select(func.count()).select_from(base)) or 0
        limit = max(1, min(int(limit or 5), 15))

        rows = db.execute(
            select(GrievanceStructured.sub_issue, func.count(GrievanceStructured.id))
            .join(GrievanceRaw, GrievanceRaw.id == GrievanceStructured.raw_id)
            .where(GrievanceRaw.id.in_(select(base.c.id)))
            .where(GrievanceStructured.sub_issue.is_not(None))
            .where(func.trim(GrievanceStructured.sub_issue) != "")
            .group_by(GrievanceStructured.sub_issue)
            .order_by(func.count(GrievanceStructured.id).desc())
            .limit(limit)
        ).all()

        out = []
        for sub, n in rows:
            sub = str(sub or "").strip()
            if not sub:
                continue
            pct = (float(n) / float(total)) if total else 0.0
            out.append({"subTopic": sub, "count": int(n), "pct": round(pct, 4)})

        return {"ward": ward, "total": int(total), "limit": limit, "rows": out, "ai_meta": self._ai_meta(db)}

    def subtopics_by_department(self, db: Session, f: Filters, *, department: str, limit: int = 10) -> dict:
        """
        Top AI sub-topics for a specific department (stored sub_issue).
        """
        department = (department or "").strip()
        if not department:
            return {"department": "", "total": 0, "limit": int(limit or 10), "rows": [], "ai_meta": self._ai_meta(db)}

        f2 = Filters(start_date=f.start_date, end_date=f.end_date, wards=f.wards, department=department, category=f.category)
        base = self._base(db, f2).subquery()
        total = db.scalar(select(func.count()).select_from(base)) or 0
        limit = max(1, min(int(limit or 10), 25))

        rows = db.execute(
            select(GrievanceStructured.sub_issue, func.count(GrievanceStructured.id))
            .join(GrievanceRaw, GrievanceRaw.id == GrievanceStructured.raw_id)
            .where(GrievanceRaw.id.in_(select(base.c.id)))
            .where(GrievanceStructured.sub_issue.is_not(None))
            .where(func.trim(GrievanceStructured.sub_issue) != "")
            .group_by(GrievanceStructured.sub_issue)
            .order_by(func.count(GrievanceStructured.id).desc())
            .limit(limit)
        ).all()

        out = []
        for sub, n in rows:
            sub = str(sub or "").strip()
            if not sub:
                continue
            pct = (float(n) / float(total)) if total else 0.0
            out.append({"subTopic": sub, "count": int(n), "pct": round(pct, 4)})

        return {"department": department, "total": int(total), "limit": limit, "rows": out, "ai_meta": self._ai_meta(db)}

    def subtopics_trend(self, db: Session, f: Filters, *, subtopic: str) -> dict:
        """
        Month-wise trend for one sub-topic.
        Uses DB aggregation when possible (SQLite), falls back to Python grouping otherwise.
        """
        subtopic = (subtopic or "").strip()
        if not subtopic:
            return {"subTopic": "", "total": 0, "months": [], "ai_meta": self._ai_meta(db)}

        base = self._base(db, f).subquery()

        if str(settings.database_url).startswith("sqlite:"):
            month_expr = func.strftime("%Y-%m", GrievanceRaw.created_date)
            rows = db.execute(
                select(month_expr, func.count(GrievanceRaw.id))
                .join(GrievanceStructured, GrievanceStructured.raw_id == GrievanceRaw.id)
                .where(GrievanceRaw.id.in_(select(base.c.id)))
                .where(GrievanceStructured.sub_issue == subtopic)
                .where(GrievanceRaw.created_date.is_not(None))
                .group_by(month_expr)
                .order_by(month_expr.asc())
            ).all()
            months = [{"month": (m or ""), "count": int(n)} for (m, n) in rows if m]
            total = sum(x["count"] for x in months)
            return {"subTopic": subtopic, "total": int(total), "months": months, "ai_meta": self._ai_meta(db)}

        # Fallback (portable)
        rows2 = db.execute(
            select(GrievanceRaw.created_date)
            .join(GrievanceStructured, GrievanceStructured.raw_id == GrievanceRaw.id)
            .where(GrievanceRaw.id.in_(select(base.c.id)))
            .where(GrievanceStructured.sub_issue == subtopic)
            .where(GrievanceRaw.created_date.is_not(None))
        ).scalars().all()
        by_month: dict[str, int] = defaultdict(int)
        for d in rows2:
            if not d:
                continue
            by_month[d.strftime("%Y-%m")] += 1
        months = [{"month": k, "count": int(by_month[k])} for k in sorted(by_month.keys())]
        total = sum(by_month.values())
        return {"subTopic": subtopic, "total": int(total), "months": months, "ai_meta": self._ai_meta(db)}


