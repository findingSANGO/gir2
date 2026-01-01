from __future__ import annotations

import datetime as dt
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

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
        wards.sort()
        depts.sort()
        cats.sort()
        return {"wards": wards, "departments": depts, "categories": cats}

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

        base = self._processed_base(start_date=prev_start, end_date=end_date, wards=wards, department=department, ai_category=ai_category).subquery()
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

        base = self._processed_base(start_date=prev_start, end_date=end_date, wards=wards, department=department, ai_category=ai_category).subquery()
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

        base_q = self._processed_base(start_date=start_date, end_date=end_date, wards=wards, department=department, ai_category=ai_category).subquery()
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
        retro = self.retrospective(db, f)
        why = self.inferential(db, f)
        return {
            "ai_meta": retro.get("ai_meta") or why.get("ai_meta"),
            "feedbackDistribution": retro["feedbackDistribution"],
            "avgFeedback": retro["totals"]["avgFeedback"],
            "lowFeedbackDrivers": why["drivers"],
            "insights": (retro["insights"] + why["insights"])[:5],
        }

    def closure(self, db: Session, f: Filters) -> dict:
        # Closure buckets + avg by category and ward
        base = self._base(db, f).subquery()
        ai_meta = self._ai_meta(db)
        joined = db.execute(
            select(GrievanceStructured.category, GrievanceRaw.ward, GrievanceRaw.created_date, GrievanceRaw.closed_date)
            .join(GrievanceRaw, GrievanceRaw.id == GrievanceStructured.raw_id)
            .where(GrievanceRaw.id.in_(select(base.c.id)))
        ).all()
        by_cat: dict[str, list[int]] = defaultdict(list)
        by_ward: dict[str, list[int]] = defaultdict(list)
        bucket_counts = Counter()
        for cat, ward, a, b in joined:
            d = _closure_days(a, b)
            bucket_counts[_bucket(d)] += 1
            if d is not None:
                by_cat[cat].append(d)
                by_ward[ward or "Unknown"].append(d)

        avg_by_cat = [{"category": k, "avgDays": round(sum(v)/len(v), 2), "count": len(v)} for k, v in by_cat.items() if v]
        avg_by_ward = [{"ward": k, "avgDays": round(sum(v)/len(v), 2), "count": len(v)} for k, v in by_ward.items() if v]
        avg_by_cat.sort(key=lambda x: x["avgDays"], reverse=True)
        avg_by_ward.sort(key=lambda x: x["avgDays"], reverse=True)
        buckets = [{"bucket": b, "count": bucket_counts.get(b, 0)} for b in ["<7", "7-14", ">14", "Unknown"]]

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

    def executive_overview(
        self,
        db: Session,
        *,
        start_date: dt.date,
        end_date: dt.date,
        wards: list[str] | None = None,
        department: str | None = None,
        ai_category: str | None = None,
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

        return {
            "total_grievances": int(total),
            "avg_feedback_rating": None,
            "avg_closure_time_days": None,
            "status_breakdown": status_breakdown,
            "top_categories": top_categories,
            "top_subtopics": top_subtopics,
            "grievances_over_time": grievances_over_time,
            "powered_by_caseA_flags": {
                "top_categories": True,
                "top_subtopics": True,
                "status_breakdown": False,
                "grievances_over_time": False,
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
        limit: int = 25,
    ) -> dict:
        """
        "One-of-a-kind" = sub-topics that appear exactly once in the selected date range + filters.
        Returns the single underlying complaint row for each such sub-topic.
        """
        limit = max(5, min(int(limit or 25), 100))

        q = self._processed_base(start_date=start_date, end_date=end_date, wards=wards, department=department, ai_category=ai_category).subquery()
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


