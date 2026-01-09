import { useContext, useEffect, useMemo, useState } from "react";
import { LineCard, VerticalBarCard } from "../components/Charts.jsx";
import { api } from "../services/api.js";
import { FiltersContext } from "../App.jsx";
import { colorForKey } from "../utils/chartColors.js";
import {
  BarChart3,
  ClipboardList,
  Database,
  TriangleAlert,
  AlertTriangle,
  ListOrdered
} from "lucide-react";
import { Card, CardContent } from "../components/ui/Card.jsx";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  LabelList,
  ReferenceArea,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis
} from "recharts";

function fmtRange(startISO, endISO) {
  function fmt(iso) {
    const d = iso ? new Date(`${iso}T00:00:00`) : null;
    if (!d || Number.isNaN(d.getTime())) return "—";
    return d.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
  }
  return `${fmt(startISO)} — ${fmt(endISO)}`;
}

function SlideTabs({ slides, active, onChange }) {
  return (
    // Important: overflow-x for scroll is fine, but overflow-y must remain visible
    // otherwise Tailwind `ring-*` (box-shadow) gets clipped and pills look “cut off”.
    <div className="flex items-center gap-2 overflow-x-auto overflow-y-visible py-1 pr-1 -my-1">
      {slides.map((s, idx) => {
        const isActive = idx === active;
        return (
          <button
            key={s.key}
            type="button"
            onClick={() => onChange(idx)}
            className={
              "whitespace-nowrap rounded-full px-3 py-1.5 text-xs font-semibold ring-1 transition " +
              (isActive
                ? "bg-slateink-900 text-white ring-slateink-900"
                : "bg-white text-slateink-700 ring-slateink-200 hover:bg-slateink-50 hover:ring-slateink-300")
            }
          >
            {s.label}
          </button>
        );
      })}
    </div>
  );
}

function MetricToggle({ value, onChange }) {
  return (
    <div className="flex items-center gap-2 rounded-full bg-white/10 ring-1 ring-white/15 px-3 py-2">
      <div className="text-xs font-semibold text-white/80">METRIC TOGGLE</div>
      <div className="text-xs font-semibold text-white/80">Volume</div>
      <button
        type="button"
        onClick={() => onChange(value === "volume" ? "priority" : "volume")}
        className={
          "relative h-6 w-11 rounded-full ring-1 ring-white/20 transition " +
          (value === "priority" ? "bg-gov-500" : "bg-white/20")
        }
        aria-label="Toggle Volume/Priority"
      >
        <span
          className={
            "absolute top-0.5 h-5 w-5 rounded-full bg-white transition " +
            (value === "priority" ? "left-5" : "left-0.5")
          }
        />
      </button>
      <div className="text-xs font-semibold text-white/80">Priority</div>
    </div>
  );
}

function ProgressRow({ label, pct, subtitle }) {
  const p = Math.max(0, Math.min(100, Number(pct || 0)));
  return (
    <div>
      <div className="flex items-center justify-between">
        <div className="text-xs font-semibold text-slateink-600">{label}</div>
        <div className="text-sm font-semibold text-slateink-900">{Number(p || 0).toFixed(0)}%</div>
      </div>
      <div className="mt-2 h-2 rounded-full bg-slateink-100">
        <div className="h-2 rounded-full bg-gov-600" style={{ width: `${p}%` }} />
      </div>
      {subtitle ? <div className="mt-2 text-[11px] text-slateink-500">{subtitle}</div> : null}
    </div>
  );
}

function dotColor(urgency) {
  const u = String(urgency || "").toLowerCase();
  if (u.startsWith("high")) return "#ef4444"; // red-500
  if (u.startsWith("med")) return "#f59e0b"; // amber-500
  return "#3b82f6"; // blue-500
}

function zoneFill(zone) {
  // Soft, projector-friendly tints (low opacity in ReferenceArea)
  if (zone === "healthy") return "#22c55e"; // green
  if (zone === "fast_unhappy") return "#f59e0b"; // amber
  if (zone === "slow_ok") return "#3b82f6"; // blue
  return "#ef4444"; // red (priority)
}

function shortLabel(s) {
  const t = String(s || "").replace(/\s+/g, " ").trim();
  if (!t) return "";
  const parts = t.split(" ").filter(Boolean);
  const keep = parts.slice(0, 3).join(" ");
  return keep.length > 16 ? `${keep.slice(0, 16)}…` : keep;
}

function StatusPill({ status }) {
  const s = String(status || "").toUpperCase();
  const cls =
    s === "ACTION REQ"
      ? "bg-rose-50 text-rose-700 ring-rose-200"
      : s === "CRITICAL"
        ? "bg-slateink-900 text-white ring-slateink-900"
        : "bg-slateink-50 text-slateink-700 ring-slateink-200";
  return <span className={`inline-flex items-center rounded-full px-3 py-1 text-xs font-semibold ring-1 ${cls}`}>{s}</span>;
}

function BubbleShape(props) {
  const { cx, cy, size, payload, fill, hoveredSubtopic, showLabel } = props;
  if (cx == null || cy == null) return null;
  // NOTE: recharts passes `size` already scaled by ZAxis.range.
  // Treat it as "area-ish" and convert to a sane radius so large volumes don't
  // flood the whole chart.
  const s = Number(size || 0);
  const r = Math.max(7, Math.sqrt(Math.max(0, s)) * 0.9);
  const isActive = hoveredSubtopic && payload?.subTopic === hoveredSubtopic;
  const label = showLabel ? String(payload?.subTopic || "") : "";
  const txt = shortLabel(label);
  return (
    <g>
      <circle
        cx={cx}
        cy={cy}
        r={r}
        fill={fill}
        fillOpacity={isActive ? 0.78 : 0.60}
        stroke="#ffffff"
        strokeOpacity={0.95}
        strokeWidth={isActive ? 2.2 : 1.4}
      />
      {showLabel && txt ? (
        <text
          x={cx}
          y={cy}
          textAnchor="middle"
          dominantBaseline="central"
          fontSize={10}
          fill="#0b1220"
          opacity={isActive ? 0.9 : 0.75}
          style={{ pointerEvents: "none" }}
        >
          {txt}
        </text>
      ) : null}
    </g>
  );
}

function PainTooltip({ active, payload }) {
  if (!active || !payload?.length) return null;
  const p = payload[0]?.payload || {};
  return (
    <div className="rounded-xl bg-white p-3 shadow-lg ring-1 ring-slateink-200">
      <div className="text-sm font-semibold text-slateink-900">{p.subTopic || "—"}</div>
      <div className="mt-2 space-y-1 text-xs text-slateink-700">
        <div className="flex items-center justify-between gap-6">
          <span className="text-slateink-500">Volume</span>
          <span className="font-semibold">{p.count ?? "—"}</span>
        </div>
        <div className="flex items-center justify-between gap-6">
          <span className="text-slateink-500">Median SLA</span>
          <span className="font-semibold">{p.median_sla_days != null ? `${p.median_sla_days}d` : "—"}</span>
        </div>
        <div className="flex items-center justify-between gap-6">
          <span className="text-slateink-500">Low rating</span>
          <span className="font-semibold">{p.low_rating_pct != null ? `${p.low_rating_pct}%` : "—"}</span>
        </div>
        <div className="flex items-center justify-between gap-6">
          <span className="text-slateink-500">Tail &gt;30d</span>
          <span className="font-semibold">{p.pct_over_30d != null ? `${p.pct_over_30d}%` : "—"}</span>
        </div>
        <div className="flex items-center justify-between gap-6">
          <span className="text-slateink-500">Urgency</span>
          <span className="font-semibold">{p.urgency || "—"}</span>
        </div>
      </div>
    </div>
  );
}

function niceAxisMaxDays(v) {
  // Goal: dynamic scaling, but avoid huge empty space when max is ~6–7 days.
  // Keep a minimum visible scale of 10 days (as typical median is around ~7).
  const raw = Number(v || 0);
  const padded = raw > 0 ? raw * 1.15 : 0; // light headroom
  const base = Math.max(10, padded);

  // "Nice" rounding: 0–20 by 5s, 20–60 by 10s, above by 20s.
  const step = base <= 20 ? 5 : base <= 60 ? 10 : 20;
  return Math.max(10, Math.ceil(base / step) * step);
}

export default function IssueIntelligence2() {
  const { filters } = useContext(FiltersContext);
  const [payload, setPayload] = useState(null);
  const [overviewData, setOverviewData] = useState(null);
  const [sla, setSla] = useState(null);
  const [fwd, setFwd] = useState(null);
  const [fwdImpact, setFwdImpact] = useState(null);
  const [slide, setSlide] = useState(0);
  const [metric, setMetric] = useState("volume"); // volume | priority
  const [error, setError] = useState("");
  const [overviewError, setOverviewError] = useState("");
  const [slaError, setSlaError] = useState("");
  const [fwdError, setFwdError] = useState("");
  const [fwdImpactError, setFwdImpactError] = useState("");
  const [hoverSubtopic, setHoverSubtopic] = useState("");
  const [loadTrendSubtopic, setLoadTrendSubtopic] = useState("");
  const [loadTrend, setLoadTrend] = useState(null);
  const [loadTrendLoading, setLoadTrendLoading] = useState(false);
  const [loadTrendError, setLoadTrendError] = useState("");

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await api.issueIntelligenceV2({
          ...filters
        });
        if (cancelled) return;
        setPayload(res);
      } catch (e) {
        if (!cancelled) setError(e.message || "Failed to load Issue Intelligence");
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    filters.start_date,
    filters.end_date,
    (filters.wards || []).join(","),
    filters.department,
    filters.category,
    filters.source
  ]);

  // Pull in Executive Overview time-series (reuse the same chart on Issue Intelligence 2 → Overview)
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        setOverviewError("");
        const res = await api.executiveOverviewV2({ ...filters });
        if (cancelled) return;
        setOverviewData(res);
      } catch (e) {
        if (cancelled) return;
        setOverviewData(null);
        setOverviewError(e.message || "Failed to load overview time series");
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    filters.start_date,
    filters.end_date,
    (filters.wards || []).join(","),
    filters.department,
    filters.category,
    filters.source
  ]);

  // Dedicated SLA Snapshot widget
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        setSlaError("");
        const res = await api.closureSlaSnapshot({ ...filters });
        if (cancelled) return;
        setSla(res);
      } catch (e) {
        if (cancelled) return;
        setSla(null);
        setSlaError(e.message || "Failed to load SLA snapshot");
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    filters.start_date,
    filters.end_date,
    (filters.wards || []).join(","),
    filters.department,
    filters.category,
    filters.source
  ]);

  // Dedicated Forwarding Analytics widget
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        setFwdError("");
        const res = await api.forwardingSnapshot({ ...filters });
        if (cancelled) return;
        setFwd(res);
      } catch (e) {
        if (cancelled) return;
        setFwd(null);
        setFwdError(e.message || "Failed to load forwarding snapshot");
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    filters.start_date,
    filters.end_date,
    (filters.wards || []).join(","),
    filters.department,
    filters.category,
    filters.source
  ]);

  // Forwarding impact on resolution time (process tax)
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        setFwdImpactError("");
        const res = await api.forwardingImpactResolution({ ...filters });
        if (cancelled) return;
        setFwdImpact(res);
      } catch (e) {
        if (cancelled) return;
        setFwdImpact(null);
        setFwdImpactError(e.message || "Failed to load forwarding impact");
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    filters.start_date,
    filters.end_date,
    (filters.wards || []).join(","),
    filters.department,
    filters.category,
    filters.source
  ]);

  if (error) {
    return <div className="rounded-xl bg-white p-4 ring-1 ring-slateink-100 text-sm text-rose-700">{error}</div>;
  }

  const slides = [
    { key: "overview", label: "Overview" },
    { key: "load", label: "Issue Intelligence" },
    { key: "pain", label: "Priority Matrix" },
    { key: "hotspots", label: "Hotspots" },
    { key: "exceptions", label: "Exceptions" },
    { key: "ward", label: "Ward Ownership" },
    { key: "dept", label: "Department Performance" },
    { key: "trend", label: "Trend Analysis" },
    { key: "signals", label: "Root Signals" },
    { key: "week", label: "This Week" }
  ];

  const periodLabel = fmtRange(filters?.start_date, filters?.end_date);

  const readiness = payload?.readiness || null;
  const load = payload?.load_view || {};
  const top = load?.top_subtopics || [];
  const chartRows = useMemo(() => {
    const sorted = [...top].sort((a, b) =>
      metric === "priority"
        ? Number(b.priority_sum || 0) - Number(a.priority_sum || 0)
        : Number(b.count || 0) - Number(a.count || 0)
    );
    return sorted.slice(0, 10).map((r) => ({
      ...r,
      subTopic: r.subTopic,
      value: metric === "priority" ? r.priority_sum : r.count
    }));
  }, [top, metric]);

  const loadTrendOptions = useMemo(() => {
    // Use the same Top Sub-Topics list for the trend selector (keeps UX tight + relevant)
    const rows = payload?.load_view?.top_subtopics || [];
    const uniq = new Set();
    const out = [];
    for (const r of rows) {
      const s = String(r?.subTopic || "").trim();
      if (!s || uniq.has(s)) continue;
      uniq.add(s);
      out.push(s);
    }
    return out;
  }, [payload]);

  // Default the Load View trend selector to the top-ranked subtopic.
  useEffect(() => {
    if (slide !== 1) return;
    if (loadTrendSubtopic) return;
    if (!loadTrendOptions.length) return;
    setLoadTrendSubtopic(loadTrendOptions[0]);
  }, [slide, loadTrendSubtopic, loadTrendOptions]);

  // Load View: month-wise trend for the selected subtopic (respects global Deep Dive filters).
  useEffect(() => {
    if (slide !== 1) return;
    if (!loadTrendSubtopic) return;
    // IMPORTANT: load processed-data trend by reusing Issue Intelligence V2 with `subtopic_focus`.
    // This keeps trend consistent with the file-pipeline dataset powering Deep Dive.
    let cancelled = false;
    (async () => {
      try {
        setLoadTrendError("");
        // Keep the previous trend visible while we fetch the next one (prevents flicker).
        setLoadTrendLoading(true);
        const res = await api.issueIntelligenceV2({ ...filters, subtopic_focus: loadTrendSubtopic });
        if (cancelled) return;
        const months = res?.trend?.months || [];
        const out = months.map((m) => ({ month: m.month, count: Number(m.count || 0) }));
        setLoadTrend({ subTopic: loadTrendSubtopic, months: out });
        setLoadTrendLoading(false);
      } catch (e) {
        if (cancelled) return;
        setLoadTrendLoading(false);
        setLoadTrendError(e.message || "Failed to load sub-topic trend");
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    slide,
    loadTrendSubtopic,
    filters.start_date,
    filters.end_date,
    (filters.wards || []).join(","),
    filters.department,
    filters.category,
    filters.source
  ]);

  const callouts = load?.callouts || {};
  const pain = payload?.pain_matrix || null;
  const painPointsRaw = pain?.points || [];
  const painTop = pain?.top_painful || [];
  const painX = pain?.x_threshold_days ?? null;
  const painY = pain?.y_threshold_low_rating_pct ?? null;

  const painPoints = useMemo(() => {
    return [...painPointsRaw].sort((a, b) => Number(b.count || 0) - Number(a.count || 0));
  }, [painPointsRaw]);

  const painXMax = useMemo(() => {
    const xs = (painPoints || [])
      .map((p) => Number(p?.median_sla_days))
      .filter((n) => Number.isFinite(n) && n >= 0);
    const mx = xs.length ? Math.max(...xs) : 0;
    return niceAxisMaxDays(mx);
  }, [painPoints]);

  const painXTicks = useMemo(() => {
    const max = Number(painXMax || 10);
    const step = max <= 20 ? 5 : max <= 60 ? 10 : 20;
    const out = [];
    for (let x = 0; x <= max + 1e-9; x += step) out.push(Number(x.toFixed(6)));
    return out;
  }, [painXMax]);

  const painHigh = useMemo(() => painPoints.filter((p) => String(p.urgency || "").toLowerCase().startsWith("high")), [painPoints]);
  const painMed = useMemo(() => painPoints.filter((p) => String(p.urgency || "").toLowerCase().startsWith("med")), [painPoints]);
  const painLow = useMemo(() => painPoints.filter((p) => !String(p.urgency || "").toLowerCase().match(/^(high|med)/)), [painPoints]);

  const topLabelSet = useMemo(() => new Set((painTop || []).map((r) => r.subTopic)), [painTop]);
  const urgencyBySubtopic = useMemo(() => {
    const m = new Map();
    for (const p of painPoints) m.set(p.subTopic, p.urgency);
    return m;
  }, [painPoints]);

  const trend = useMemo(() => {
    const pts = overviewData?.time_series_daily?.rows || [];
    return pts.slice(Math.max(0, pts.length - 45)).map((r) => ({ day: r.day, created: r.created, closed: r.closed }));
  }, [overviewData]);

  const showClosedLine = Boolean(overviewData?.time_series_daily?.show_closed);

  const slaN = Number(sla?.based_on?.closed_n || 0);
  const dist = sla?.distribution?.rows || [];
  const distRows = useMemo(() => {
    return (dist || []).map((r) => ({
      bucket: r.bucket,
      pct: Number(r.pct || 0),
      band: r.band || "standard"
    }));
  }, [dist]);

  const fwdTotal = Number(fwd?.based_on?.total_n || 0);
  const fwdN = Number(fwd?.based_on?.forwarded_n || 0);
  const hopRows = useMemo(() => {
    function prettyBucket(b) {
      const s = String(b || "");
      if (s.toLowerCase().startsWith("1 hop")) return "1 Time";
      if (s.toLowerCase().startsWith("2 hop")) return "2 Times";
      if (s.toLowerCase().startsWith("3+")) return "3+ Times";
      // fallback: handle numeric-like buckets
      if (s.trim() === "1") return "1 Time";
      if (s.trim() === "2") return "2 Times";
      if (s.trim() === "3+") return "3+ Times";
      return s;
    }
    return (fwd?.distribution?.hops || []).map((r) => ({
      bucket: prettyBucket(r.bucket),
      count: Number(r.count || 0),
      band: r.band || "standard"
    }));
  }, [fwd]);

  const impactClosedN = Number(fwdImpact?.based_on?.closed_n || 0);
  const directDist = useMemo(() => {
    return (fwdImpact?.direct?.distribution || []).map((r) => ({ bucket: r.bucket, pct: Number(r.pct || 0) }));
  }, [fwdImpact]);
  const forwardedDist = useMemo(() => {
    return (fwdImpact?.forwarded?.distribution || []).map((r) => ({ bucket: r.bucket, pct: Number(r.pct || 0) }));
  }, [fwdImpact]);

  function fmt2(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return "—";
    return n.toFixed(2);
  }
  function fmtPct(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return "—";
    return `${n.toFixed(2)}%`;
  }
  function fmtPct0(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return "—";
    return `${Math.round(n)}%`;
  }

  return (
    <div className="space-y-5">
      {/* Navigation pills (global filter bar is above this page in Shell) */}
      <div
        className="sticky z-30 bg-slateink-50/80 backdrop-blur border-b border-slateink-100 -mx-4 lg:-mx-6 px-4 lg:px-6 py-1.5"
        style={{ top: "calc(var(--cgda-chrome-h, 128px) + 1px)" }}
      >
        <div className="flex items-center">
          <SlideTabs slides={slides} active={slide} onChange={setSlide} />
        </div>
      </div>

      {/* Slides container */}
      <div className="rounded-2xl bg-white ring-1 ring-slateink-100 overflow-hidden">
        <div className="p-5">
          {/* Slide 1 */}
          {slide === 0 ? (
            <div className="space-y-5">
              {/* Removed (per request): Data Readiness + Dashboard Purpose panels */}

              {overviewError ? (
                <div className="rounded-xl bg-white p-4 ring-1 ring-slateink-100 text-sm text-rose-700">{overviewError}</div>
              ) : (
                <LineCard
                  title="Grievances Over Time"
                  subtitle="Created vs Closed"
                  data={trend}
                  xKey="day"
                  lines={[
                    { key: "created", name: "Created", color: "#2b54f6" },
                    ...(showClosedLine ? [{ key: "closed", name: "Closed", color: "#16a34a" }] : [])
                  ]}
                  height={320}
                  showLegend
                />
              )}

              <div className="rounded-2xl bg-white ring-1 ring-slateink-100 overflow-hidden">
                <div className="px-5 py-4 border-b border-slateink-100 flex items-start justify-between gap-4">
                  <div>
                    <div className="text-xl font-semibold text-slateink-900">Closure Timeliness (SLA)</div>
                    <div className="mt-1 text-sm text-slateink-500">
                      <span className="font-semibold text-slateink-700">Valid closures:</span> N = {slaN.toLocaleString()}
                    </div>
                  </div>
                </div>

                <div className="p-5">
                  {slaError ? (
                    <div className="rounded-xl bg-white p-4 ring-1 ring-slateink-100 text-sm text-rose-700">{slaError}</div>
                  ) : !sla ? (
                    <div className="rounded-xl border border-dashed border-slateink-200 bg-white/40 px-4 py-10 text-center text-sm text-slateink-500">
                      Loading…
                    </div>
                  ) : slaN <= 0 ? (
                    <div className="rounded-xl border border-dashed border-slateink-200 bg-white/40 px-4 py-10 text-center text-sm text-slateink-500">
                      No closure data for this selection (needs valid close_date).
                    </div>
                  ) : (
                    <>
                      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                        <div className="rounded-2xl bg-white ring-1 ring-slateink-200 p-4 shadow-card">
                          <div className="text-xs font-semibold text-blue-700">MEDIAN RESOLUTION</div>
                          <div className="mt-2 flex items-baseline gap-2">
                            <div className="text-4xl font-semibold text-slateink-900">{fmt2(sla?.kpis?.median_days)}</div>
                            <div className="text-lg font-semibold text-slateink-500">days</div>
                          </div>
                        </div>

                        <div className="rounded-2xl bg-white ring-1 ring-slateink-200 p-4 shadow-card border-b-4 border-emerald-500">
                          <div className="text-xs font-semibold text-emerald-700">WITHIN 1 DAY</div>
                          <div className="mt-2 text-4xl font-semibold text-slateink-900">{fmtPct(sla?.kpis?.within_1d_pct)}</div>
                        </div>

                        <div className="rounded-2xl bg-white ring-1 ring-slateink-200 p-4 shadow-card">
                          <div className="text-xs font-semibold text-slateink-900">WITHIN 7 DAYS</div>
                          <div className="mt-2 text-4xl font-semibold text-slateink-900">{fmtPct(sla?.kpis?.within_7d_pct)}</div>
                        </div>

                        <div className="rounded-2xl bg-white ring-1 ring-slateink-200 p-4 shadow-card">
                          <div className="text-xs font-semibold text-rose-700">&gt; 30 DAYS</div>
                          <div className="mt-2 text-4xl font-semibold text-rose-600">{fmtPct(sla?.kpis?.over_30d_pct)}</div>
                        </div>
                      </div>

                      <div className="mt-4 rounded-2xl bg-white ring-1 ring-slateink-200 overflow-hidden">
                        <div className="px-5 py-4 border-b border-slateink-100 flex items-center justify-between gap-3">
                          <div className="text-lg font-semibold text-slateink-900">Resolution Time Distribution (Days)</div>
                        </div>
                        <div className="p-4">
                          <div className="h-[260px]">
                            <ResponsiveContainer width="100%" height="100%">
                              <BarChart data={distRows} margin={{ top: 10, right: 16, bottom: 34, left: 8 }}>
                                <CartesianGrid strokeDasharray="2 10" stroke="#eceef2" vertical={false} />
                                <XAxis
                                  dataKey="bucket"
                                  height={44}
                                  tick={{ fontSize: 12, fill: "#64748b" }}
                                  tickMargin={10}
                                  axisLine={false}
                                  tickLine={false}
                                  interval={0}
                                  label={{ value: "Time taken to resolve", position: "bottom", offset: 18, fill: "#94a3b8", fontSize: 11 }}
                                />
                                <YAxis
                                  tick={{ fontSize: 12, fill: "#64748b" }}
                                  axisLine={false}
                                  tickLine={false}
                                  domain={[0, 50]}
                                  ticks={[0, 10, 20, 30, 40, 50]}
                                  tickFormatter={(v) => `${v}%`}
                                />
                                <Tooltip formatter={(v) => `${Number(v || 0).toFixed(2)}%`} />
                                <Bar dataKey="pct" radius={[10, 10, 4, 4]} maxBarSize={72}>
                                  {(distRows || []).map((_, idx) => (
                                    <Cell key={idx} fill="#2563eb" opacity={0.9} />
                                  ))}
                                  <LabelList
                                    dataKey="pct"
                                    position="center"
                                    fill="#ffffff"
                                    fontSize={12}
                                    fontWeight={700}
                                    formatter={(v) => `${Math.round(Number(v || 0))}%`}
                                  />
                                </Bar>
                              </BarChart>
                            </ResponsiveContainer>
                          </div>
                        </div>
                      </div>
                    </>
                  )}
                </div>
              </div>

              <div className="rounded-2xl bg-white ring-1 ring-slateink-100 overflow-hidden">
                <div className="px-5 py-4 border-b border-slateink-100 flex items-start justify-between gap-4">
                  <div>
                    <div className="text-xl font-semibold text-slateink-900">Forwarding Analytics</div>
                    <div className="mt-1 text-sm text-slateink-500">Operational friction &amp; routing efficiency</div>
                  </div>
                  <div className="text-xs font-semibold text-slateink-400">{fwd?.as_of ? String(fwd.as_of) : ""}</div>
                </div>

                <div className="p-5">
                  {fwdError ? (
                    <div className="rounded-xl bg-white p-4 ring-1 ring-slateink-100 text-sm text-rose-700">{fwdError}</div>
                  ) : !fwd ? (
                    <div className="rounded-xl border border-dashed border-slateink-200 bg-white/40 px-4 py-10 text-center text-sm text-slateink-500">
                      Loading…
                    </div>
                  ) : fwdTotal <= 0 ? (
                    <div className="rounded-xl border border-dashed border-slateink-200 bg-white/40 px-4 py-10 text-center text-sm text-slateink-500">
                      No records for this selection.
                    </div>
                  ) : (
                    <>
                      <div className="grid gap-3 lg:grid-cols-2">
                        <div className="rounded-2xl bg-white ring-1 ring-slateink-200 p-4 shadow-card">
                          <div className="text-xs font-semibold text-blue-700">GRIEVANCES FORWARDED</div>
                          <div className="mt-2 text-4xl font-semibold text-slateink-900">
                            {fmtPct(fwd?.kpis?.forwarded_pct)}
                          </div>
                          <div className="mt-2 text-xs text-slateink-500">Routing intervention required</div>
                        </div>

                        <div className="rounded-2xl bg-white ring-1 ring-slateink-200 p-4 shadow-card">
                          <div className="text-xs font-semibold text-slateink-900">MEDIAN FORWARD DELAY</div>
                          <div className="mt-2 flex items-baseline gap-2">
                            <div className="text-4xl font-semibold text-slateink-900">{fmt2(fwd?.kpis?.median_forward_delay_days)}</div>
                            <div className="text-lg font-semibold text-slateink-500">days</div>
                          </div>
                          <div className="mt-2 text-xs text-slateink-500">Time lost before reaching concerned officer</div>
                        </div>
                      </div>

                      <div className="mt-4 grid gap-4 lg:grid-cols-12">
                        <div className="lg:col-span-8 rounded-2xl bg-white ring-1 ring-slateink-200 overflow-hidden">
                          <div className="px-5 py-4 border-b border-slateink-100 flex items-center justify-between gap-3">
                            <div className="text-lg font-semibold text-slateink-900">Forwarding Events Distribution</div>
                            <div className="text-xs font-semibold text-slateink-500">
                              Among forwarded grievances (n ≈ {fwdN.toLocaleString()})
                            </div>
                          </div>
                          <div className="p-4">
                            <div className="h-[260px]">
                              <ResponsiveContainer width="100%" height="100%">
                                <BarChart data={hopRows} margin={{ top: 12, right: 16, bottom: 34, left: 8 }}>
                                  <CartesianGrid strokeDasharray="2 10" stroke="#eceef2" vertical={false} />
                                  <XAxis
                                    dataKey="bucket"
                                    height={44}
                                    tick={{ fontSize: 12, fill: "#64748b" }}
                                    tickMargin={10}
                                    axisLine={false}
                                    tickLine={false}
                                    interval={0}
                                    label={{ value: "Forwarded", position: "bottom", offset: 18, fill: "#94a3b8", fontSize: 11 }}
                                  />
                                  <YAxis
                                    tick={{ fontSize: 12, fill: "#64748b" }}
                                    axisLine={false}
                                    tickLine={false}
                                    label={{
                                      value: "Number of Grievances",
                                      angle: -90,
                                      position: "insideLeft",
                                      offset: 0,
                                      fill: "#94a3b8",
                                      fontSize: 11
                                    }}
                                  />
                                  <Tooltip />
                                  <Bar dataKey="count" radius={[10, 10, 4, 4]} maxBarSize={90}>
                                    {(hopRows || []).map((_, idx) => (
                                      <Cell key={idx} fill="#2563eb" opacity={0.9} />
                                    ))}
                                    <LabelList
                                      dataKey="count"
                                      position="center"
                                      fill="#ffffff"
                                      fontSize={14}
                                      fontWeight={800}
                                      formatter={(v) => String(Number(v || 0).toLocaleString())}
                                    />
                                  </Bar>
                                </BarChart>
                              </ResponsiveContainer>
                            </div>
                          </div>
                        </div>

                        <div className="lg:col-span-4 rounded-2xl bg-white ring-1 ring-slateink-200 overflow-hidden">
                          <div className="px-5 py-4 border-b border-slateink-100 flex items-center gap-2">
                            <div className="text-lg font-semibold text-slateink-900">Multiple Hops (Ping-Pong)</div>
                          </div>
                          <div className="p-5 space-y-4">
                            <div className="flex items-center justify-between gap-3">
                              <div className="flex items-center gap-3">
                                <div className="h-12 w-12 rounded-full bg-indigo-50 ring-1 ring-indigo-100 flex items-center justify-center text-indigo-700 font-semibold">
                                  2+
                                </div>
                                <div>
                                  <div className="text-sm font-semibold text-slateink-900">Re-forwarded</div>
                                  <div className="text-xs text-slateink-500">Grievances moved &gt;2 times</div>
                                </div>
                              </div>
                              <div className="text-right">
                                <div className="text-3xl font-semibold text-slateink-900">
                                  {Number(fwd?.multiple_hops?.reforwarded_ge2 || 0).toLocaleString()}
                                </div>
                                <div className="text-xs font-semibold text-indigo-600">High Friction</div>
                              </div>
                            </div>

                            <div className="flex items-center justify-between gap-3">
                              <div className="flex items-center gap-3">
                                <div className="h-12 w-12 rounded-full bg-rose-50 ring-1 ring-rose-100 flex items-center justify-center text-rose-700 font-semibold">
                                  3+
                                </div>
                                <div>
                                  <div className="text-sm font-semibold text-slateink-900">Chronic Routing Issues</div>
                                  <div className="text-xs text-slateink-500">Grievances moved &gt;3 times</div>
                                </div>
                              </div>
                              <div className="text-right">
                                <div className="text-3xl font-semibold text-slateink-900">
                                  {Number(fwd?.multiple_hops?.chronic_ge3 || 0).toLocaleString()}
                                </div>
                                <div className="text-xs font-semibold text-rose-600">Critical Waste</div>
                              </div>
                            </div>

                            <div className="rounded-2xl bg-white ring-1 ring-slateink-200 p-4 text-sm text-slateink-700 leading-relaxed">
                              <span className="font-semibold">Insight:</span>{" "}
                              {String(fwd?.insight || "").replace(/^Insight:\s*/i, "")}
                            </div>
                          </div>
                        </div>
                      </div>
                    </>
                  )}
                </div>
              </div>

              <div className="rounded-2xl bg-white ring-1 ring-slateink-100 overflow-hidden">
                <div className="px-5 py-4 border-b border-slateink-100 flex items-start justify-between gap-4">
                  <div>
                    <div className="text-xl font-semibold text-slateink-900">Forwarding Impact on Resolution Time</div>
                    <div className="mt-1 text-sm text-slateink-500">The delay analysis</div>
                  </div>
                  <div className="text-xs font-semibold text-slateink-400">{fwdImpact?.as_of ? String(fwdImpact.as_of) : ""}</div>
                </div>

                <div className="p-5">
                  {fwdImpactError ? (
                    <div className="rounded-xl bg-white p-4 ring-1 ring-slateink-100 text-sm text-rose-700">{fwdImpactError}</div>
                  ) : !fwdImpact ? (
                    <div className="rounded-xl border border-dashed border-slateink-200 bg-white/40 px-4 py-10 text-center text-sm text-slateink-500">
                      Loading…
                    </div>
                  ) : impactClosedN <= 0 ? (
                    <div className="rounded-xl border border-dashed border-slateink-200 bg-white/40 px-4 py-10 text-center text-sm text-slateink-500">
                      No closure data for this selection (needs valid close_date).
                    </div>
                  ) : (
                    <div className="grid gap-4 lg:grid-cols-2 relative">
                      {/* Direct */}
                      <div className="rounded-2xl bg-white ring-1 ring-slateink-200 overflow-hidden">
                        <div className="px-5 py-4 border-b border-slateink-100 flex items-center justify-between gap-3">
                          <div className="flex items-center gap-3">
                            <div>
                              <div className="text-lg font-semibold text-slateink-900">Direct Closure (Not Forwarded)</div>
                              <div className="text-xs font-semibold text-slateink-500">Efficient Path</div>
                            </div>
                          </div>
                        </div>
                        <div className="p-5">
                          <div className="grid grid-cols-2 gap-4">
                            <div>
                              <div className="text-xs font-semibold text-slateink-500">MEDIAN TIME</div>
                              <div className="mt-2 flex items-baseline gap-2">
                                <div className="text-4xl font-semibold text-emerald-700">{fmt2(fwdImpact?.direct?.median_days)}</div>
                                <div className="text-lg font-semibold text-slateink-500">days</div>
                              </div>
                            </div>
                            <div>
                              <div className="text-xs font-semibold text-slateink-500">MEAN TIME</div>
                              <div className="mt-2 flex items-baseline gap-2">
                                <div className="text-4xl font-semibold text-slateink-900">{fmt2(fwdImpact?.direct?.mean_days)}</div>
                                <div className="text-lg font-semibold text-slateink-500">days</div>
                              </div>
                            </div>
                          </div>

                          <div className="mt-5 text-sm font-semibold text-slateink-700">Resolution Time Distribution</div>
                          <div className="mt-3 h-[200px]">
                            <ResponsiveContainer width="100%" height="100%">
                              <BarChart data={directDist} margin={{ top: 6, right: 8, bottom: 6, left: 0 }}>
                                <CartesianGrid strokeDasharray="2 10" stroke="#eceef2" vertical={false} />
                                <XAxis dataKey="bucket" tick={{ fontSize: 11, fill: "#64748b" }} axisLine={false} tickLine={false} interval={0} />
                                <YAxis tick={{ fontSize: 11, fill: "#64748b" }} axisLine={false} tickLine={false} tickFormatter={(v) => `${v}%`} />
                                <Tooltip formatter={(v) => `${Number(v || 0).toFixed(2)}%`} />
                                <Bar dataKey="pct" radius={[10, 10, 4, 4]} maxBarSize={56}>
                                  {(directDist || []).map((_, idx) => (
                                    <Cell key={idx} fill={idx <= 1 ? "#22c55e" : "#4ade80"} opacity={0.9} />
                                  ))}
                                </Bar>
                              </BarChart>
                            </ResponsiveContainer>
                          </div>
                        </div>
                      </div>

                      {/* VS pill */}
                      <div className="hidden lg:flex items-center justify-center absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2">
                        <div className="h-12 w-12 rounded-full bg-white shadow-card ring-1 ring-slateink-200 flex items-center justify-center text-slateink-600 font-bold">
                          VS
                        </div>
                      </div>

                      {/* Forwarded */}
                      <div className="rounded-2xl bg-white ring-1 ring-slateink-200 overflow-hidden">
                        <div className="px-5 py-4 border-b border-slateink-100 flex items-center justify-between gap-3">
                          <div className="flex items-center gap-3">
                            <div>
                              <div className="text-lg font-semibold text-slateink-900">Re-routed (Forwarded)</div>
                              <div className="text-xs font-semibold text-slateink-500">High Friction</div>
                            </div>
                          </div>
                          {fwdImpact?.comparison?.median_uplift_pct != null ? (
                            <div className="rounded-lg bg-rose-50 ring-1 ring-rose-100 px-3 py-1 text-sm font-semibold text-rose-700">
                              +{fmtPct0(fwdImpact?.comparison?.median_uplift_pct)} vs Direct
                            </div>
                          ) : null}
                        </div>
                        <div className="p-5">
                          <div className="grid grid-cols-2 gap-4">
                            <div>
                              <div className="text-xs font-semibold text-slateink-500">MEDIAN TIME</div>
                              <div className="mt-2 flex items-baseline gap-2">
                                <div className="text-4xl font-semibold text-rose-600">{fmt2(fwdImpact?.forwarded?.median_days)}</div>
                                <div className="text-lg font-semibold text-slateink-500">days</div>
                              </div>
                            </div>
                            <div>
                              <div className="text-xs font-semibold text-slateink-500">MEAN TIME</div>
                              <div className="mt-2 flex items-baseline gap-2">
                                <div className="text-4xl font-semibold text-slateink-900">{fmt2(fwdImpact?.forwarded?.mean_days)}</div>
                                <div className="text-lg font-semibold text-slateink-500">days</div>
                              </div>
                            </div>
                          </div>

                          <div className="mt-5 text-sm font-semibold text-slateink-700">Resolution Time Distribution</div>
                          <div className="mt-3 h-[200px]">
                            <ResponsiveContainer width="100%" height="100%">
                              <BarChart data={forwardedDist} margin={{ top: 6, right: 8, bottom: 6, left: 0 }}>
                                <CartesianGrid strokeDasharray="2 10" stroke="#eceef2" vertical={false} />
                                <XAxis dataKey="bucket" tick={{ fontSize: 11, fill: "#64748b" }} axisLine={false} tickLine={false} interval={0} />
                                <YAxis tick={{ fontSize: 11, fill: "#64748b" }} axisLine={false} tickLine={false} tickFormatter={(v) => `${v}%`} />
                                <Tooltip formatter={(v) => `${Number(v || 0).toFixed(2)}%`} />
                                <Bar dataKey="pct" radius={[10, 10, 4, 4]} maxBarSize={56}>
                                  {(forwardedDist || []).map((r, idx) => (
                                    <Cell
                                      key={idx}
                                      fill={idx >= 2 ? "#ef4444" : "#fb7185"}
                                      opacity={0.92}
                                    />
                                  ))}
                                </Bar>
                              </BarChart>
                            </ResponsiveContainer>
                          </div>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>
          ) : null}

          {/* Slide 2 */}
          {slide === 1 ? (
            <div className="space-y-4">
              <div className="rounded-2xl bg-gradient-to-br from-slateink-950 via-slateink-900 to-indigo-950 ring-1 ring-slateink-900/30 px-5 py-4">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div className="text-white">
                    <div className="text-lg font-semibold">AI enabled Subtopic Analysis</div>
                  </div>
                  <MetricToggle value={metric} onChange={setMetric} />
                </div>
              </div>

              <div className="grid gap-4 lg:grid-cols-12">
                <div className="lg:col-span-8">
                  <VerticalBarCard
                    title={`Top 10 Sub-Topics by ${metric === "priority" ? "Priority" : "Volume"}`}
                    subtitle={undefined}
                    data={chartRows}
                    yKey="subTopic"
                    valueKey="value"
                    height={460}
                    total={metric === "priority" ? null : Number(chartRows.reduce((a, r) => a + Number(r.count || 0), 0))}
                  />
                </div>
                <div className="lg:col-span-4">
                  <div className="text-xs font-semibold tracking-wide text-slateink-500">KEY INSIGHTS</div>
                  <div className="mt-3 space-y-3">
                    <Card>
                      <CardContent>
                        <div className="flex items-center justify-between">
                          <div className="text-[11px] font-semibold text-slateink-500">HIGHEST VOLUME SUB-TOPIC</div>
                          <div className="rounded-md bg-indigo-50 ring-1 ring-indigo-100 px-2 py-1 text-[10px] font-semibold text-indigo-700">
                            VOLUME LEADER
                          </div>
                        </div>
                        <div className="mt-2 text-sm font-semibold text-slateink-900">{callouts?.volume_leader?.subTopic || "—"}</div>
                        <div className="mt-1 text-2xl font-semibold text-indigo-700">
                          {callouts?.volume_leader?.count ?? "—"}
                          <span className="ml-2 text-xs font-semibold text-slateink-500">grievances</span>
                        </div>
                      </CardContent>
                    </Card>

                    <Card>
                      <CardContent>
                        <div className="flex items-center justify-between">
                          <div className="text-[11px] font-semibold text-slateink-500">HIGHEST PRIORITY INDEX</div>
                          <div className="rounded-md bg-slateink-50 ring-1 ring-slateink-100 px-2 py-1 text-[10px] font-semibold text-slateink-700">
                            TOP PRIORITY
                          </div>
                        </div>
                        <div className="mt-2 text-sm font-semibold text-slateink-900">{callouts?.priority_leader?.subTopic || "—"}</div>
                        <div className="mt-1 text-2xl font-semibold text-slateink-900">
                          {callouts?.priority_leader?.priority_sum ?? "—"}
                          <span className="ml-2 text-xs font-semibold text-slateink-500">score sum</span>
                        </div>
                      </CardContent>
                    </Card>

                    <Card className="ring-1 ring-rose-200 bg-rose-50/30">
                      <CardContent>
                        <div className="flex items-center justify-between">
                          <div className="text-[11px] font-semibold text-slateink-500">MOST URGENT SUB-TOPIC</div>
                          <div className="rounded-md bg-rose-50 ring-1 ring-rose-200 px-2 py-1 text-[10px] font-semibold text-rose-700">
                            CRITICAL
                          </div>
                        </div>
                        <div className="mt-2 text-sm font-semibold text-slateink-900">{callouts?.urgent_leader?.subTopic || "—"}</div>
                        <div className="mt-1 text-2xl font-semibold text-rose-700">
                          {callouts?.urgent_leader?.high_urgency_pct ?? "—"}%
                          <span className="ml-2 text-xs font-semibold text-slateink-500">High Urgency</span>
                        </div>
                      </CardContent>
                    </Card>
                  </div>
                </div>
              </div>

              {/* Copy from Issue Intelligence: Month-wise Sub-Topic Trends (shown at bottom of Load View) */}
              {loadTrendError ? (
                <div className="rounded-xl bg-white p-4 ring-1 ring-slateink-100 text-sm text-rose-700">{loadTrendError}</div>
              ) : (
                <LineCard
                  title="Month-wise Sub-Topic Trends"
                  subtitle="Counts grouped by month (Created Date)."
                  right={
                    <div className="flex flex-wrap items-center gap-2">
                      {loadTrendLoading ? (
                        <span className="text-xs font-semibold text-slateink-500">Updating…</span>
                      ) : null}
                      <span className="text-xs text-slateink-500">Sub-Topic</span>
                      <select
                        value={loadTrendSubtopic}
                        onChange={(e) => setLoadTrendSubtopic(e.target.value)}
                        className="h-9 rounded-xl border border-slateink-200 bg-white px-3 text-xs font-semibold outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100 max-w-[360px]"
                      >
                        {loadTrendOptions.map((s) => (
                          <option key={s} value={s}>
                            {s}
                          </option>
                        ))}
                      </select>
                    </div>
                  }
                  data={loadTrend ? loadTrend.months || [] : null}
                  xKey="month"
                  lines={[{ key: "count", name: "Grievances", color: colorForKey(loadTrendSubtopic) }]}
                  height={340}
                />
              )}
            </div>
          ) : null}

          {/* Slide 3 */}
          {slide === 2 ? (
            <div className="space-y-4">
              <div className="rounded-2xl bg-gradient-to-br from-slateink-950 via-slateink-900 to-indigo-950 ring-1 ring-slateink-900/30 px-5 py-4">
                <div className="flex flex-wrap items-center justify-between gap-4">
                  <div className="flex items-center gap-3 text-white">
                    <div className="h-11 w-11 rounded-2xl bg-rose-600 flex items-center justify-center">
                      <AlertTriangle className="h-5 w-5" />
                    </div>
                    <div>
                      <div className="text-lg font-semibold">Pain View: Sub-Topic Pain Matrix</div>
                      <div className="text-xs text-white/70">Where to intervene • Resolution Time vs. Citizen Satisfaction</div>
                    </div>
                  </div>

                  <div className="flex flex-wrap items-center gap-4 text-xs text-white/80">
                    <div className="inline-flex items-center gap-2">
                      <span className="h-3 w-3 rounded-full" style={{ background: dotColor("High") }} />
                      High Urgency
                    </div>
                    <div className="inline-flex items-center gap-2">
                      <span className="h-3 w-3 rounded-full" style={{ background: dotColor("Med") }} />
                      Med Urgency
                    </div>
                    <div className="inline-flex items-center gap-2">
                      <span className="h-3 w-3 rounded-full" style={{ background: dotColor("Low") }} />
                      Low Urgency
                    </div>
                    <div className="h-4 w-px bg-white/20" />
                    <div className="inline-flex items-center gap-2">
                      <span className="inline-flex items-center gap-1">
                        <span className="h-2 w-2 rounded-full ring-1 ring-white/40" />
                        <span className="h-3 w-3 rounded-full ring-1 ring-white/40" />
                        <span className="h-4 w-4 rounded-full ring-1 ring-white/40" />
                      </span>
                      Bubble Size = Volume
                    </div>
                  </div>
                </div>
              </div>

              <div className="grid gap-4 lg:grid-cols-12">
                <div className="lg:col-span-8">
                  <div className="rounded-2xl bg-white ring-1 ring-slateink-200 overflow-hidden">
                    <div className="px-5 py-4 border-b border-slateink-100">
                      <div className="text-xl font-semibold text-slateink-900">Pain Matrix</div>
                      <div className="mt-1 text-sm text-slateink-500">
                        Sub-topics positioned by delay vs citizen dissatisfaction. Focus on the top-right quadrant.
                      </div>
                    </div>
                    <div className="relative p-4">
                      {!pain ? (
                        <div className="rounded-xl border border-dashed border-slateink-200 bg-white/40 px-4 py-10 text-center text-sm text-slateink-500">
                          Loading…
                        </div>
                      ) : !painPoints.length ? (
                        <div className="rounded-xl border border-dashed border-slateink-200 bg-white/40 px-4 py-10 text-center text-sm text-slateink-500">
                          No pain matrix data for this selection (needs both SLA and rating coverage).
                        </div>
                      ) : (
                        <div className="h-[520px]">
                          <ResponsiveContainer width="100%" height="100%">
                            <ScatterChart margin={{ top: 8, right: 16, bottom: 18, left: 18 }}>
                              {/* No gridlines. Only quadrant separation lines + calm zone fills. */}
                              <XAxis
                                type="number"
                                dataKey="median_sla_days"
                                tick={{ fontSize: 12, fill: "#64748b" }}
                                axisLine={false}
                                tickLine={false}
                                ticks={painXTicks}
                                domain={[0, painXMax]}
                                label={{
                                  value: "Median Resolution Time (Days)  •  Slower →",
                                  position: "insideBottomRight",
                                  offset: -6,
                                  fill: "#94a3b8",
                                  fontSize: 11
                                }}
                              />
                              <YAxis
                                type="number"
                                dataKey="low_rating_pct"
                                tick={{ fontSize: 12, fill: "#64748b" }}
                                axisLine={false}
                                tickLine={false}
                                tickCount={5}
                                domain={[0, 100]}
                                label={{
                                  value: "Citizen Dissatisfaction (%)  •  More Dissatisfied ↑",
                                  angle: -90,
                                  position: "insideLeft",
                                  offset: -4,
                                  fill: "#94a3b8",
                                  fontSize: 11
                                }}
                              />
                              {/* Keep bubbles readable even for high-volume subtopics */}
                              <ZAxis type="number" dataKey="count" range={[64, 625]} />
                              <Tooltip content={<PainTooltip />} />
                              {/* Zone fills: use soft tint + very low opacity */}
                              <ReferenceArea x1={0} x2={painX} y1={0} y2={painY} fill={zoneFill("healthy")} fillOpacity={0.05} strokeOpacity={0} />
                              <ReferenceArea x1={0} x2={painX} y1={painY} y2={100} fill={zoneFill("fast_unhappy")} fillOpacity={0.05} strokeOpacity={0} />
                              <ReferenceArea x1={painX} x2={painXMax} y1={0} y2={painY} fill={zoneFill("slow_ok")} fillOpacity={0.05} strokeOpacity={0} />
                              <ReferenceArea x1={painX} x2={painXMax} y1={painY} y2={100} fill={zoneFill("priority")} fillOpacity={0.05} strokeOpacity={0} />
                              {painX != null ? <ReferenceLine x={painX} stroke="#cbd5e1" strokeDasharray="6 6" /> : null}
                              {painY != null ? <ReferenceLine y={painY} stroke="#cbd5e1" strokeDasharray="6 6" /> : null}

                              <Scatter
                                data={painHigh}
                                fill={dotColor("High")}
                                onMouseEnter={(p) => setHoverSubtopic(p?.subTopic || "")}
                                onMouseLeave={() => setHoverSubtopic("")}
                                shape={(p) => (
                                  <BubbleShape {...p} hoveredSubtopic={hoverSubtopic} showLabel={topLabelSet.has(p?.payload?.subTopic)} />
                                )}
                              />
                              <Scatter
                                data={painMed}
                                fill={dotColor("Med")}
                                onMouseEnter={(p) => setHoverSubtopic(p?.subTopic || "")}
                                onMouseLeave={() => setHoverSubtopic("")}
                                shape={(p) => (
                                  <BubbleShape {...p} hoveredSubtopic={hoverSubtopic} showLabel={topLabelSet.has(p?.payload?.subTopic)} />
                                )}
                              />
                              <Scatter
                                data={painLow}
                                fill={dotColor("Low")}
                                onMouseEnter={(p) => setHoverSubtopic(p?.subTopic || "")}
                                onMouseLeave={() => setHoverSubtopic("")}
                                shape={(p) => (
                                  <BubbleShape {...p} hoveredSubtopic={hoverSubtopic} showLabel={topLabelSet.has(p?.payload?.subTopic)} />
                                )}
                              />
                            </ScatterChart>
                          </ResponsiveContainer>

                          {/* Quadrant labels (anchored to plot area, not the full container) */}
                          <div className="pointer-events-none absolute top-6 bottom-6 right-6 left-[88px] text-xs font-semibold text-slateink-500">
                            <div className="absolute left-7 top-8">
                              <div className="text-[11px] tracking-wide text-slateink-400">FAST BUT UNHAPPY</div>
                              <div className="mt-1 text-xl font-semibold text-slateink-400/50">Quality Risk</div>
                            </div>
                            <div className="absolute right-10 top-8 text-right">
                              <div className="text-[11px] tracking-wide text-slateink-400">PRIORITY INTERVENTION</div>
                              <div className="mt-1 text-xl font-semibold text-slateink-400/50">Act First</div>
                            </div>
                            <div className="absolute left-7 bottom-10">
                              <div className="text-[11px] tracking-wide text-slateink-400">HEALTHY ZONE</div>
                              <div className="mt-1 text-xl font-semibold text-slateink-400/50">Stable</div>
                            </div>
                            <div className="absolute right-10 bottom-10 text-right">
                              <div className="text-[11px] tracking-wide text-slateink-400">SLOW BUT ACCEPTABLE</div>
                              <div className="mt-1 text-xl font-semibold text-slateink-400/50">Process Fix</div>
                            </div>
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                </div>

                <div className="lg:col-span-4">
                  <div className="rounded-2xl bg-white ring-1 ring-slateink-200 overflow-hidden">
                    <div className="px-5 py-4 border-b border-slateink-100">
                      <div className="flex items-center gap-2 text-lg font-semibold text-slateink-900">
                        <ListOrdered className="h-5 w-5 text-gov-700" />
                        Top 5 Painful Subtopics
                      </div>
                      <div className="mt-1 text-sm text-slateink-500">Ranked by composite pain index (Delay + Dissatisfaction)</div>
                    </div>
                    <div className="divide-y divide-slateink-100">
                      {!pain ? (
                        <div className="p-5 text-sm text-slateink-500">Loading…</div>
                      ) : !painTop.length ? (
                        <div className="p-5 text-sm text-slateink-500">No painful subtopics available for this selection.</div>
                      ) : (
                        painTop.map((r) => (
                          <div
                            key={r.subTopic}
                            className={
                              "p-5 transition " +
                              (hoverSubtopic === r.subTopic ? "bg-slateink-50/70" : "")
                            }
                            onMouseEnter={() => setHoverSubtopic(r.subTopic)}
                            onMouseLeave={() => setHoverSubtopic("")}
                          >
                            <div className="flex items-center justify-between gap-3">
                              <div className="flex items-center gap-3">
                                <div className="h-9 w-9 rounded-full bg-slateink-50 ring-1 ring-slateink-200 flex items-center justify-center text-sm font-semibold text-slateink-700">
                                  {r.rank}
                                </div>
                                <div>
                                  <div className="flex items-center gap-2">
                                    <span
                                      className="h-2.5 w-2.5 rounded-full"
                                      style={{ background: dotColor(urgencyBySubtopic.get(r.subTopic) || "Low") }}
                                    />
                                    <div className="text-sm font-semibold text-slateink-900">{r.subTopic}</div>
                                  </div>
                                </div>
                              </div>
                              <StatusPill status={r.status} />
                            </div>

                            <div className="mt-4 grid grid-cols-3 gap-2">
                              <div className="rounded-xl bg-slateink-50 ring-1 ring-slateink-100 p-3 text-center">
                                <div className="text-xs font-semibold text-slateink-400">MED SLA</div>
                                <div className="mt-1 text-sm font-semibold text-slateink-900">{r.median_sla_days ?? "—"}d</div>
                              </div>
                              <div className="rounded-xl bg-slateink-50 ring-1 ring-slateink-100 p-3 text-center">
                                <div className="text-xs font-semibold text-slateink-400">LOW RATING</div>
                                <div className="mt-1 text-sm font-semibold text-rose-700">{r.low_rating_pct ?? "—"}%</div>
                              </div>
                              <div className="rounded-xl bg-slateink-50 ring-1 ring-slateink-100 p-3 text-center">
                                <div className="text-xs font-semibold text-slateink-400">TAIL &gt;30D</div>
                                <div className="mt-1 text-sm font-semibold text-slateink-900">{r.pct_over_30d ?? "—"}%</div>
                              </div>
                            </div>
                          </div>
                        ))
                      )}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          ) : null}

          {/* Placeholder for slides 4+ */}
          {slide >= 3 ? (
            <div className="rounded-2xl bg-slateink-50 ring-1 ring-slateink-100 p-8 text-center">
              <div className="text-lg font-semibold text-slateink-900">{slides[slide]?.label} (Coming next)</div>
              <div className="mt-2 text-sm text-slateink-600">
                Share Slide {slide + 1} screenshot and I’ll implement it in the same full-screen slide system.
              </div>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}


