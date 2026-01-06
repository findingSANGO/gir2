import { useContext, useEffect, useMemo, useState } from "react";
import { VerticalBarCard } from "../components/Charts.jsx";
import { api } from "../services/api.js";
import { FiltersContext } from "../App.jsx";
import {
  BarChart3,
  ChevronLeft,
  ChevronRight,
  ClipboardList,
  Database,
  ShieldCheck,
  TriangleAlert,
  AlertTriangle,
  ListOrdered
} from "lucide-react";
import { Card, CardContent } from "../components/ui/Card.jsx";
import {
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
    <div className="flex items-center gap-2 overflow-auto">
      {slides.map((s, idx) => {
        const isActive = idx === active;
        return (
          <button
            key={s.key}
            type="button"
            onClick={() => onChange(idx)}
            className={
              "whitespace-nowrap rounded-full px-3 py-1.5 text-xs font-semibold ring-1 transition " +
              (isActive ? "bg-white text-slateink-900 ring-white" : "bg-white/10 text-white ring-white/15 hover:bg-white/15")
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

export default function IssueIntelligence2() {
  const { filters } = useContext(FiltersContext);
  const [payload, setPayload] = useState(null);
  const [slide, setSlide] = useState(0);
  const [metric, setMetric] = useState("volume"); // volume | priority
  const [error, setError] = useState("");
  const [hoverSubtopic, setHoverSubtopic] = useState("");

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

  if (error) {
    return <div className="rounded-xl bg-white p-4 ring-1 ring-slateink-100 text-sm text-rose-700">{error}</div>;
  }

  const slides = [
    { key: "overview", label: "Overview" },
    { key: "load", label: "Load View" },
    { key: "pain", label: "Pain Matrix" },
    { key: "hotspots", label: "Hotspots" },
    { key: "exceptions", label: "Exceptions" },
    { key: "ward", label: "Ward Ownership" },
    { key: "dept", label: "Department Performance" },
    { key: "trend", label: "Trend Analysis" },
    { key: "signals", label: "Root Signals" },
    { key: "week", label: "This Week" }
  ];

  function prev() {
    setSlide((s) => Math.max(0, s - 1));
  }
  function next() {
    setSlide((s) => Math.min(slides.length - 1, s + 1));
  }

  // Keyboard navigation: ← / → to move between slides
  useEffect(() => {
    function onKeyDown(e) {
      const tag = String(e?.target?.tagName || "").toLowerCase();
      if (tag === "input" || tag === "textarea" || tag === "select") return;
      if (e.key === "ArrowLeft") prev();
      if (e.key === "ArrowRight") next();
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const periodLabel = fmtRange(filters?.start_date, filters?.end_date);
  const scope = `Ward: ${(filters?.wards || []).length ? `${filters.wards.length} selected` : "All"}  •  Dept: ${
    filters?.department || "All"
  }  •  Cat: ${filters?.category || "All"}`;

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

  const callouts = load?.callouts || {};
  const pain = payload?.pain_matrix || null;
  const painPointsRaw = pain?.points || [];
  const painTop = pain?.top_painful || [];
  const painX = pain?.x_threshold_days ?? null;
  const painY = pain?.y_threshold_low_rating_pct ?? null;

  const painPoints = useMemo(() => {
    return [...painPointsRaw].sort((a, b) => Number(b.count || 0) - Number(a.count || 0));
  }, [painPointsRaw]);

  const painHigh = useMemo(() => painPoints.filter((p) => String(p.urgency || "").toLowerCase().startsWith("high")), [painPoints]);
  const painMed = useMemo(() => painPoints.filter((p) => String(p.urgency || "").toLowerCase().startsWith("med")), [painPoints]);
  const painLow = useMemo(() => painPoints.filter((p) => !String(p.urgency || "").toLowerCase().match(/^(high|med)/)), [painPoints]);

  const topLabelSet = useMemo(() => new Set((painTop || []).map((r) => r.subTopic)), [painTop]);
  const urgencyBySubtopic = useMemo(() => {
    const m = new Map();
    for (const p of painPoints) m.set(p.subTopic, p.urgency);
    return m;
  }, [painPoints]);

  return (
    <div className="space-y-5">
      {/* Slide navigation header (inside Issue Intelligence section) */}
      <div className="rounded-2xl bg-gradient-to-br from-slateink-950 via-slateink-900 to-indigo-950 ring-1 ring-slateink-900/30 overflow-hidden">
        <div className="px-5 py-5">
          <div className="flex items-center gap-2 text-xs font-semibold text-white/70">
            <span className="rounded-full bg-white/10 ring-1 ring-white/15 px-3 py-1">DASHBOARD</span>
            <span className="text-white/50">GRIEVANCE ANALYTICS 2025</span>
          </div>
          <div className="mt-2 text-3xl font-semibold text-white">Issue Intelligence 2</div>
          <div className="mt-1 text-sm text-white/70">What’s driving problems + where to act</div>

          <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
            <SlideTabs slides={slides} active={slide} onChange={setSlide} />
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={prev}
                disabled={slide === 0}
                className="h-10 w-10 rounded-xl bg-white/10 ring-1 ring-white/15 text-white disabled:opacity-40 hover:bg-white/15 flex items-center justify-center"
                title="Previous slide"
              >
                <ChevronLeft className="h-5 w-5" />
              </button>
              <button
                type="button"
                onClick={next}
                disabled={slide === slides.length - 1}
                className="h-10 w-10 rounded-xl bg-white/10 ring-1 ring-white/15 text-white disabled:opacity-40 hover:bg-white/15 flex items-center justify-center"
                title="Next slide"
              >
                <ChevronRight className="h-5 w-5" />
              </button>
            </div>
          </div>
        </div>

        <div className="bg-white">
          <div className="px-5 py-3 flex flex-wrap items-center gap-3 text-xs">
            <div className="inline-flex items-center gap-2 text-slateink-700">
              <span className="font-semibold text-slateink-900">PERIOD</span>
              <span className="rounded-full bg-slateink-50 ring-1 ring-slateink-200 px-3 py-1">{periodLabel}</span>
            </div>
            <div className="h-6 w-px bg-slateink-200" />
            <div className="inline-flex items-center gap-2 text-slateink-700">
              <span className="font-semibold text-slateink-900">SCOPE FILTERS</span>
              <span className="rounded-full bg-slateink-50 ring-1 ring-slateink-200 px-3 py-1">{scope}</span>
            </div>
            <div className="ml-auto text-[11px] font-semibold text-slateink-400">CONFIDENTIAL • REPORT</div>
          </div>
        </div>
      </div>

      {/* Slides container */}
      <div className="rounded-2xl bg-white ring-1 ring-slateink-100 overflow-hidden">
        <div className="p-5">
          {/* Slide 1 */}
          {slide === 0 ? (
            <div className="grid gap-5 lg:grid-cols-12">
              <div className="lg:col-span-4">
                <div className="flex items-center gap-2 text-sm font-semibold text-slateink-900">
                  <Database className="h-4 w-4 text-gov-700" />
                  Data Readiness
                </div>
                <div className="mt-4 space-y-5">
                  <ProgressRow label="AI Coverage" pct={readiness?.ai?.pct} subtitle="Enrichment success rate" />
                  <ProgressRow label="Close Date Coverage" pct={readiness?.close_date?.pct} subtitle="Timestamps validity" />
                  <ProgressRow label="Rating Coverage" pct={readiness?.rating?.pct} subtitle="Citizen feedback received" />
                </div>
                <div className="mt-5 rounded-xl bg-slateink-50 ring-1 ring-slateink-100 p-3 text-xs text-slateink-600">
                  High data readiness allows for confident segmentation of issues by urgency and SLA risk.
                </div>
              </div>

              <div className="lg:col-span-8">
                <div className="flex items-center gap-2 text-sm font-semibold text-slateink-900">
                  <ShieldCheck className="h-4 w-4 text-indigo-700" />
                  Dashboard Purpose
                </div>
                <div className="mt-4 grid gap-3 sm:grid-cols-2">
                  {[
                    {
                      icon: BarChart3,
                      title: "Identify Top Issues",
                      desc: "Pinpoint highest volume categories driving citizen complaints.",
                      tone: "bg-slateink-50 ring-slateink-100"
                    },
                    {
                      icon: TriangleAlert,
                      title: "Identify Painful Issues",
                      desc: "Spot problems that are slow to resolve, low-rated, and highly urgent.",
                      tone: "bg-rose-50 ring-rose-100"
                    },
                    {
                      icon: Database,
                      title: "Identify Hotspots",
                      desc: "Locate specific infrastructure entities or geographic zones causing failures.",
                      tone: "bg-indigo-50 ring-indigo-100"
                    },
                    {
                      icon: ClipboardList,
                      title: "Produce Priorities",
                      desc: "Generate actionable task lists for Ward and Department owners.",
                      tone: "bg-emerald-50 ring-emerald-100"
                    }
                  ].map((x) => {
                    const Icon = x.icon;
                    return (
                      <div key={x.title} className={`rounded-2xl ring-1 p-4 ${x.tone}`}>
                        <div className="flex items-start gap-3">
                          <div className="h-9 w-9 rounded-xl bg-white ring-1 ring-slateink-100 flex items-center justify-center">
                            <Icon className="h-4 w-4 text-slateink-700" />
                          </div>
                          <div>
                            <div className="text-sm font-semibold text-slateink-900">{x.title}</div>
                            <div className="mt-1 text-xs text-slateink-600">{x.desc}</div>
                          </div>
                        </div>
                      </div>
                    );
                  })}
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
                    <div className="text-lg font-semibold">Load View: Top Sub-Topics</div>
                    <div className="text-xs text-white/70">{periodLabel} • Overall View</div>
                  </div>
                  <MetricToggle value={metric} onChange={setMetric} />
                </div>
              </div>

              <div className="grid gap-4 lg:grid-cols-12">
                <div className="lg:col-span-8">
                  <VerticalBarCard
                    title={`Top 10 Sub-Topics by ${metric === "priority" ? "Priority" : "Volume"}`}
                    subtitle={
                      load?.source
                        ? `Source: ${load.source} • Total Records Analyzed: ${load.total_records_analyzed || 0}`
                        : undefined
                    }
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
                                tickCount={4}
                                domain={[0, (max) => Math.ceil((Number(max || 0) + 5) / 10) * 10]}
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
                              <ReferenceArea x1={painX} x2={9999} y1={0} y2={painY} fill={zoneFill("slow_ok")} fillOpacity={0.05} strokeOpacity={0} />
                              <ReferenceArea x1={painX} x2={9999} y1={painY} y2={100} fill={zoneFill("priority")} fillOpacity={0.05} strokeOpacity={0} />
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

                          {/* Quadrant labels */}
                          <div className="pointer-events-none absolute inset-0 p-6 text-xs font-semibold text-slateink-500">
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


