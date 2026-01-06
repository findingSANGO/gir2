import { useContext, useEffect, useMemo, useState } from "react";
import { useLocation } from "react-router-dom";
import { CheckCircle2, ClipboardCheck, LineChart as LineChartIcon, MessageSquareText, Timer } from "lucide-react";
import StatCard from "../components/StatCard.jsx";
import { LineCard, VerticalBarCard } from "../components/Charts.jsx";
import { api } from "../services/api.js";
import { FiltersContext } from "../App.jsx";
import AIBadge from "../components/AIBadge.jsx";
import { Card, CardContent, CardHeader } from "../components/ui/Card.jsx";

function Toggle({ value, onChange, options }) {
  return (
    <div className="inline-flex rounded-full bg-white ring-1 ring-slateink-200 p-1">
      {options.map((o) => {
        const active = value === o.key;
        return (
          <button
            key={o.key}
            type="button"
            onClick={() => onChange(o.key)}
            className={
              "h-8 rounded-full px-3 text-xs font-semibold transition " +
              (active ? "bg-slateink-900 text-white" : "text-slateink-700 hover:bg-slateink-50")
            }
          >
            {o.label}
          </button>
        );
      })}
    </div>
  );
}

function fmtPrettyRange(startISO, endISO) {
  function fmt(iso) {
    const d = iso ? new Date(`${iso}T00:00:00`) : null;
    if (!d || Number.isNaN(d.getTime())) return "—";
    return d.toLocaleDateString(undefined, { day: "numeric", month: "short", year: "numeric" });
  }
  return `${fmt(startISO)} — ${fmt(endISO)}`;
}

function formatLastUpdated(isoZ) {
  if (!isoZ) return null;
  const d = new Date(isoZ);
  if (Number.isNaN(d.getTime())) return null;
  return d.toLocaleString(undefined, { day: "2-digit", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" });
}

function marks(score, max) {
  return `${Math.max(0, Math.min(max, score))}/${max} Marks`;
}

export default function Dashboard() {
  const { filters } = useContext(FiltersContext);
  const { search } = useLocation();
  const [data, setData] = useState(null);
  const [error, setError] = useState("");
  const [mode, setMode] = useState("volume"); // volume | priority
  const [pipe, setPipe] = useState(null);
  const [pipeErr, setPipeErr] = useState("");

  const debugOn = useMemo(() => {
    try {
      const q = new URLSearchParams(search || "");
      return q.get("debug") === "1";
    } catch {
      return false;
    }
  }, [search]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const a = await api.executiveOverviewV2(filters);
        if (cancelled) return;
        setData(a);
      } catch (e) {
        setError(e.message || "Failed to load dashboard data");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [filters]);

  useEffect(() => {
    if (!debugOn) return;
    let cancelled = false;
    (async () => {
      try {
        setPipeErr("");
        const p = await api.pipelineStatus({
          source: filters?.source,
          start_date: filters?.start_date,
          end_date: filters?.end_date
        });
        if (cancelled) return;
        setPipe(p);
      } catch (e) {
        if (cancelled) return;
        setPipeErr(e.message || "Failed to load pipeline status");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [debugOn, filters?.source, filters?.start_date, filters?.end_date]);

  if (error) {
    return <div className="rounded-xl bg-white p-4 ring-1 ring-slateink-100 text-sm text-rose-700">{error}</div>;
  }

  const total = data?.totals?.total_grievances ?? "—";
  const avgClosure = data?.totals?.avg_closure_time_days != null ? `${data.totals.avg_closure_time_days} days` : "—";
  const avgRating = data?.totals?.avg_rating != null ? `${data.totals.avg_rating} / 5` : "—";
  const showAI = true;

  const openBacklog = useMemo(() => {
    const v = data?.totals?.open_backlog;
    if (v == null) return "—";
    return v;
  }, [data]);

  const trend = useMemo(() => {
    const pts = data?.time_series_daily?.rows || [];
    // keep last 45 points for chart readability
    return pts.slice(Math.max(0, pts.length - 45)).map((r) => ({ day: r.day, created: r.created, closed: r.closed }));
  }, [data]);

  // We removed the old Year/Qtr/Month/Week/Day toggle from this page (range is controlled in Filters bar).
  // Keep the label stable for UI copy.
  const periodLabel = "daily";

  const closureCoverage = data?.totals?.avg_closure_coverage;
  const ratingCoverage = data?.totals?.avg_rating_coverage;

  const modeOptions = [
    { key: "volume", label: "Volume" },
    { key: "priority", label: "Priority" }
  ];

  const topCategories = useMemo(() => {
    const rows = data?.top?.categories || [];
    const sorted = [...rows].sort((a, b) =>
      mode === "priority" ? Number(b.priority_sum || 0) - Number(a.priority_sum || 0) : Number(b.count || 0) - Number(a.count || 0)
    );
    return sorted.map((r) => ({
      ...r,
      tooltip_lines: [
        { label: "Volume", value: r.count },
        { label: "Priority (sum)", value: r.priority_sum }
      ]
    }));
  }, [data, mode]);

  const topSubtopics = useMemo(() => {
    const rows = data?.top?.subtopics || [];
    const sorted = [...rows].sort((a, b) =>
      mode === "priority" ? Number(b.priority_sum || 0) - Number(a.priority_sum || 0) : Number(b.count || 0) - Number(a.count || 0)
    );
    return sorted.map((r) => ({
      ...r,
      tooltip_lines: [
        { label: "Volume", value: r.count },
        { label: "Priority (sum)", value: r.priority_sum }
      ]
    }));
  }, [data, mode]);

  const risk = data?.operational_risk_snapshot || {};
  const showClosedLine = Boolean(data?.time_series_daily?.show_closed);
  const totalForMode = mode === "priority" ? Number(data?.totals?.total_priority_sum || 0) : Number(data?.totals?.total_grievances || 0);

  const lastUpdated = useMemo(() => formatLastUpdated(data?.generated_at) || null, [data?.generated_at]);

  const reportingRange = fmtPrettyRange(filters?.start_date, filters?.end_date);
  const reportingN = data?.totals?.total_grievances ?? 0;

  const closureMedian = data?.totals?.median_closure_time_days;
  const closureP90 = data?.totals?.p90_closure_time_days;
  const closeCoveragePct = data?.totals?.closed_coverage?.pct;

  const escalationRate = data?.escalation?.rate_pct ?? 0;
  const escalatedCount = data?.escalation?.escalated_count ?? 0;

  const aiKnown = data?.analytics?.ai_coverage_known ?? 0;
  const aiTotal = data?.analytics?.ai_coverage_total ?? 0;
  const aiPct = aiTotal ? Math.round((aiKnown / aiTotal) * 100) : 0;

  // Simple deterministic scoring (mockup-style) — audit-friendly
  const closureMarks =
    avgClosure === "—"
      ? 0
      : data?.totals?.avg_closure_time_days <= 10
        ? 4
        : data?.totals?.avg_closure_time_days <= 14
          ? 3
          : data?.totals?.avg_closure_time_days <= 21
            ? 2
            : 1;
  const escalationMarks = escalationRate <= 10 ? 3 : escalationRate <= 15 ? 2 : escalationRate <= 25 ? 1 : 0;
  const feedbackMarks =
    data?.totals?.avg_rating == null ? 0 : data?.totals?.avg_rating >= 3.5 ? 4 : data?.totals?.avg_rating >= 3.0 ? 3 : data?.totals?.avg_rating >= 2.5 ? 2 : 1;
  const analyticsMarks = aiPct >= 90 ? 3 : aiPct >= 75 ? 2 : aiPct >= 50 ? 1 : 0;
  const totalMarks = closureMarks + escalationMarks + feedbackMarks + analyticsMarks;
  const totalMax = 14;

  return (
    <div className="space-y-5">
      {debugOn ? (
        <div className="rounded-2xl bg-white ring-1 ring-slateink-100 p-4">
          <div className="flex items-start justify-between gap-3">
            <div>
              <div className="text-sm font-semibold text-slateink-900">Data Pipeline Debug (temporary)</div>
              <div className="mt-1 text-xs text-slateink-500">
                Shows raw → preprocessed → staged → AI-enriched → dashboard-eligible counts for the currently applied filters.
              </div>
            </div>
            <div className="text-xs font-semibold text-slateink-600">debug=1</div>
          </div>
          {pipeErr ? (
            <div className="mt-3 text-sm text-rose-700">{pipeErr}</div>
          ) : pipe ? (
            <div className="mt-3 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
              <div className="rounded-xl bg-slateink-50 ring-1 ring-slateink-100 p-3">
                <div className="text-xs font-semibold text-slateink-600">RAW (data/raw2)</div>
                <div className="mt-1 text-sm text-slateink-900">{pipe?.raw2?.latest_file || "—"}</div>
                <div className="mt-1 text-xs text-slateink-600">
                  rows: <span className="font-semibold">{pipe?.raw2?.raw_rows ?? "—"}</span> • unique ids:{" "}
                  <span className="font-semibold">{pipe?.raw2?.raw_unique_ids ?? "—"}</span>
                </div>
              </div>

              <div className="rounded-xl bg-slateink-50 ring-1 ring-slateink-100 p-3">
                <div className="text-xs font-semibold text-slateink-600">PREPROCESSED (DB)</div>
                <div className="mt-1 text-sm text-slateink-900">{pipe?.db?.preprocessed_source || "—"}</div>
                <div className="mt-1 text-xs text-slateink-600">
                  rows: <span className="font-semibold">{pipe?.db?.preprocessed_rows ?? "—"}</span>
                </div>
              </div>

              <div className="rounded-xl bg-slateink-50 ring-1 ring-slateink-100 p-3">
                <div className="text-xs font-semibold text-slateink-600">STAGED (DB)</div>
                <div className="mt-1 text-sm text-slateink-900">{pipe?.db?.staged_source || "—"}</div>
                <div className="mt-1 text-xs text-slateink-600">
                  rows: <span className="font-semibold">{pipe?.db?.staged_rows ?? "—"}</span> • created_date ok:{" "}
                  <span className="font-semibold">{pipe?.db?.staged_created_date_nonnull ?? "—"}</span>
                </div>
                <div className="mt-1 text-[11px] text-slateink-500">
                  created_date: {pipe?.db?.staged_created_date_min || "—"} → {pipe?.db?.staged_created_date_max || "—"}
                </div>
              </div>

              <div className="rounded-xl bg-slateink-50 ring-1 ring-slateink-100 p-3">
                <div className="text-xs font-semibold text-slateink-600">AI OUTPUTS (on staged rows)</div>
                <div className="mt-1 text-xs text-slateink-600">
                  ai_subtopic filled: <span className="font-semibold">{pipe?.db?.staged_ai_subtopic_filled ?? "—"}</span>
                </div>
              </div>

              <div className="rounded-xl bg-slateink-50 ring-1 ring-slateink-100 p-3">
                <div className="text-xs font-semibold text-slateink-600">DASHBOARD ELIGIBLE (date filter)</div>
                <div className="mt-1 text-xs text-slateink-600">
                  filter: <span className="font-semibold">{pipe?.db?.filter_start_date || "—"}</span> →{" "}
                  <span className="font-semibold">{pipe?.db?.filter_end_date || "—"}</span>
                </div>
                <div className="mt-1 text-xs text-slateink-600">
                  eligible rows: <span className="font-semibold">{pipe?.db?.eligible_rows_for_filter ?? "—"}</span>
                </div>
              </div>
            </div>
          ) : (
            <div className="mt-3 text-sm text-slateink-600">Loading pipeline status…</div>
          )}
        </div>
      ) : null}

      {/* Mockup-style header */}
      <div className="rounded-2xl bg-white ring-1 ring-slateink-100 p-4 lg:p-5">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <div className="text-2xl font-semibold text-slateink-900">Grievance Management Executive Overview</div>
            <div className="mt-1 text-sm text-slateink-500">
              Reporting Period (N={reportingN}) <span className="mx-2">•</span> {reportingRange}
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            {lastUpdated ? (
              <div className="rounded-full bg-white ring-1 ring-slateink-200 px-3 py-1 text-xs font-semibold text-slateink-700">
                Last Updated: {lastUpdated} IST
              </div>
            ) : null}
          </div>
        </div>
      </div>

      {/* Performance Scorecard */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="h-12 w-12 rounded-2xl bg-gov-50 ring-1 ring-gov-100 flex items-center justify-center">
            <ClipboardCheck className="h-6 w-6 text-gov-700" />
          </div>
          <div className="text-xl font-semibold text-slateink-900">Performance Scorecard</div>
        </div>
        <div className="rounded-full bg-white ring-1 ring-slateink-200 px-4 py-2 text-sm font-semibold text-slateink-900">
          Total Score: <span className="ml-1">{totalMarks}/{totalMax}</span>
        </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        {/* Closure Time */}
        <Card className="rounded-2xl">
          <CardContent>
            <div className="flex items-start justify-between gap-3 pt-5">
              <div>
                <div className="text-sm font-semibold tracking-wide text-slateink-700">CLOSURE TIME</div>
                <div className="mt-4 flex items-baseline gap-2">
                  <div className="text-5xl font-semibold text-slateink-900">
                    {data?.totals?.avg_closure_time_days != null ? data.totals.avg_closure_time_days : "—"}
                  </div>
                  <div className="text-lg text-slateink-700">days avg</div>
                </div>
                <div className="mt-2 text-sm text-slateink-600">
                  (N={closureCoverage?.known ?? 0} with close_date)
                </div>
              </div>
              <div className="rounded-full bg-gov-50 text-gov-800 ring-1 ring-gov-100 px-3 py-1 text-sm font-semibold">
                {marks(closureMarks, 4)}
              </div>
            </div>

            <div className="mt-5 rounded-2xl bg-white ring-1 ring-slateink-100 px-4 py-4 flex items-center justify-between">
              <div className="text-sm font-semibold text-slateink-800">Median: {closureMedian != null ? `${closureMedian}d` : "—"}</div>
              <div className="text-slateink-300">|</div>
              <div className="text-sm font-semibold text-slateink-800">P90: {closureP90 != null ? `${closureP90}d` : "—"}</div>
            </div>

            <div className="mt-4 rounded-xl bg-gov-50 ring-1 ring-gov-100 px-4 py-3 text-sm text-gov-800 flex items-center gap-2">
              <CheckCircle2 className="h-4 w-4" />
              <span>{closeCoveragePct != null ? `${closeCoveragePct}%` : "—"} close date coverage</span>
            </div>
          </CardContent>
        </Card>

        {/* Escalation */}
        <Card className="rounded-2xl">
          <CardContent>
            <div className="flex items-start justify-between gap-3 pt-5">
              <div>
                <div className="text-sm font-semibold tracking-wide text-slateink-700">ESCALATION</div>
                <div className="mt-4 inline-flex items-center gap-2 rounded-full bg-white ring-1 ring-slateink-200 px-3 py-1 text-sm font-semibold">
                  <CheckCircle2 className="h-4 w-4 text-slateink-700" />
                  Enabled (1/1)
                </div>
                <div className="mt-5 flex items-baseline gap-2">
                  <div className="text-5xl font-semibold text-slateink-900">{escalationRate}</div>
                  <div className="text-lg text-slateink-700">% rate</div>
                </div>
                <div className="mt-2 text-sm text-slateink-600">(N={escalatedCount} escalated)</div>
              </div>
              <div className="rounded-full bg-gov-50 text-gov-800 ring-1 ring-gov-100 px-3 py-1 text-sm font-semibold">
                {marks(escalationMarks, 3)}
              </div>
            </div>
            <div className="mt-6 rounded-xl bg-indigo-50/50 ring-1 ring-indigo-100 px-4 py-3 text-sm text-indigo-800 flex items-center gap-2">
              <LineChartIcon className="h-4 w-4" />
              <span>Quality control active</span>
            </div>
          </CardContent>
        </Card>

        {/* Citizen Feedback */}
        <Card className="rounded-2xl">
          <CardContent>
            <div className="flex items-start justify-between gap-3 pt-5">
              <div>
                <div className="text-sm font-semibold tracking-wide text-slateink-700">CITIZEN FEEDBACK</div>
                <div className="mt-4 flex items-baseline gap-2">
                  <div className="text-5xl font-semibold text-slateink-900">
                    {data?.totals?.avg_rating != null ? data.totals.avg_rating : "—"}
                  </div>
                  <div className="text-lg text-slateink-700">/5 stars</div>
                </div>
                <div className="mt-2 text-sm text-slateink-600">(N={ratingCoverage?.known ?? 0} rated)</div>
              </div>
              <div className="rounded-full bg-gov-50 text-gov-800 ring-1 ring-gov-100 px-3 py-1 text-sm font-semibold">
                {marks(feedbackMarks, 4)}
              </div>
            </div>
            <div className="mt-6 rounded-xl bg-slateink-50 ring-1 ring-slateink-100 px-4 py-3 text-sm text-slateink-700 flex items-center gap-2">
              <MessageSquareText className="h-4 w-4" />
              <span>Low rating (1–2): {risk?.low_rating_1_2?.pct ?? 0}%</span>
            </div>
          </CardContent>
        </Card>

        {/* Analytics */}
        <Card className="rounded-2xl">
          <CardContent>
            <div className="flex items-start justify-between gap-3 pt-5">
              <div>
                <div className="text-sm font-semibold tracking-wide text-slateink-700">ANALYTICS</div>
                <div className="mt-4 inline-flex items-center gap-2 rounded-xl bg-indigo-50/60 ring-1 ring-indigo-100 px-3 py-2 text-sm font-semibold text-indigo-800">
                  <Timer className="h-4 w-4" />
                  AI COVERAGE: {aiPct}%
                </div>
                <div className="mt-5 space-y-2">
                  <div className="rounded-xl bg-emerald-50/60 ring-1 ring-emerald-100 px-4 py-3 text-sm text-emerald-800 flex items-center gap-2">
                    <CheckCircle2 className="h-4 w-4" />
                    <span>Executive Overview + Issue Intelligence ready</span>
                  </div>
                </div>
              </div>
              <div className="rounded-full bg-gov-50 text-gov-800 ring-1 ring-gov-100 px-3 py-1 text-sm font-semibold">
                {marks(analyticsMarks, 3)}
              </div>
            </div>
            <div className="mt-4 text-xs text-slateink-500">AI coverage computed from non-empty sub-topics: {aiKnown}/{aiTotal}.</div>
          </CardContent>
        </Card>
      </div>

      {/* Keep existing visual metric cards, but de-emphasize under scorecard */}
      <div className="rounded-2xl bg-white ring-1 ring-slateink-100 p-4">
        <div className="text-sm font-semibold text-slateink-800">Operational Totals</div>
        <div className="mt-3 grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <StatCard title="Total grievances" value={total} subtitle="Selected Created Date range" />
          <StatCard title="Average closure time" value={avgClosure} subtitle={`Coverage: ${closureCoverage?.known ?? 0}/${closureCoverage?.total ?? 0}`} />
          <StatCard title="Average feedback rating" value={avgRating} subtitle={`Coverage: ${ratingCoverage?.known ?? 0}/${ratingCoverage?.total ?? 0}`} />
          <StatCard title="Open backlog" value={openBacklog} subtitle="From status breakdown (best-effort)" />
        </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-12">
        <div className="lg:col-span-8">
          <LineCard
            title={`Grievances over time (${periodLabel})`}
            subtitle={
              showClosedLine
                ? "Created vs Closed (closed line shown when coverage is sufficient)"
                : `Created (closed line hidden due to low coverage: ${data?.totals?.closed_coverage?.pct ?? 0}%)`
            }
            data={trend}
            xKey="day"
            lines={[
              { key: "created", name: "Created", color: "#2b54f6" },
              ...(showClosedLine ? [{ key: "closed", name: "Closed", color: "#16a34a" }] : [])
            ]}
            height={300}
          />
        </div>
        <div className="lg:col-span-4 space-y-4">
          <Card>
            <CardHeader title="Operational Risk Snapshot" subtitle="Quick operational health indicators (computed)" />
            <CardContent>
              <div className="grid grid-cols-2 gap-3">
                <div className="rounded-xl bg-white ring-1 ring-slateink-100 p-3">
                  <div className="text-xs font-semibold text-slateink-600">% within 3d</div>
                  <div className="mt-1 text-lg font-semibold text-slateink-900">{risk?.within_3d?.pct ?? 0}%</div>
                  <div className="text-xs text-slateink-500">{risk?.within_3d?.count ?? 0} / {total === "—" ? 0 : total}</div>
                </div>
                <div className="rounded-xl bg-white ring-1 ring-slateink-100 p-3">
                  <div className="text-xs font-semibold text-slateink-600">% &gt;30d</div>
                  <div className="mt-1 text-lg font-semibold text-slateink-900">{risk?.over_30d?.pct ?? 0}%</div>
                  <div className="text-xs text-slateink-500">{risk?.over_30d?.count ?? 0} / {total === "—" ? 0 : total}</div>
                </div>
                <div className="rounded-xl bg-white ring-1 ring-slateink-100 p-3">
                  <div className="text-xs font-semibold text-slateink-600">% forwarded</div>
                  <div className="mt-1 text-lg font-semibold text-slateink-900">{risk?.forwarded?.pct ?? 0}%</div>
                  <div className="text-xs text-slateink-500">{risk?.forwarded?.count ?? 0} / {total === "—" ? 0 : total}</div>
                </div>
                <div className="rounded-xl bg-white ring-1 ring-slateink-100 p-3">
                  <div className="text-xs font-semibold text-slateink-600">% low rating (1–2)</div>
                  <div className="mt-1 text-lg font-semibold text-slateink-900">{risk?.low_rating_1_2?.pct ?? 0}%</div>
                  <div className="text-xs text-slateink-500">{risk?.low_rating_1_2?.count ?? 0} / {total === "—" ? 0 : total}</div>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader title="Executive insights" subtitle="Commissioner-ready summary (no extra AI calls)" />
            <CardContent>
              <ul className="space-y-2 text-sm text-slateink-700 leading-relaxed">
                {(data?.insights || []).slice(0, 6).map((t, idx) => (
                  <li key={idx} className="flex gap-2">
                    <span className="mt-2 h-1.5 w-1.5 rounded-full bg-gov-600 shrink-0" />
                    <span>{t}</span>
                  </li>
                ))}
              </ul>
              <div className="mt-3 text-xs text-slateink-500">
                AI-derived charts are labeled <span className="font-semibold">Powered by caseA</span> for traceability.
              </div>
            </CardContent>
          </Card>
        </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-12">
        <div className="lg:col-span-6">
          <VerticalBarCard
            title={
              <span className="inline-flex items-center gap-2">
                Top issue categories {showAI ? <AIBadge /> : null}
              </span>
            }
            ai={showAI}
            right={<Toggle value={mode} onChange={setMode} options={modeOptions} />}
            data={topCategories.slice(0, 10)}
            yKey="category"
            valueKey={mode === "priority" ? "priority_sum" : "count"}
            height={360}
            total={totalForMode}
          />
        </div>
        <div className="lg:col-span-6">
          <VerticalBarCard
            title={
              <span className="inline-flex items-center gap-2">
                Top sub-topics {showAI ? <AIBadge /> : null}
              </span>
            }
            ai={showAI}
            right={<Toggle value={mode} onChange={setMode} options={modeOptions} />}
            data={topSubtopics.slice(0, 10)}
            yKey="subTopic"
            valueKey={mode === "priority" ? "priority_sum" : "count"}
            height={360}
            total={totalForMode}
          />
        </div>
      </div>
    </div>
  );
}


