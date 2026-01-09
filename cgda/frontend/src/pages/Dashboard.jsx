import { useContext, useEffect, useMemo, useState } from "react";
import { CheckCircle2 } from "lucide-react";
import StatCard from "../components/StatCard.jsx";
import { LineCard, VerticalBarCard } from "../components/Charts.jsx";
import { api } from "../services/api.js";
import { FiltersContext } from "../App.jsx";
import AIBadge from "../components/AIBadge.jsx";
import { Card, CardContent, CardHeader } from "../components/ui/Card.jsx";
import { displaySubtopicLabel } from "../utils/labels.js";

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

export default function Dashboard() {
  const { filters } = useContext(FiltersContext);
  const [data, setData] = useState(null);
  const [error, setError] = useState("");
  const [mode, setMode] = useState("volume"); // volume | priority

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
      subTopicDisplay: displaySubtopicLabel(r?.subTopic) || "—",
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

  function fmtInt(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return "—";
    return String(Math.round(n));
  }
  function fmt1(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return "—";
    return n.toFixed(1);
  }

  return (
    <div className="space-y-5">
      {/* Mockup-style header */}
      <div className="rounded-2xl bg-white ring-1 ring-slateink-100 p-4 lg:p-5">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <div className="text-2xl font-semibold text-slateink-900">Executive Overview</div>
            <div className="mt-1 text-sm text-slateink-500">
              Reporting period <span className="mx-2">•</span> N={reportingN} <span className="mx-2">•</span> {reportingRange}
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

      {/* At-a-glance KPIs */}
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard title="Total grievances" value={total} subtitle="Selected date range" />
        <StatCard
          title="Avg closure time"
          value={data?.totals?.avg_closure_time_days != null ? `${fmtInt(data.totals.avg_closure_time_days)} days` : "—"}
          subtitle={`Coverage: ${closureCoverage?.known ?? 0}/${closureCoverage?.total ?? 0}`}
        />
        <StatCard
          title="Escalation rate"
          value={Number.isFinite(Number(escalationRate)) ? `${fmt1(escalationRate)}%` : "—"}
          subtitle={`Escalated: ${escalatedCount}`}
        />
        <StatCard
          title="Avg rating"
          value={data?.totals?.avg_rating != null ? `${fmt1(data.totals.avg_rating)} / 5` : "—"}
          subtitle={`Rated: ${ratingCoverage?.known ?? 0}/${ratingCoverage?.total ?? 0}`}
        />
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
            yKey="subTopicDisplay"
            colorKey="subTopic"
            valueKey={mode === "priority" ? "priority_sum" : "count"}
            height={360}
            total={totalForMode}
          />
        </div>
      </div>
    </div>
  );
}


