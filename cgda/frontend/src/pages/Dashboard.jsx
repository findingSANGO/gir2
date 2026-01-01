import { useContext, useEffect, useMemo, useState } from "react";
import StatCard from "../components/StatCard.jsx";
import { LineCard, VerticalBarCard } from "../components/Charts.jsx";
import { api } from "../services/api.js";
import { FiltersContext } from "../App.jsx";
import AIBadge from "../components/AIBadge.jsx";
import { Card, CardContent, CardHeader } from "../components/ui/Card.jsx";

export default function Dashboard() {
  const { filters } = useContext(FiltersContext);
  const [data, setData] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const a = await api.executiveOverview(filters);
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

  const total = data?.total_grievances ?? "—";
  const avgClosure = data?.avg_closure_time_days != null ? `${data.avg_closure_time_days} days` : "—";
  const avgRating = data?.avg_feedback_rating != null ? `${data.avg_feedback_rating} / 5` : "—";
  const showAI = true;

  const openBacklog = useMemo(() => {
    const rows = data?.status_breakdown || [];
    if (!rows.length || data?.total_grievances == null) return "—";
    const closedLike = rows
      .filter((r) => String(r.status || "").toLowerCase().includes("closed") || String(r.status || "").toLowerCase().includes("resolved"))
      .reduce((acc, r) => acc + Number(r.count || 0), 0);
    const v = Number(data.total_grievances) - closedLike;
    return Number.isFinite(v) ? Math.max(0, v) : "—";
  }, [data]);

  const trend = useMemo(() => {
    const pts = data?.grievances_over_time || [];
    // keep last 45 points for chart readability
    return pts.slice(Math.max(0, pts.length - 45)).map((r) => ({ day: r.date, count: r.count }));
  }, [data]);

  return (
    <div className="space-y-5">
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard title="Total grievances" value={total} subtitle="For selected date range (Created Date)" />
        <StatCard title="Average closure time" value={avgClosure} subtitle="Not available in processed dataset" />
        <StatCard title="Average feedback rating" value={avgRating} subtitle="Not available in processed dataset" />
        <StatCard title="Open backlog" value={openBacklog} subtitle="Computed from status breakdown (best-effort)" />
      </div>

      <div className="grid gap-4 lg:grid-cols-12">
        <div className="lg:col-span-8">
          <LineCard
            title="Grievances over time (daily)"
            data={trend}
            xKey="day"
            lines={[{ key: "count", name: "Grievances" }]}
            height={300}
          />
        </div>
        <div className="lg:col-span-4">
          <Card>
            <CardHeader title="Executive insights" subtitle="Commissioner-ready summary (no extra AI calls)" />
            <CardContent>
              <ul className="space-y-2 text-sm text-slateink-700 leading-relaxed">
                <li className="flex gap-2">
                  <span className="mt-2 h-1.5 w-1.5 rounded-full bg-gov-600 shrink-0" />
                  <span>Use the <span className="font-semibold">GO</span> button to apply filters once (fast, SQL-based).</span>
                </li>
                <li className="flex gap-2">
                  <span className="mt-2 h-1.5 w-1.5 rounded-full bg-gov-600 shrink-0" />
                  <span>AI-derived charts are labeled <span className="font-semibold">Powered by caseA</span> for traceability.</span>
                </li>
              </ul>
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
            data={(data?.top_categories || []).slice(0, 10)}
            yKey="category"
            valueKey="count"
            height={360}
            total={Number(data?.total_grievances || 0)}
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
            data={(data?.top_subtopics || []).slice(0, 10)}
            yKey="subTopic"
            valueKey="count"
            height={360}
            total={Number(data?.total_grievances || 0)}
          />
        </div>
      </div>
    </div>
  );
}


