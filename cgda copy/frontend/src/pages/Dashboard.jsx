import { useContext, useEffect, useMemo, useState } from "react";
import StatCard from "../components/StatCard.jsx";
import { BarCard, LineCard } from "../components/Charts.jsx";
import { api } from "../services/api.js";
import { FiltersContext } from "../App.jsx";
import AIBadge from "../components/AIBadge.jsx";

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

  const trend = useMemo(() => {
    const pts = data?.grievances_over_time || [];
    // keep last 45 points for chart readability
    return pts.slice(Math.max(0, pts.length - 45)).map((r) => ({ day: r.date, count: r.count }));
  }, [data]);

  return (
    <div className="space-y-5">
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        <StatCard title="Total grievances" value={total} subtitle="For selected date range (Created Date)" />
        <StatCard title="Average closure time" value={avgClosure} subtitle="Not available in processed dataset" />
        <StatCard title="Average feedback rating" value={avgRating} subtitle="Not available in processed dataset" />
      </div>

      <div className="grid gap-4 lg:grid-cols-3">
        <div className="rounded-xl bg-white shadow-card ring-1 ring-slateink-100 p-4 transition-shadow hover:shadow-[0_1px_2px_rgba(15,23,42,0.08),0_12px_36px_rgba(15,23,42,0.10)]">
          <div className="flex items-center justify-between gap-3">
            <div className="text-sm font-semibold text-slateink-800">Executive insights</div>
          </div>
          <ul className="mt-3 space-y-2 text-sm text-slateink-700">
            <li className="flex gap-2">
              <span className="mt-1 h-1.5 w-1.5 rounded-full bg-gov-600 shrink-0" />
              <span>Use the GO button to apply date range filters instantly (no AI calls).</span>
            </li>
          </ul>
        </div>
        <LineCard
          title="Grievances over time (daily)"
          data={trend}
          xKey="day"
          lines={[{ key: "count", name: "Grievances", color: "#1f3fd0" }]}
          height={240}
        />
        <BarCard
          title={
            <>
              Top issue categories {showAI ? <AIBadge /> : null}
            </>
          }
          ai={showAI}
          data={(data?.top_categories || []).slice(0, 8)}
          xKey="category"
          bars={[{ key: "count", name: "Grievances", color: "#2b54f6" }]}
          height={240}
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-3">
        <BarCard
          title={
            <>
              Top sub-topics {showAI ? <AIBadge /> : null}
            </>
          }
          ai={showAI}
          data={(data?.top_subtopics || []).slice(0, 10)}
          xKey="subTopic"
          bars={[{ key: "count", name: "Grievances", color: "#4f46e5" }]}
          height={260}
        />
        <div className="lg:col-span-2 rounded-xl bg-white shadow-card ring-1 ring-slateink-100 p-4">
          <div className="flex items-center justify-between gap-3">
            <div className="text-sm font-semibold text-slateink-800">
              Top sub-topics {showAI ? <AIBadge /> : null}
            </div>
          </div>
          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="text-left text-xs text-slateink-500">
                  <th className="py-2 pr-3">Sub-Topic</th>
                  <th className="py-2 pr-3">Count</th>
                </tr>
              </thead>
              <tbody>
                {(data?.top_subtopics || []).slice(0, 12).map((r) => (
                  <tr key={r.subTopic} className="border-t border-slateink-100">
                    <td className="py-2 pr-3 text-slateink-800">{r.subTopic}</td>
                    <td className="py-2 pr-3 font-semibold text-slateink-900">{r.count}</td>
                  </tr>
                ))}
                {!(data?.top_subtopics || []).length ? (
                  <tr>
                    <td className="py-3 text-sm text-slateink-500" colSpan={2}>
                      No sub-topic data for this date range.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}


