import { useContext, useEffect, useState } from "react";
import { BarCard } from "../components/Charts.jsx";
import { api } from "../services/api.js";
import { FiltersContext } from "../App.jsx";
import AIBadge from "../components/AIBadge.jsx";

export default function FeedbackAnalytics() {
  const { filters } = useContext(FiltersContext);
  const [data, setData] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await api.feedback(filters);
        if (cancelled) return;
        setData(res);
      } catch (e) {
        setError(e.message || "Failed to load feedback analytics");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [filters]);

  if (error) {
    return <div className="rounded-xl bg-white p-4 ring-1 ring-slateink-100 text-sm text-rose-700">{error}</div>;
  }

  const showAI = data?.ai_meta?.ai_provider === "caseA";

  return (
    <div className="space-y-5">
      <div className="grid gap-4 lg:grid-cols-3">
        <BarCard
          title="Star rating distribution"
          data={data?.feedbackDistribution || []}
          xKey="star"
          bars={[{ key: "count", name: "Count", color: "#46526a" }]}
        />
        <div className="rounded-xl bg-white shadow-card ring-1 ring-slateink-100 p-4 transition-shadow hover:shadow-[0_1px_2px_rgba(15,23,42,0.08),0_12px_36px_rgba(15,23,42,0.10)]">
          <div className="text-sm font-semibold text-slateink-800">Feedback insights</div>
          <ul className="mt-3 space-y-2 text-sm text-slateink-700">
            {(data?.insights || []).slice(0, 5).map((it, idx) => (
              <li key={idx} className="flex gap-2">
                <span className="mt-1 h-1.5 w-1.5 rounded-full bg-gov-600 shrink-0" />
                <span>{it}</span>
              </li>
            ))}
          </ul>
        </div>
        <BarCard
          title={
            <>
              Top dissatisfaction reasons {showAI ? <AIBadge /> : null}
            </>
          }
          ai={showAI}
          data={(data?.lowFeedbackDrivers?.topDissatisfactionReasons || []).slice(0, 8).map((r) => ({ reason: r.reason, count: r.count }))}
          xKey="reason"
          bars={[{ key: "count", name: "Count", color: "#2b54f6" }]}
          height={260}
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-3">
        <BarCard
          title={
            <>
              Low feedback (≤2) by category {showAI ? <AIBadge /> : null}
            </>
          }
          ai={showAI}
          data={(data?.lowFeedbackDrivers?.byCategory || []).slice(0, 10)}
          xKey="category"
          bars={[{ key: "count", name: "Count", color: "#2b54f6" }]}
          height={280}
        />
        <BarCard
          title="Low feedback (≤2) by closure bucket"
          data={data?.lowFeedbackDrivers?.byClosureBucket || []}
          xKey="bucket"
          bars={[{ key: "count", name: "Count", color: "#1f3fd0" }]}
          height={280}
        />
        <BarCard
          title="Low feedback (≤2) by ward"
          data={(data?.lowFeedbackDrivers?.byWard || []).slice(0, 10)}
          xKey="ward"
          bars={[{ key: "count", name: "Count", color: "#384357" }]}
          height={280}
        />
      </div>
    </div>
  );
}


