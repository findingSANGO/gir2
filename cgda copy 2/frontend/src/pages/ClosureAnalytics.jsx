import { useContext, useEffect, useState } from "react";
import { BarCard, PieCard, SimpleTable } from "../components/Charts.jsx";
import { api } from "../services/api.js";
import { FiltersContext } from "../App.jsx";
import AIBadge from "../components/AIBadge.jsx";

export default function ClosureAnalytics() {
  const { filters } = useContext(FiltersContext);
  const [data, setData] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await api.closure(filters);
        if (cancelled) return;
        setData(res);
      } catch (e) {
        setError(e.message || "Failed to load closure analytics");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [filters]);

  if (error) {
    return <div className="rounded-xl bg-white p-4 ring-1 ring-slateink-100 text-sm text-rose-700">{error}</div>;
  }

  const delayData = (data?.closureBuckets || []).map((d) => ({ name: d.bucket, value: d.count }));
  const showAI = data?.ai_meta?.ai_provider === "caseA";

  return (
    <div className="space-y-5">
      <div className="grid gap-4 lg:grid-cols-3">
        <div className="rounded-xl bg-white shadow-card ring-1 ring-slateink-100 p-4 transition-shadow hover:shadow-[0_1px_2px_rgba(15,23,42,0.08),0_12px_36px_rgba(15,23,42,0.10)]">
          <div className="text-sm font-semibold text-slateink-800">Closure insights</div>
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
              Average closure time by category {showAI ? <AIBadge /> : null}
            </>
          }
          ai={showAI}
          data={(data?.avgClosureByCategory || []).slice(0, 10)}
          xKey="category"
          bars={[{ key: "avgDays", name: "Avg days", color: "#2b54f6" }]}
          height={300}
        />
        <PieCard
          title="Delay buckets (overall)"
          data={delayData}
          nameKey="name"
          valueKey="value"
          colors={["#2b54f6", "#46526a", "#1f3fd0", "#b0b8c8"]}
          height={300}
        />
      </div>

      <SimpleTable
        title="Wards with longest average closure"
        ai={showAI}
        columns={[
          { key: "ward", label: "Ward" },
          { key: "avgDays", label: "Avg days" },
          { key: "count", label: "Count" }
        ]}
        rows={(data?.avgClosureByWard || []).slice(0, 10)}
      />
    </div>
  );
}


