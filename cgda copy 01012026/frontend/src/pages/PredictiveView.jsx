import { useContext, useEffect, useState } from "react";
import { SimpleTable } from "../components/Charts.jsx";
import { api } from "../services/api.js";
import { FiltersContext } from "../App.jsx";
import AIBadge from "../components/AIBadge.jsx";

export default function PredictiveView() {
  const { filters } = useContext(FiltersContext);
  const [data, setData] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await api.predictive(filters);
        if (cancelled) return;
        setData(res);
      } catch (e) {
        setError(e.message || "Failed to load predictive view");
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
        <div className="rounded-xl bg-white shadow-card ring-1 ring-slateink-100 p-4 transition-shadow hover:shadow-[0_1px_2px_rgba(15,23,42,0.08),0_12px_36px_rgba(15,23,42,0.10)]">
          <div className="text-sm font-semibold text-slateink-800">Predictive insights</div>
          <ul className="mt-3 space-y-2 text-sm text-slateink-700">
            {(data?.insights || []).slice(0, 5).map((it, idx) => (
              <li key={idx} className="flex gap-2">
                <span className="mt-1 h-1.5 w-1.5 rounded-full bg-gov-600 shrink-0" />
                <span>{it}</span>
              </li>
            ))}
          </ul>
        </div>
        <div className="lg:col-span-2 rounded-xl bg-white shadow-card ring-1 ring-slateink-100 p-4">
        <div className="text-sm font-semibold text-slateink-800">How predictions work (MVP)</div>
        <div className="mt-2 text-sm text-slateink-700 leading-relaxed">
          This MVP uses <span className="font-semibold">rule-based</span> signals: compares grievance volume in the last 30
          days vs the previous 30 days. It is designed for clarity and auditability.
        </div>
      </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <SimpleTable
          title="Wards likely to spike (next 30 days)"
          columns={[
            { key: "ward", label: "Ward" },
            { key: "risk", label: "Risk" },
            { key: "last30", label: "Last 30d" },
            { key: "prev30", label: "Prev 30d" }
          ]}
          rows={data?.wardRisk || []}
        />
        <SimpleTable
          title={
            <>
              Issue types at risk {showAI ? <AIBadge /> : null}
            </>
          }
          ai={showAI}
          columns={[
            { key: "category", label: "Issue category" },
            { key: "risk", label: "Risk" },
            { key: "last30", label: "Last 30d" },
            { key: "prev30", label: "Prev 30d" }
          ]}
          rows={data?.categoryRisk || []}
        />
      </div>
    </div>
  );
}


