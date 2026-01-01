import { useContext, useEffect, useState } from "react";
import { api } from "../services/api.js";
import { FiltersContext } from "../App.jsx";
import AIBadge from "../components/AIBadge.jsx";

function downloadBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

export default function Evidence() {
  const { filters } = useContext(FiltersContext);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState("");
  const [showAI, setShowAI] = useState(false);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const r = await api.retrospective(filters);
        if (!cancelled) setShowAI(r?.ai_meta?.ai_provider === "caseA");
      } catch {
        // ignore
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [filters]);

  async function exportCsv() {
    setBusy(true);
    setMsg("");
    try {
      // api.exportStructuredCsv returns text; fetch as blob for clean download
      const token = localStorage.getItem("cgda_token");
      const res = await fetch(`${api.baseUrl}/api/data/export_structured_csv`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      const blob = await res.blob();
      downloadBlob(blob, "cgda_structured_export.csv");
      setMsg("Structured CSV exported.");
    } catch (e) {
      setMsg(e.message || "Export failed");
    } finally {
      setBusy(false);
    }
  }

  async function commissionerPdf() {
    setBusy(true);
    setMsg("");
    try {
      const token = localStorage.getItem("cgda_token");
      const qs = new URLSearchParams();
      if (filters.start_date) qs.set("start_date", filters.start_date);
      if (filters.end_date) qs.set("end_date", filters.end_date);
      if (filters.wards?.length) qs.set("wards", filters.wards.join(","));
      if (filters.department) qs.set("department", filters.department);
      if (filters.category) qs.set("category", filters.category);
      const url = `${api.baseUrl}/api/reports/commissioner_pdf${qs.toString() ? `?${qs}` : ""}`;
      const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
      const blob = await res.blob();
      downloadBlob(blob, "cgda_commissioner_summary.pdf");
      setMsg("Commissioner PDF generated.");
    } catch (e) {
      setMsg(e.message || "PDF generation failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="space-y-4">
      <div className="rounded-xl bg-white shadow-card ring-1 ring-slateink-100 p-4">
        <div className="text-sm font-semibold text-slateink-800">Evidence & exports</div>
        <div className="mt-2 text-sm text-slateink-700">
          Use this section during the Commissioner demo to export structured outputs and generate a PDF summary (with filter context).
        </div>
        <div className="mt-2 text-xs text-slateink-500 flex items-center gap-2">
          {showAI ? <AIBadge /> : null}
          <span>Badge appears wherever Gemini-derived (structured) analytics outputs are used.</span>
        </div>
        <div className="mt-4 flex flex-wrap gap-2">
          <button
            disabled={busy}
            onClick={commissionerPdf}
            className="rounded-lg bg-slateink-900 px-3 py-2 text-sm font-semibold text-white hover:bg-slateink-800 disabled:opacity-50"
          >
            Generate Commissioner PDF <span className="ml-2">{showAI ? <AIBadge /> : null}</span>
          </button>
          <button
            disabled={busy}
            onClick={exportCsv}
            className="rounded-lg bg-white px-3 py-2 text-sm font-semibold text-slateink-800 ring-1 ring-slateink-200 hover:ring-slateink-300 disabled:opacity-50"
          >
            Export CSV (structured)
          </button>
        </div>
        {msg ? <div className="mt-3 text-sm text-slateink-700">{msg}</div> : null}
      </div>

      <div className="rounded-xl bg-white shadow-card ring-1 ring-slateink-100 p-4">
        <div className="text-sm font-semibold text-slateink-800">Demo checklist (acceptance)</div>
        <ul className="mt-3 space-y-2 text-sm text-slateink-700">
          <li>• Upload CSV → processing job → structured data created</li>
          <li>• All dashboards show non-empty charts</li>
          <li>• Feedback analytics surfaces top low-feedback drivers + reasons</li>
          <li>• Predictive view shows wards/categories at risk</li>
          <li>• Commissioner PDF downloads successfully</li>
        </ul>
      </div>
    </div>
  );
}


