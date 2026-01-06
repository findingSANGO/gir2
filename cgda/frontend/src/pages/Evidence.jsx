import { useEffect, useState } from "react";
import { api } from "../services/api.js";

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
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState("");
  const [latest, setLatest] = useState({ weekly: null, monthly: null, quarterly: null, annual: null });
  const [file, setFile] = useState(null);
  const [periodType, setPeriodType] = useState("weekly");
  const [periodStart, setPeriodStart] = useState("");
  const [periodEnd, setPeriodEnd] = useState("");
  const [notes, setNotes] = useState("");

  async function refreshLatest() {
    try {
      const [w, m, q, a] = await Promise.all([
        api.reportsLatest("weekly"),
        api.reportsLatest("monthly"),
        api.reportsLatest("quarterly"),
        api.reportsLatest("annual")
      ]);
      setLatest({
        weekly: w.latest,
        monthly: m.latest,
        quarterly: q.latest,
        annual: a.latest
      });
    } catch {
      // ignore
    }
  }

  useEffect(() => {
    refreshLatest();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function upload() {
    if (!file) return;
    setBusy(true);
    setMsg("");
    try {
      if (!periodStart || !periodEnd) {
        setMsg("Please set period start and end dates.");
        return;
      }
      await api.reportsUpload({ file, periodType, periodStart, periodEnd, notes: notes || null });
      setFile(null);
      setNotes("");
      setMsg("Report uploaded.");
      await refreshLatest();
    } catch (e) {
      setMsg(e.message || "Upload failed");
    } finally {
      setBusy(false);
    }
  }

  function Card({ title, row }) {
    return (
      <div className="rounded-xl bg-white shadow-card ring-1 ring-slateink-100 p-4">
        <div className="text-sm font-semibold text-slateink-800">{title}</div>
        <div className="mt-1 text-xs text-slateink-500">Manual upload (looks automated; no GenAI)</div>
        {row ? (
          <div className="mt-3 text-sm text-slateink-700">
            <div>
              <span className="font-semibold">Period:</span> {row.period_start} → {row.period_end}
            </div>
            <div className="text-xs text-slateink-500 mt-1">Uploaded: {row.uploaded_at}</div>
            <button
              disabled={busy}
              onClick={async () => {
                const token = localStorage.getItem("cgda_token");
                const res = await fetch(api.reportsDownloadUrl(row.id), { headers: { Authorization: `Bearer ${token}` } });
                const blob = await res.blob();
                downloadBlob(blob, `${row.period_type}_${row.period_start}_${row.period_end}.pdf`);
              }}
              className="mt-3 rounded-lg bg-slateink-900 px-3 py-2 text-sm font-semibold text-white hover:bg-slateink-800 disabled:opacity-50"
            >
              Download Latest
            </button>
          </div>
        ) : (
          <div className="mt-3 text-sm text-slateink-500">No report uploaded yet.</div>
        )}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="rounded-xl bg-white shadow-card ring-1 ring-slateink-100 p-4">
        <div className="text-sm font-semibold text-slateink-800">Evidence (manual uploads)</div>
        <div className="mt-2 text-sm text-slateink-700">
          Upload weekly/monthly/quarterly/annual PDF reports. This is purely manual right now, but presented like an automated evidence feed.
        </div>

        <div className="mt-4 grid gap-3 md:grid-cols-2">
          <div>
            <div className="text-xs font-semibold text-slateink-600">PDF file</div>
            <input
              type="file"
              accept="application/pdf"
              onChange={(e) => setFile(e.target.files?.[0] || null)}
              className="mt-1 block w-full text-sm text-slateink-700 file:mr-3 file:rounded-lg file:border-0 file:bg-gov-600 file:px-3 file:py-2 file:text-sm file:font-semibold file:text-white hover:file:bg-gov-700"
            />
          </div>
          <div>
            <div className="text-xs font-semibold text-slateink-600">Period type</div>
            <select
              value={periodType}
              onChange={(e) => setPeriodType(e.target.value)}
              className="mt-1 h-10 w-full rounded-xl border border-slateink-200 bg-white px-3 text-sm outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
            >
              <option value="weekly">Weekly</option>
              <option value="monthly">Monthly</option>
              <option value="quarterly">Quarterly</option>
              <option value="annual">Annual</option>
            </select>
          </div>
          <div>
            <div className="text-xs font-semibold text-slateink-600">Period start</div>
            <input
              type="date"
              value={periodStart}
              onChange={(e) => setPeriodStart(e.target.value)}
              className="mt-1 h-10 w-full rounded-xl border border-slateink-200 bg-white px-3 text-sm outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
            />
          </div>
          <div>
            <div className="text-xs font-semibold text-slateink-600">Period end</div>
            <input
              type="date"
              value={periodEnd}
              onChange={(e) => setPeriodEnd(e.target.value)}
              className="mt-1 h-10 w-full rounded-xl border border-slateink-200 bg-white px-3 text-sm outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
            />
          </div>
          <div className="md:col-span-2">
            <div className="text-xs font-semibold text-slateink-600">Notes (optional)</div>
            <input
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              placeholder="e.g., Uploaded by IT Head, scan from meeting minutes"
              className="mt-1 h-10 w-full rounded-xl border border-slateink-200 bg-white px-3 text-sm outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
            />
          </div>
        </div>

        <div className="mt-4 flex items-center gap-2">
          <button
            disabled={busy || !file}
            onClick={upload}
            className="rounded-lg bg-slateink-900 px-3 py-2 text-sm font-semibold text-white hover:bg-slateink-800 disabled:opacity-50"
          >
            Upload Report
          </button>
          <button
            disabled={busy}
            onClick={refreshLatest}
            className="rounded-lg bg-white px-3 py-2 text-sm font-semibold text-slateink-800 ring-1 ring-slateink-200 hover:ring-slateink-300 disabled:opacity-50"
          >
            Refresh
          </button>
        </div>
        {msg ? <div className="mt-3 text-sm text-slateink-700">{msg}</div> : null}
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Card title="Weekly" row={latest.weekly} />
        <Card title="Monthly" row={latest.monthly} />
        <Card title="Quarterly" row={latest.quarterly} />
        <Card title="Annual" row={latest.annual} />
      </div>
    </div>
  );
}


