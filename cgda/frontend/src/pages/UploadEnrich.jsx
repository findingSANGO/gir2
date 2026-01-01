import React, { useEffect, useMemo, useState } from "react";
import AIBadge from "../components/AIBadge.jsx";
import { api } from "../services/api.js";

function ProgressBar({ value }) {
  const pct = Math.max(0, Math.min(100, value || 0));
  return (
    <div className="h-2 w-full rounded-full bg-slateink-100 ring-1 ring-slateink-200 overflow-hidden">
      <div className="h-full bg-indigo-600" style={{ width: `${pct}%` }} />
    </div>
  );
}

export default function UploadEnrich() {
  const [latest, setLatest] = useState(null);
  const [runs, setRuns] = useState([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  const [results, setResults] = useState([]);
  const [resOffset, setResOffset] = useState(0);
  const [resTotal, setResTotal] = useState(0);

  async function refresh() {
    setErr("");
    const [l, r] = await Promise.all([api.dataLatest(), api.dataRuns()]);
    setLatest(l.latest || null);
    setRuns(r.runs || []);
  }

  useEffect(() => {
    refresh();
    const t = setInterval(() => refresh().catch(() => {}), 2500);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const active = useMemo(() => runs.find((x) => x.status === "running" || x.status === "queued"), [runs]);
  const last = useMemo(() => runs[0] || null, [runs]);

  const progress = useMemo(() => {
    const r = active || last;
    if (!r) return 0;
    const total = r.total_rows || 0;
    if (!total) return 0;
    return Math.round(((r.processed + r.skipped + r.failed) / total) * 100);
  }, [active, last]);

  async function runEnrichment() {
    setBusy(true);
    setErr("");
    try {
      await api.dataIngest(100);
      await refresh();
    } catch (e) {
      setErr(e.message || "Failed to start enrichment");
    } finally {
      setBusy(false);
    }
  }

  async function loadResults(offset = 0) {
    setBusy(true);
    setErr("");
    try {
      const res = await api.dataResults(100, offset);
      setResults(res.rows || []);
      setResTotal(res.total_rows || 0);
      setResOffset(offset);
    } catch (e) {
      setErr(e.message || "Failed to load results");
      setResults([]);
      setResTotal(0);
    } finally {
      setBusy(false);
    }
  }

  async function download() {
    setBusy(true);
    setErr("");
    try {
      const blob = await api.downloadEnrichedCsv();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "grievances_enriched.csv";
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (e) {
      setErr(e.message || "Download failed");
    } finally {
      setBusy(false);
    }
  }

  async function downloadPreprocessed() {
    setBusy(true);
    setErr("");
    try {
      const blob = await api.downloadPreprocessedCsv();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "preprocessed_latest.csv";
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (e) {
      setErr(e.message || "Download failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="space-y-5">
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-xl font-semibold text-slateink-900">Upload & Enrich</div>
          <div className="text-sm text-slateink-500">
            Drop the NMMC/IES export (CSV/XLSX) into <span className="font-mono">data/raw/</span>. The raw file is never modified.
          </div>
        </div>
        <div className="pt-1">
          <AIBadge text="Powered by caseA" />
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2 rounded-2xl bg-white shadow-card ring-1 ring-slateink-100 p-5">
          <div className="flex items-center justify-between gap-3">
            <div>
              <div className="text-sm font-semibold text-slateink-900">Latest detected raw file</div>
              <div className="mt-1 text-sm text-slateink-600">
                {latest ? (
                  <>
                    <span className="font-mono">{latest.filename}</span>
                    <span className="text-slateink-400"> · {latest.mtime_iso}</span>
                  </>
                ) : (
                  <span className="text-slateink-500">No file found in data/raw/</span>
                )}
              </div>
            </div>
            <button
              disabled={busy || !latest}
              onClick={runEnrichment}
              className="rounded-xl bg-slateink-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-50 hover:bg-slateink-800"
            >
              {busy ? "Starting..." : "Run Enrichment (100)"}
            </button>
          </div>

          <div className="mt-4">
            <div className="flex items-center justify-between text-xs text-slateink-500">
              <span>Progress</span>
              <span>{progress}%</span>
            </div>
            <div className="mt-2">
              <ProgressBar value={progress} />
            </div>
            {active ? (
              <div className="mt-2 text-xs text-slateink-600">
                Status: <span className="font-semibold">{active.status}</span> · processed {active.processed}, skipped {active.skipped},
                failed {active.failed}, total {active.total_rows}
              </div>
            ) : last ? (
              <div className="mt-2 text-xs text-slateink-600">
                Last run: <span className="font-semibold">{last.status}</span> · processed {last.processed}, skipped {last.skipped}, failed{" "}
                {last.failed}, total {last.total_rows}
              </div>
            ) : null}
            {err ? <div className="mt-3 text-sm text-red-700">{err}</div> : null}
          </div>
        </div>

        <div className="rounded-2xl bg-white shadow-card ring-1 ring-slateink-100 p-5">
          <div className="text-sm font-semibold text-slateink-900">Outputs</div>
          <div className="mt-1 text-xs text-slateink-500">Analytics-ready CSV with AI columns.</div>
          <button
            disabled={busy}
            onClick={downloadPreprocessed}
            className="mt-4 w-full rounded-xl bg-white px-4 py-2 text-sm font-semibold text-slateink-800 ring-1 ring-slateink-200 hover:ring-slateink-300 disabled:opacity-50"
          >
            Download Input Dataset (100)
          </button>
          <button
            disabled={busy}
            onClick={download}
            className="mt-3 w-full rounded-xl bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50"
          >
            Download Enriched CSV
          </button>
          <div className="mt-3 text-xs text-slateink-500">
            Includes: <span className="font-mono">AI_Category</span>, <span className="font-mono">AI_SubTopic</span>,{" "}
            <span className="font-mono">AI_InputHash</span>, <span className="font-mono">AI_Error</span>
          </div>
          <div className="mt-2 text-[10px] text-slateink-400">CaseA.ai</div>
        </div>
      </div>

      <div className="rounded-2xl bg-white shadow-card ring-1 ring-slateink-100 p-5">
        <div className="flex items-center justify-between">
          <div className="text-sm font-semibold text-slateink-900">Recent runs</div>
          <button
            onClick={() => refresh().catch(() => {})}
            className="rounded-lg bg-white px-3 py-2 text-xs font-semibold text-slateink-800 ring-1 ring-slateink-200 hover:ring-slateink-300"
          >
            Refresh
          </button>
        </div>
        <div className="mt-3 overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-slateink-500">
                <th className="py-2 pr-3">Run</th>
                <th className="py-2 pr-3">File</th>
                <th className="py-2 pr-3">Status</th>
                <th className="py-2 pr-3">Processed</th>
                <th className="py-2 pr-3">Skipped</th>
                <th className="py-2 pr-3">Failed</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((r) => (
                <tr key={r.run_id} className="border-t border-slateink-100">
                  <td className="py-2 pr-3 font-mono text-xs text-slateink-700">{r.run_id}</td>
                  <td className="py-2 pr-3 text-slateink-700">{r.raw_filename}</td>
                  <td className="py-2 pr-3">
                    <span
                      className={
                        "inline-flex items-center rounded-full px-2 py-1 text-xs font-semibold " +
                        (r.status === "completed"
                          ? "bg-emerald-50 text-emerald-700"
                          : r.status === "failed"
                          ? "bg-red-50 text-red-700"
                          : "bg-indigo-50 text-indigo-700")
                      }
                    >
                      {r.status}
                    </span>
                    {r.error ? <div className="mt-1 text-xs text-red-700 max-w-md">{r.error}</div> : null}
                  </td>
                  <td className="py-2 pr-3 text-slateink-700">{r.processed}</td>
                  <td className="py-2 pr-3 text-slateink-700">{r.skipped}</td>
                  <td className="py-2 pr-3 text-slateink-700">{r.failed}</td>
                </tr>
              ))}
              {!runs.length ? (
                <tr>
                  <td className="py-3 text-sm text-slateink-500" colSpan={6}>
                    No runs yet.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>

      <div className="rounded-2xl bg-white shadow-card ring-1 ring-slateink-100 p-5">
        <div className="flex items-center justify-between gap-3">
          <div>
            <div className="text-sm font-semibold text-slateink-900">
              Main results (Subcategories) <AIBadge />
            </div>
            <div className="text-xs text-slateink-500">
              Per grievance, the detected Subcategory is shown as AI_SubTopic. Showing {results.length ? `${resOffset + 1}–${resOffset + results.length}` : "—"} of{" "}
              {resTotal || "—"}.
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button
              disabled={busy || resOffset <= 0}
              onClick={() => loadResults(Math.max(0, resOffset - 100))}
              className="rounded-lg bg-white px-3 py-2 text-xs font-semibold text-slateink-800 ring-1 ring-slateink-200 hover:ring-slateink-300 disabled:opacity-50"
            >
              Prev
            </button>
            <button
              disabled={busy}
              onClick={() => loadResults(resOffset)}
              className="rounded-lg bg-white px-3 py-2 text-xs font-semibold text-slateink-800 ring-1 ring-slateink-200 hover:ring-slateink-300 disabled:opacity-50"
            >
              Load 100
            </button>
            <button
              disabled={busy || !resTotal || resOffset + results.length >= resTotal}
              onClick={() => loadResults(resOffset + 100)}
              className="rounded-lg bg-white px-3 py-2 text-xs font-semibold text-slateink-800 ring-1 ring-slateink-200 hover:ring-slateink-300 disabled:opacity-50"
            >
              Next
            </button>
          </div>
        </div>

        <div className="mt-3 overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-slateink-500">
                <th className="py-2 pr-3">Grievance</th>
                <th className="py-2 pr-3">Ward</th>
                <th className="py-2 pr-3">Department</th>
                <th className="py-2 pr-3">Category</th>
                <th className="py-2 pr-3">Subcategory</th>
                <th className="py-2 pr-3">Confidence</th>
              </tr>
            </thead>
            <tbody>
              {results.map((r) => (
                <tr key={r.grievance_id} className="border-t border-slateink-100">
                  <td className="py-2 pr-3">
                    <div className="font-mono text-xs text-slateink-700">{r.grievance_id}</div>
                    <div className="text-xs text-slateink-500 line-clamp-2 max-w-md">{r.subject}</div>
                  </td>
                  <td className="py-2 pr-3 text-slateink-700">{r.ward || "—"}</td>
                  <td className="py-2 pr-3 text-slateink-700">{r.department || "—"}</td>
                  <td className="py-2 pr-3 text-slateink-800">{r.category || "—"}</td>
                  <td className="py-2 pr-3">
                    <span className="inline-flex items-center rounded-full bg-indigo-50 px-2 py-1 text-xs font-semibold text-indigo-700">
                      {r.subcategory || "General Civic Issue"}
                    </span>
                    {r.error ? <div className="mt-1 text-xs text-red-700">{r.error}</div> : null}
                  </td>
                  <td className="py-2 pr-3 text-slateink-700">{r.confidence || "—"}</td>
                </tr>
              ))}
              {!results.length ? (
                <tr>
                  <td className="py-3 text-sm text-slateink-500" colSpan={6}>
                    Click <b>Load</b> after you run enrichment to see detected subcategories.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}


