import { useEffect, useMemo, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { Download, Layers } from "lucide-react";
import { api } from "../services/api.js";
import { Card, CardContent, CardHeader } from "../components/ui/Card.jsx";
import Button from "../components/ui/Button.jsx";
import Badge from "../components/ui/Badge.jsx";

function fmtDate(iso) {
  if (!iso) return "—";
  const d = new Date(`${iso}T00:00:00`);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleDateString(undefined, { day: "2-digit", month: "short", year: "numeric" });
}

function isEnriched(ds) {
  return Number(ds?.ai_subtopic_rows || 0) > 0 || Number(ds?.ai_category_rows || 0) > 0;
}

export default function Datasets() {
  const { search } = useLocation();
  const navigate = useNavigate();
  const [datasets, setDatasets] = useState([]);
  const [source, setSource] = useState("");
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [loadingRows, setLoadingRows] = useState(false);
  const [building, setBuilding] = useState(false);
  const [error, setError] = useState("");
  const [view, setView] = useState("ai_records"); // ai_records | preview

  async function refreshDatasets({ preferSource = null } = {}) {
    setLoading(true);
    setError("");
    const res = await api.datasetsProcessed();
    const all = res?.datasets || [];
    const enriched = all.filter(isEnriched);
    setDatasets(enriched);

    if (preferSource) {
      setSource(preferSource);
      return;
    }

    // Default: show run1_100 if available, else first enriched dataset.
    const run1 = enriched.find((d) => String(d.source || "").includes("__run1_100"));
    const chosen = run1?.source || enriched[0]?.source || "";
    setSource(chosen);
  }

  // Load dataset inventory
  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setError("");
      try {
        const res = await api.datasetsProcessed();
        if (cancelled) return;
        const all = res?.datasets || [];
        const enriched = all.filter(isEnriched);
        setDatasets(enriched);

        // Default: show run1_100 if available, else first enriched dataset.
        const run1 = enriched.find((d) => String(d.source || "").includes("__run1_100"));
        const chosen = run1?.source || enriched[0]?.source || "";
        setSource(chosen);
      } catch (e) {
        if (!cancelled) setError(e.message || "Failed to load datasets");
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const selected = useMemo(() => datasets.find((d) => d.source === source) || null, [datasets, source]);

  // Read view mode from query param (?view=ai-records|preview)
  useEffect(() => {
    try {
      const q = new URLSearchParams(search || "");
      const v = (q.get("view") || "").toLowerCase();
      if (v === "preview") setView("preview");
      if (v === "ai-records" || v === "ai_records") setView("ai_records");
    } catch {
      // ignore
    }
  }, [search]);

  function setViewAndUrl(next) {
    setView(next);
    try {
      const q = new URLSearchParams(search || "");
      q.set("view", next === "preview" ? "preview" : "ai-records");
      navigate({ search: `?${q.toString()}` }, { replace: true });
    } catch {
      // ignore
    }
  }

  // Load rows for selected dataset
  useEffect(() => {
    let cancelled = false;
    (async () => {
      if (!source) {
        setRows([]);
        return;
      }
      setLoadingRows(true);
      setError("");
      try {
        const first = await api.dataResults(200, 0, { source });
        if (cancelled) return;
        const total = Number(first?.total_rows || 0);
        let out = [...(first?.rows || [])];

        // Fetch remaining pages only if needed (cap to keep UI fast)
        const hardCap = 5000;
        let offset = out.length;
        while (offset < total && offset < hardCap) {
          // eslint-disable-next-line no-await-in-loop
          const page = await api.dataResults(200, offset, { source });
          if (cancelled) return;
          out = out.concat(page?.rows || []);
          offset = out.length;
        }

        setRows(out);
      } catch (e) {
        if (!cancelled) setError(e.message || "Failed to load dataset rows");
      } finally {
        if (!cancelled) setLoadingRows(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [source]);

  const downloadHref = useMemo(() => {
    if (!source) return "#";
    const q = new URLSearchParams({ source });
    return `/api/data/processed/download?${q.toString()}`;
  }, [source]);

  const inputCols = [
    { key: "raw_id", label: "raw_id" },
    { key: "grievance_code", label: "grievance_code" },
    { key: "created_date", label: "created_date" },
    { key: "department", label: "department" },
    { key: "status", label: "status" },
    { key: "subject", label: "subject" },
    { key: "description", label: "description" }
  ];

  const aiCols = [
    { key: "category", label: "ai_category" },
    { key: "subcategory", label: "ai_subtopic" },
    { key: "confidence", label: "ai_confidence" },
    { key: "issue_type", label: "ai_issue_type" },
    { key: "entities_json", label: "ai_entities" },
    { key: "urgency", label: "ai_urgency" },
    { key: "sentiment", label: "ai_sentiment" },
    { key: "resolution_quality", label: "ai_resolution_quality" },
    { key: "reopen_risk", label: "ai_reopen_risk" },
    { key: "feedback_driver", label: "ai_feedback_driver" },
    { key: "closure_theme", label: "ai_closure_theme" },
    { key: "extra_summary", label: "ai_extra_summary" },
    { key: "ai_model", label: "ai_model" },
    { key: "ai_error", label: "ai_error" }
  ];

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader
          title="Datasets"
          subtitle="AI-enriched dataset snapshots available for dashboards (stored in SQLite; no Gemini calls on page load)."
          right={
            <div className="flex items-center gap-2">
              <Badge variant="ai">Enriched only</Badge>
              <Layers className="h-4 w-4 text-slateink-400" />
            </div>
          }
        />
        <CardContent>
          {error ? (
            <div className="rounded-xl bg-white p-4 ring-1 ring-slateink-100 text-sm text-rose-700">{error}</div>
          ) : null}

          <div className="flex flex-wrap items-end gap-3">
            <div className="min-w-[320px]">
              <div className="text-xs font-semibold text-slateink-600">Dataset snapshot</div>
              <select
                value={source}
                onChange={(e) => setSource(e.target.value)}
                disabled={loading}
                className="mt-1 h-11 w-full rounded-xl border border-slateink-200 bg-white px-3 text-sm outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100 disabled:bg-slateink-50"
              >
                {!datasets.length ? <option value="">No enriched datasets found</option> : null}
                {datasets.map((d) => (
                  <option key={d.source} value={d.source}>
                    {d.source}
                  </option>
                ))}
              </select>
              {selected ? (
                <div className="mt-1 text-[11px] text-slateink-500">
                  <span className="font-semibold text-slateink-700">Rows:</span> {Number(selected.count || 0).toLocaleString()}
                  {"  "}•{" "}
                  <span className="font-semibold text-slateink-700">AI:</span>{" "}
                  {Number(selected.ai_subtopic_rows || 0).toLocaleString()}
                  {"  "}•{" "}
                  <span className="font-semibold text-slateink-700">Range:</span> {fmtDate(selected.min_created_date)} →{" "}
                  {fmtDate(selected.max_created_date)}
                </div>
              ) : null}
            </div>

            <div className="ml-auto flex items-center gap-2">
              <div className="hidden md:flex items-center gap-2 mr-2">
                <Button variant={view === "ai_records" ? "dark" : "secondary"} onClick={() => setViewAndUrl("ai_records")}>
                  Dataset AI Records
                </Button>
                <Button variant={view === "preview" ? "dark" : "secondary"} onClick={() => setViewAndUrl("preview")}>
                  Preview (compact)
                </Button>
              </div>
              <Button
                variant="dark"
                disabled={!source || building}
                onClick={async () => {
                  if (!source) return;
                  setBuilding(true);
                  setError("");
                  try {
                    const res = await api.buildAiOutputDataset({
                      baseSource: source,
                      sampleSize: 100,
                      outputSource: "ai_output_dataset",
                      forceReprocess: true
                    });
                    await refreshDatasets({ preferSource: res?.output_source || "ai_output_dataset" });
                  } catch (e) {
                    setError(e.message || "Failed to start Gemini run");
                  } finally {
                    setBuilding(false);
                  }
                }}
                title="Clone 100 rows from selected dataset into ai_output_dataset and run Gemini enrichment"
              >
                {building ? "Starting Gemini…" : "Build AI Output (100)"}
              </Button>
              <Button
                variant="secondary"
                onClick={() => window.open(downloadHref, "_blank", "noopener,noreferrer")}
                disabled={!source}
                title="Download processed dataset as CSV"
              >
                <Download className="h-4 w-4" />
                Download CSV
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader
          title={
            source
              ? view === "ai_records"
                ? `Dataset AI Records: ${source}`
                : `Preview: ${source}`
              : view === "ai_records"
                ? "Dataset AI Records"
                : "Preview"
          }
          subtitle={
            source
              ? loadingRows
                ? "Loading rows…"
                : `Showing ${rows.length.toLocaleString()} row(s)`
              : "Select a dataset snapshot to view records"
          }
        />
        <CardContent>
          {!source ? (
            <div className="text-sm text-slateink-600">Pick a dataset snapshot above.</div>
          ) : loadingRows ? (
            <div className="text-sm text-slateink-600">Loading…</div>
          ) : !rows.length ? (
            <div className="text-sm text-slateink-600">No rows found for this dataset.</div>
          ) : (
            <div className="overflow-auto rounded-xl ring-1 ring-slateink-100">
              {view === "ai_records" ? (
                <div className="p-3 bg-white">
                  <div className="flex flex-wrap items-center gap-2 mb-3">
                    <span className="text-xs font-semibold text-slateink-600">Legend:</span>
                    <span className="rounded-full bg-amber-50 text-amber-900 ring-1 ring-amber-200 px-2 py-1 text-[11px] font-semibold">
                      Collected data (raw/preprocessed)
                    </span>
                    <span className="rounded-full bg-emerald-50 text-emerald-900 ring-1 ring-emerald-200 px-2 py-1 text-[11px] font-semibold">
                      AI outputs (Gemini)
                    </span>
                    <span className="text-[11px] text-slateink-500">
                      Columns align to `ticket_record_enrichment_prompt.txt` output schema.
                    </span>
                  </div>
                  <table className="min-w-[1600px] w-full border-collapse bg-white text-sm">
                    <thead className="text-slateink-700">
                      <tr>
                        {inputCols.map((c) => (
                          <th
                            key={c.key}
                            className="px-3 py-2 text-left text-xs font-semibold whitespace-nowrap bg-amber-50 border-b border-amber-100"
                          >
                            {c.label}
                          </th>
                        ))}
                        {aiCols.map((c) => (
                          <th
                            key={c.key}
                            className="px-3 py-2 text-left text-xs font-semibold whitespace-nowrap bg-emerald-50 border-b border-emerald-100"
                          >
                            {c.label}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((r) => (
                        <tr key={r.grievance_id} className="border-t border-slateink-100">
                          <td className="px-3 py-2 whitespace-nowrap bg-amber-50/60">{r.raw_id || "—"}</td>
                          <td className="px-3 py-2 whitespace-nowrap bg-amber-50/60">{r.grievance_code || "—"}</td>
                          <td className="px-3 py-2 whitespace-nowrap bg-amber-50/60">{r.created_date ? fmtDate(r.created_date) : "—"}</td>
                          <td className="px-3 py-2 whitespace-nowrap bg-amber-50/60">{r.department || "—"}</td>
                          <td className="px-3 py-2 whitespace-nowrap bg-amber-50/60">{r.status || "—"}</td>
                          <td className="px-3 py-2 bg-amber-50/60 max-w-[280px]">
                            <div className="line-clamp-2">{r.subject || "—"}</div>
                          </td>
                          <td className="px-3 py-2 bg-amber-50/60 max-w-[420px]">
                            <div className="line-clamp-3 whitespace-pre-wrap">{r.description || "—"}</div>
                          </td>

                          <td className="px-3 py-2 whitespace-nowrap bg-emerald-50/60">{r.category || "—"}</td>
                          <td className="px-3 py-2 whitespace-nowrap bg-emerald-50/60">{r.subcategory || "—"}</td>
                          <td className="px-3 py-2 whitespace-nowrap bg-emerald-50/60">{r.confidence || "—"}</td>
                          <td className="px-3 py-2 whitespace-nowrap bg-emerald-50/60">{r.issue_type || "—"}</td>
                          <td className="px-3 py-2 bg-emerald-50/60 max-w-[260px]">
                            <div className="line-clamp-2">{r.entities_json || "—"}</div>
                          </td>
                          <td className="px-3 py-2 whitespace-nowrap bg-emerald-50/60">{r.urgency || "—"}</td>
                          <td className="px-3 py-2 whitespace-nowrap bg-emerald-50/60">{r.sentiment || "—"}</td>
                          <td className="px-3 py-2 whitespace-nowrap bg-emerald-50/60">{r.resolution_quality || "—"}</td>
                          <td className="px-3 py-2 whitespace-nowrap bg-emerald-50/60">{r.reopen_risk || "—"}</td>
                          <td className="px-3 py-2 bg-emerald-50/60 max-w-[220px]">
                            <div className="line-clamp-2">{r.feedback_driver || "—"}</div>
                          </td>
                          <td className="px-3 py-2 bg-emerald-50/60 max-w-[220px]">
                            <div className="line-clamp-2">{r.closure_theme || "—"}</div>
                          </td>
                          <td className="px-3 py-2 bg-emerald-50/60 max-w-[420px]">
                            <div className="line-clamp-3 whitespace-pre-wrap">{r.extra_summary || "—"}</div>
                          </td>
                          <td className="px-3 py-2 whitespace-nowrap bg-emerald-50/60">{r.ai_model || "—"}</td>
                          <td className="px-3 py-2 bg-emerald-50/60 max-w-[260px]">
                            <div className="line-clamp-2 text-rose-700">{r.ai_error || "—"}</div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <table className="min-w-[1000px] w-full border-collapse bg-white text-sm">
                  <thead className="bg-slateink-50 text-slateink-700">
                    <tr>
                      {[
                        "raw_id",
                        "grievance_code",
                        "created_date",
                        "ward",
                        "department",
                        "status",
                        "category",
                        "subcategory",
                        "confidence",
                        "urgency",
                        "sentiment",
                        "actionable_score"
                      ].map((h) => (
                        <th key={h} className="px-3 py-2 text-left text-xs font-semibold whitespace-nowrap">
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((r) => (
                      <tr key={r.grievance_id} className="border-t border-slateink-100">
                        <td className="px-3 py-2 whitespace-nowrap">{r.raw_id || "—"}</td>
                        <td className="px-3 py-2 whitespace-nowrap">{r.grievance_code || "—"}</td>
                        <td className="px-3 py-2 whitespace-nowrap">{r.created_date ? fmtDate(r.created_date) : "—"}</td>
                        <td className="px-3 py-2 whitespace-nowrap">{r.ward || "—"}</td>
                        <td className="px-3 py-2 whitespace-nowrap">{r.department || "—"}</td>
                        <td className="px-3 py-2 whitespace-nowrap">{r.status || "—"}</td>
                        <td className="px-3 py-2 whitespace-nowrap">{r.category || "—"}</td>
                        <td className="px-3 py-2 whitespace-nowrap">{r.subcategory || "—"}</td>
                        <td className="px-3 py-2 whitespace-nowrap">{r.confidence || "—"}</td>
                        <td className="px-3 py-2 whitespace-nowrap">{r.urgency || "—"}</td>
                        <td className="px-3 py-2 whitespace-nowrap">{r.sentiment || "—"}</td>
                        <td className="px-3 py-2 whitespace-nowrap">{r.actionable_score ?? "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}


