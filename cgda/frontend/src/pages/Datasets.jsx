import { useEffect, useMemo, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { Layers } from "lucide-react";
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

export default function Datasets() {
  const { search } = useLocation();
  const navigate = useNavigate();
  const [datasets, setDatasets] = useState([]);
  const [source, setSource] = useState("");
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [loadingRows, setLoadingRows] = useState(false);
  const [error, setError] = useState("");
  const [view, setView] = useState("ai_records"); // ai_records | preview
  const [pageSize, setPageSize] = useState(50);
  const [pageOffset, setPageOffset] = useState(0);
  const [totalRows, setTotalRows] = useState(0);
  const [quality, setQuality] = useState(null);
  const [qualityLoading, setQualityLoading] = useState(false);
  const [aiCoverage, setAiCoverage] = useState(null);
  const [aiCoverageLoading, setAiCoverageLoading] = useState(false);

  async function refreshDatasets({ preferSource = null } = {}) {
    setLoading(true);
    setError("");
    const res = await api.datasetsProcessed();
    const all = res?.datasets || [];
    setDatasets(all);

    if (preferSource) {
      setSource(preferSource);
      return;
    }

    // Default: prefer the final full dataset when available.
    const preferred =
      all.find((d) => d.source === "processed_data_raw4")?.source ||
      all.find((d) => d.source === "processed_data_500")?.source;
    const chosen = preferred || all[0]?.source || "";
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
        setDatasets(all);

        const preferred =
          all.find((d) => d.source === "processed_data_raw4")?.source ||
          all.find((d) => d.source === "processed_data_500")?.source;
        const chosen = preferred || all[0]?.source || "";
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
        setTotalRows(0);
        setPageOffset(0);
        return;
      }
      setLoadingRows(true);
      setError("");
      try {
        const res = await api.dataResults(Number(pageSize || 50), Number(pageOffset || 0), { source });
        if (cancelled) return;
        setRows(res?.rows || []);
        setTotalRows(Number(res?.total_rows || 0));
      } catch (e) {
        if (!cancelled) setError(e.message || "Failed to load dataset rows");
      } finally {
        if (!cancelled) setLoadingRows(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [source, pageSize, pageOffset]);

  // Load cross-check stats for selected dataset
  useEffect(() => {
    let cancelled = false;
    (async () => {
      if (!source) {
        setQuality(null);
        return;
      }
      setQualityLoading(true);
      try {
        const q = await api.datasetQuality(source);
        if (cancelled) return;
        setQuality(q);
      } catch (e) {
        if (cancelled) return;
        // Keep page usable even if this fails.
        setQuality(null);
      } finally {
        if (!cancelled) setQualityLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [source]);

  // Load AI column coverage stats for selected dataset
  useEffect(() => {
    let cancelled = false;
    (async () => {
      if (!source) {
        setAiCoverage(null);
        return;
      }
      setAiCoverageLoading(true);
      try {
        const c = await api.aiCoverage(source);
        if (cancelled) return;
        setAiCoverage(c);
      } catch (e) {
        if (cancelled) return;
        setAiCoverage(null);
      } finally {
        if (!cancelled) setAiCoverageLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [source]);

  useEffect(() => {
    // Reset pagination on dataset switch.
    setPageOffset(0);
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

  const aiCoverageRows = useMemo(() => {
    const total = Number(aiCoverage?.total_rows || 0);
    const counts = aiCoverage?.counts || {};
    const order = [
      "ai_category",
      "ai_subtopic",
      "ai_confidence",
      "ai_issue_type",
      "ai_entities",
      "ai_urgency",
      "ai_sentiment",
      "ai_resolution_quality",
      "ai_reopen_risk",
      "ai_feedback_driver",
      "ai_closure_theme",
      "ai_extra_summary",
      "ai_model",
      "ai_error"
    ];
    return order.map((k) => {
      const filled = Number(counts?.[k] || 0);
      const blank = Math.max(0, total - filled);
      const pct = total > 0 ? Math.round((filled / total) * 1000) / 10 : 0;
      return { key: k, filled, blank, pct, total };
    });
  }, [aiCoverage]);

  const page = Math.floor((pageOffset || 0) / (pageSize || 50)) + 1;
  const totalPages = totalRows ? Math.max(1, Math.ceil(totalRows / (pageSize || 50))) : 1;
  const canPrev = (pageOffset || 0) > 0;
  const canNext = (pageOffset || 0) + (pageSize || 50) < totalRows;

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader
          title="Datasets"
          subtitle="Browse AI-enriched dataset snapshots used by dashboards (read-only)."
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
                {!datasets.length ? <option value="">No datasets found</option> : null}
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
            </div>
          </div>

          {source ? (
            <div className="mt-4 rounded-2xl bg-white ring-1 ring-slateink-100 p-4">
              <div className="text-sm font-semibold text-slateink-900">Dataset quality cross-checks</div>
              <div className="mt-1 text-xs text-slateink-500">
                Counts are computed on the selected dataset source in SQLite (fast aggregates).
              </div>
              {qualityLoading ? (
                <div className="mt-3 text-sm text-slateink-600">Loading…</div>
              ) : quality ? (
                <div className="mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
                  <div className="rounded-xl bg-slateink-50 ring-1 ring-slateink-100 p-3">
                    <div className="text-[11px] font-semibold text-slateink-600">Total rows</div>
                    <div className="mt-1 text-lg font-semibold text-slateink-900">{Number(quality.total_rows || 0).toLocaleString()}</div>
                  </div>
                  <div className="rounded-xl bg-slateink-50 ring-1 ring-slateink-100 p-3">
                    <div className="text-[11px] font-semibold text-slateink-600">Duplicate rows</div>
                    <div className="mt-1 text-lg font-semibold text-slateink-900">
                      {Number(quality?.duplicates?.duplicate_rows || 0).toLocaleString()}
                    </div>
                    <div className="mt-1 text-[11px] text-slateink-500">By raw_id</div>
                  </div>
                  <div className="rounded-xl bg-slateink-50 ring-1 ring-slateink-100 p-3">
                    <div className="text-[11px] font-semibold text-slateink-600">Deduplicated total</div>
                    <div className="mt-1 text-lg font-semibold text-slateink-900">
                      {Number(quality.deduplicated_total_rows || 0).toLocaleString()}
                    </div>
                  </div>
                  <div className="rounded-xl bg-slateink-50 ring-1 ring-slateink-100 p-3">
                    <div className="text-[11px] font-semibold text-slateink-600">Has closing date</div>
                    <div className="mt-1 text-lg font-semibold text-slateink-900">
                      {Number(quality.closed_date_rows || 0).toLocaleString()}
                    </div>
                  </div>
                  <div className="rounded-xl bg-slateink-50 ring-1 ring-slateink-100 p-3">
                    <div className="text-[11px] font-semibold text-slateink-600">Has rating</div>
                    <div className="mt-1 text-lg font-semibold text-slateink-900">
                      {Number(quality.star_rating_rows || 0).toLocaleString()}
                    </div>
                    <div className="mt-1 text-[11px] text-slateink-500">
                      Both: {Number(quality.closed_date_and_star_rating_rows || 0).toLocaleString()}
                    </div>
                  </div>
                </div>
              ) : (
                <div className="mt-3 text-sm text-slateink-600">Cross-checks unavailable for this dataset.</div>
              )}

              <div className="mt-5 border-t border-slateink-100 pt-4">
                <div className="text-sm font-semibold text-slateink-900">AI output coverage (by column)</div>
                <div className="mt-1 text-xs text-slateink-500">
                  Filled = non-empty values present in `grievances_processed` for the selected dataset source.
                </div>
                {aiCoverageLoading ? (
                  <div className="mt-3 text-sm text-slateink-600">Loading…</div>
                ) : aiCoverage ? (
                  <div className="mt-3 overflow-auto rounded-xl ring-1 ring-slateink-100">
                    <table className="min-w-[720px] w-full border-collapse bg-white text-sm">
                      <thead className="bg-emerald-50 text-emerald-900">
                        <tr>
                          <th className="px-3 py-2 text-left text-xs font-semibold whitespace-nowrap border-b border-emerald-100">
                            AI column
                          </th>
                          <th className="px-3 py-2 text-right text-xs font-semibold whitespace-nowrap border-b border-emerald-100">
                            Filled
                          </th>
                          <th className="px-3 py-2 text-right text-xs font-semibold whitespace-nowrap border-b border-emerald-100">
                            Blank
                          </th>
                          <th className="px-3 py-2 text-right text-xs font-semibold whitespace-nowrap border-b border-emerald-100">
                            Coverage
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {aiCoverageRows.map((r) => (
                          <tr key={r.key} className="border-t border-slateink-100">
                            <td className="px-3 py-2 bg-emerald-50/60 font-semibold text-emerald-900 whitespace-nowrap">
                              {r.key}
                            </td>
                            <td className="px-3 py-2 text-right bg-white tabular-nums">{r.filled.toLocaleString()}</td>
                            <td className="px-3 py-2 text-right bg-white tabular-nums text-slateink-600">{r.blank.toLocaleString()}</td>
                            <td className="px-3 py-2 text-right bg-white tabular-nums">
                              {r.total ? `${r.pct.toLocaleString()}%` : "—"}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="mt-3 text-sm text-slateink-600">AI coverage unavailable for this dataset.</div>
                )}
              </div>
            </div>
          ) : null}
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
                : totalRows
                  ? `Page ${page.toLocaleString()} of ${totalPages.toLocaleString()} • ${totalRows.toLocaleString()} total row(s)`
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
                  <div className="flex flex-wrap items-center justify-between gap-2 mb-3">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="text-xs font-semibold text-slateink-600">Legend:</span>
                      <span className="rounded-full bg-amber-50 text-amber-900 ring-1 ring-amber-200 px-2 py-1 text-[11px] font-semibold">
                        Collected data (raw/preprocessed)
                      </span>
                      <span className="rounded-full bg-emerald-50 text-emerald-900 ring-1 ring-emerald-200 px-2 py-1 text-[11px] font-semibold">
                        AI outputs
                      </span>
                      <span className="text-[11px] text-slateink-500">
                        Columns align to the configured AI output schema.
                      </span>
                    </div>
                    <div className="flex items-center gap-2">
                      <label className="text-xs font-semibold text-slateink-600">Rows per page</label>
                      <select
                        value={String(pageSize)}
                        onChange={(e) => {
                          const next = Number(e.target.value || 50);
                          setPageSize(next);
                          setPageOffset(0);
                        }}
                        className="h-9 rounded-xl border border-slateink-200 bg-white px-2 text-xs outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
                      >
                        {[25, 50, 100, 200].map((n) => (
                          <option key={n} value={n}>
                            {n}
                          </option>
                        ))}
                      </select>
                      <Button variant="secondary" disabled={!canPrev} onClick={() => setPageOffset(Math.max(0, pageOffset - pageSize))}>
                        Prev
                      </Button>
                      <Button variant="secondary" disabled={!canNext} onClick={() => setPageOffset(pageOffset + pageSize)}>
                        Next
                      </Button>
                    </div>
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


