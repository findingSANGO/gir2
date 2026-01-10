import { useEffect, useMemo, useState } from "react";
import { api } from "../services/api.js";
import Button from "./ui/Button.jsx";

function fmt(v) {
  if (v == null || v === "") return "—";
  return String(v);
}

function clip(s, n = 120) {
  const t = String(s || "").replace(/\s+/g, " ").trim();
  if (!t) return "";
  return t.length > n ? `${t.slice(0, n)}…` : t;
}

export default function CaseListModal({
  open,
  onClose,
  title = "Case list",
  subtitle = "",
  filters,
  drillField,
  drillValue
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [payload, setPayload] = useState(null);
  const [pageSize, setPageSize] = useState(25);
  const [page, setPage] = useState(0);

  const offset = page * pageSize;

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    (async () => {
      setBusy(true);
      setError("");
      try {
        const res = await api.cases(filters, {
          limit: pageSize,
          offset,
          drillField,
          drillValue
        });
        if (!cancelled) setPayload(res);
      } catch (e) {
        if (!cancelled) setError(e.message || "Failed to load cases");
      } finally {
        if (!cancelled) setBusy(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [open, pageSize, offset, drillField, drillValue, filters]);

  // Reset paging when drilldown changes / opened anew.
  useEffect(() => {
    if (!open) return;
    setPage(0);
  }, [open, drillField, drillValue]);

  const rows = payload?.rows || [];
  const total = payload?.total_rows ?? 0;
  const pageCount = pageSize ? Math.ceil(total / pageSize) : 0;

  const canPrev = page > 0;
  const canNext = offset + pageSize < total;

  const headerRight = useMemo(() => {
    return (
      <div className="flex items-center gap-2">
        <span className="text-xs text-slateink-500">Rows</span>
        <select
          value={pageSize}
          onChange={(e) => setPageSize(Number(e.target.value))}
          className="h-9 rounded-xl border border-slateink-200 bg-white px-3 text-xs font-semibold outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
        >
          {[10, 25, 50, 100].map((n) => (
            <option key={n} value={n}>
              {n}
            </option>
          ))}
        </select>
        <Button variant="secondary" onClick={onClose}>
          Close
        </Button>
      </div>
    );
  }, [pageSize, onClose]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-[100]">
      <div className="absolute inset-0 bg-slateink-900/40" onClick={onClose} />
      <div className="absolute inset-x-0 top-10 mx-auto w-[min(1200px,94vw)]">
        <div className="rounded-2xl bg-white shadow-card ring-1 ring-slateink-200 overflow-hidden">
          <div className="px-5 py-4 border-b border-slateink-100 flex items-start justify-between gap-4">
            <div>
              <div className="text-lg font-semibold text-slateink-900">{title}</div>
              {subtitle ? <div className="mt-1 text-xs text-slateink-500">{subtitle}</div> : null}
              <div className="mt-1 text-xs text-slateink-500">
                {busy ? "Loading…" : `${total.toLocaleString()} records`}
                {drillField ? (
                  <span className="ml-2">
                    • Filter: <span className="font-semibold">{drillField}</span> ={" "}
                    <span className="font-semibold">{fmt(drillValue)}</span>
                  </span>
                ) : null}
              </div>
            </div>
            {headerRight}
          </div>

          <div className="p-4">
            {error ? (
              <div className="rounded-xl bg-white p-4 ring-1 ring-rose-200 text-sm text-rose-700">{error}</div>
            ) : null}

            <div className="overflow-auto rounded-xl ring-1 ring-slateink-200">
              <table className="min-w-[1200px] w-full text-sm">
                <thead className="bg-slateink-50">
                  <tr className="text-left text-xs font-semibold tracking-wide text-slateink-600">
                    <th className="py-3 px-3">Grievance ID</th>
                    <th className="py-3 px-3">Created</th>
                    <th className="py-3 px-3">Ward</th>
                    <th className="py-3 px-3">Department</th>
                    <th className="py-3 px-3">Status</th>
                    <th className="py-3 px-3">AI Subtopic</th>
                    <th className="py-3 px-3">Urgency</th>
                    <th className="py-3 px-3">Sentiment</th>
                    <th className="py-3 px-3">SLA (days)</th>
                    <th className="py-3 px-3">Forwards</th>
                    <th className="py-3 px-3">Rating</th>
                    <th className="py-3 px-3">Subject</th>
                    <th className="py-3 px-3">Description</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slateink-100">
                  {busy && !rows.length ? (
                    <tr>
                      <td className="py-6 px-3 text-slateink-500" colSpan={13}>
                        Loading…
                      </td>
                    </tr>
                  ) : !rows.length ? (
                    <tr>
                      <td className="py-6 px-3 text-slateink-500" colSpan={13}>
                        No cases found for this selection.
                      </td>
                    </tr>
                  ) : (
                    rows.map((r) => (
                      <tr key={r.grievance_id} className="hover:bg-slateink-50/60">
                        <td className="py-3 px-3 font-mono text-xs">{fmt(r.grievance_id)}</td>
                        <td className="py-3 px-3">{fmt(r.created_date)}</td>
                        <td className="py-3 px-3">{fmt(r.ward)}</td>
                        <td className="py-3 px-3">{fmt(r.department)}</td>
                        <td className="py-3 px-3">{fmt(r.status)}</td>
                        <td className="py-3 px-3">{fmt(r.ai_subtopic)}</td>
                        <td className="py-3 px-3">{fmt(r.ai_urgency)}</td>
                        <td className="py-3 px-3">{fmt(r.ai_sentiment)}</td>
                        <td className="py-3 px-3">{fmt(r.resolution_days)}</td>
                        <td className="py-3 px-3">{fmt(r.forward_count)}</td>
                        <td className="py-3 px-3">{fmt(r.feedback_rating)}</td>
                        <td className="py-3 px-3" title={fmt(r.subject)}>
                          {clip(r.subject, 90) || "—"}
                        </td>
                        <td className="py-3 px-3" title={fmt(r.description)}>
                          {clip(r.description, 140) || "—"}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>

            <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
              <div className="text-xs text-slateink-500">
                Showing{" "}
                <span className="font-semibold">
                  {total ? `${offset + 1}-${Math.min(offset + pageSize, total)}` : "0"}
                </span>{" "}
                of <span className="font-semibold">{total.toLocaleString()}</span>
              </div>
              <div className="flex items-center gap-2">
                <Button variant="secondary" disabled={!canPrev} onClick={() => setPage((p) => Math.max(0, p - 1))}>
                  Prev
                </Button>
                <div className="text-xs font-semibold text-slateink-700">
                  Page {pageCount ? page + 1 : 0} / {pageCount}
                </div>
                <Button variant="secondary" disabled={!canNext} onClick={() => setPage((p) => p + 1)}>
                  Next
                </Button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}


