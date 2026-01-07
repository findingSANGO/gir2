import { useEffect, useMemo, useState } from "react";
import { useLocation } from "react-router-dom";
import { api } from "../services/api.js";
import { Card } from "./ui/Card.jsx";
import Button from "./ui/Button.jsx";
import Badge from "./ui/Badge.jsx";

// Configurable constant: Financial Year starts on 01 Apr by default.
const FY_START_MONTH = 4; // 1-12
const FY_START_DAY = 1; // 1-31

function Select({ value, onChange, options, placeholder }) {
  return (
    <select
      value={value || ""}
      onChange={(e) => onChange(e.target.value || null)}
      className="h-9 rounded-xl border border-slateink-200 bg-white px-2 text-xs outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
    >
      <option value="">{placeholder}</option>
      {options.map((o) => (
        <option key={o} value={o}>
          {o}
        </option>
      ))}
    </select>
  );
}

function MultiSelect({ value, onChange, options, placeholder }) {
  const [open, setOpen] = useState(false);
  const selected = value || [];
  const label = selected.length ? `${selected.length} selected` : placeholder;

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setOpen((s) => !s)}
        className="h-9 rounded-xl border border-slateink-200 bg-white px-2 text-xs text-left min-w-44 hover:border-slateink-300"
      >
        {label}
      </button>
      {open ? (
        <div className="absolute z-20 mt-2 w-72 max-h-72 overflow-auto rounded-2xl bg-white shadow-card ring-1 ring-slateink-100 p-2">
          <div className="flex items-center justify-between px-2 py-1">
            <div className="text-xs font-semibold text-slateink-600">Wards</div>
            <button
              type="button"
              onClick={() => onChange([])}
              className="text-xs font-semibold text-gov-700 hover:underline"
            >
              Clear
            </button>
          </div>
          <div className="mt-1 space-y-1">
            {options.map((o) => {
              const checked = selected.includes(o);
              return (
                <label key={o} className="flex items-center gap-2 px-2 py-1 rounded-lg hover:bg-slateink-50">
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={(e) => {
                      const next = e.target.checked ? [...selected, o] : selected.filter((x) => x !== o);
                      onChange(next);
                    }}
                  />
                  <span className="text-sm text-slateink-800">{o}</span>
                </label>
              );
            })}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function startOfMonthISO() {
  const d = new Date();
  const m = new Date(d.getFullYear(), d.getMonth(), 1);
  return m.toISOString().slice(0, 10);
}
function startOfWeekISO() {
  const d = new Date();
  const day = d.getDay(); // 0 Sun
  const diff = (day === 0 ? 6 : day - 1); // Monday start
  d.setDate(d.getDate() - diff);
  return d.toISOString().slice(0, 10);
}
function daysAgoISO(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString().slice(0, 10);
}
function todayISO() {
  return new Date().toISOString().slice(0, 10);
}

function parseISODate(iso) {
  if (!iso) return null;
  const d = new Date(`${iso}T00:00:00`);
  return Number.isNaN(d.getTime()) ? null : d;
}

function startOfWeekFromISO(anchorISO) {
  const d = parseISODate(anchorISO) || new Date();
  const day = d.getDay(); // 0 Sun
  const diff = (day === 0 ? 6 : day - 1); // Monday start
  const m = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  m.setDate(m.getDate() - diff);
  return m.toISOString().slice(0, 10);
}
function startOfMonthFromISO(anchorISO) {
  const d = parseISODate(anchorISO) || new Date();
  const m = new Date(d.getFullYear(), d.getMonth(), 1);
  return m.toISOString().slice(0, 10);
}
function startOfQuarterFromISO(anchorISO) {
  const d = parseISODate(anchorISO) || new Date();
  const qStartMonth = Math.floor(d.getMonth() / 3) * 3; // 0,3,6,9
  const m = new Date(d.getFullYear(), qStartMonth, 1);
  return m.toISOString().slice(0, 10);
}
function startOfYearFromISO(anchorISO) {
  const d = parseISODate(anchorISO) || new Date();
  const y = new Date(d.getFullYear(), 0, 1);
  return y.toISOString().slice(0, 10);
}

function formatPretty(iso) {
  const d = parseISODate(iso);
  if (!d) return "—";
  return d.toLocaleDateString(undefined, { day: "2-digit", month: "short", year: "numeric" });
}

function addDaysISO(iso, deltaDays) {
  const d = parseISODate(iso);
  if (!d) return iso;
  d.setDate(d.getDate() + deltaDays);
  return d.toISOString().slice(0, 10);
}

function startOfFYISO(anchorISO) {
  const d = parseISODate(anchorISO) || new Date();
  const y = d.getFullYear();
  const m = d.getMonth() + 1;
  const fyYear = m >= FY_START_MONTH ? y : y - 1;
  const fy = new Date(fyYear, FY_START_MONTH - 1, FY_START_DAY);
  return fy.toISOString().slice(0, 10);
}

export default function Filters({ filters, setFilters, onApply, showDataset = true }) {
  const { pathname } = useLocation();
  const isExecutive = pathname === "/" || pathname === "/new" || pathname === "/old";

  const [dims, setDims] = useState({ wards: [], departments: [], categories: [], datasets: [] });
  const [preset, setPreset] = useState(isExecutive ? "yesterday" : "last30");
  const [customOpen, setCustomOpen] = useState(false);
  const [customStart, setCustomStart] = useState("");
  const [customEnd, setCustomEnd] = useState("");
  const [datasetsMeta, setDatasetsMeta] = useState([]);

  const presets = useMemo(
    () => [
      { key: "yesterday", label: "Yesterday" },
      { key: "today", label: "Today", start: todayISO(), end: todayISO() },
      { key: "last7", label: "Last 7 days", start: daysAgoISO(6), end: todayISO() },
      { key: "last14", label: "Last 14 days" },
      { key: "last30", label: "Last 30 days", start: daysAgoISO(29), end: todayISO() },
      { key: "tillDate", label: "Till Date" },
      { key: "thisWeek", label: "This Week", start: startOfWeekISO(), end: todayISO() },
      { key: "thisMonth", label: "This Month", start: startOfMonthISO(), end: todayISO() },
      { key: "custom", label: "Custom Range", start: null, end: null }
    ],
    []
  );

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const d = await api.dimensions();
        if (!cancelled) setDims(d);
      } catch {
        // ignore
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // Executive-only: we need dataset max date to resolve "Till Date" and also anchor the other presets.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      if (!isExecutive) return;
      try {
        const res = await api.datasetsProcessed();
        if (!cancelled) setDatasetsMeta(res?.datasets || []);
      } catch {
        // ignore
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [isExecutive]);

  const datasetMaxDate = useMemo(() => {
    const src = filters?.source;
    if (!src) return null;
    const d = (datasetsMeta || []).find((x) => x.source === src);
    return d?.max_created_date || null;
  }, [datasetsMeta, filters?.source]);

  const datasetMinDate = useMemo(() => {
    const src = filters?.source;
    if (!src) return null;
    const d = (datasetsMeta || []).find((x) => x.source === src);
    return d?.min_created_date || null;
  }, [datasetsMeta, filters?.source]);

  const datasetSummary = useMemo(() => {
    const src = filters?.source;
    if (!src) return null;
    const d = (datasetsMeta || []).find((x) => x.source === src);
    if (!d) return null;
    const total = d.count != null ? Number(d.count) : null;
    const ai = d.ai_subtopic_rows != null ? Number(d.ai_subtopic_rows) : null;
    const closeRows = d.closed_rows != null ? Number(d.closed_rows) : null;
    const fbRows = d.feedback_rows != null ? Number(d.feedback_rows) : null;
    return { total, ai, closeRows, fbRows, min: d.min_created_date || null, max: d.max_created_date || null };
  }, [datasetsMeta, filters?.source]);

  const anchorEnd = useMemo(() => {
    // Prefer dataset max date (loaded snapshot), else fall back to yesterday.
    const y = daysAgoISO(1);
    if (datasetMaxDate) return datasetMaxDate;
    return y;
  }, [datasetMaxDate]);

  // Default for Executive Overview: Yesterday (anchored to dataset max date).
  useEffect(() => {
    if (!isExecutive) return;
    // If the app is still on the generic 30-day default, snap to Yesterday for the Executive UX.
    const looksDefault = filters?.start_date === daysAgoISO(29) && filters?.end_date === todayISO();
    if (!looksDefault) return;
    const y = anchorEnd;
    setPreset("yesterday");
    setFilters((s) => ({ ...s, start_date: y, end_date: y }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isExecutive, datasetMaxDate]);

  const resolvedRangeLabel = useMemo(() => {
    if (!filters?.start_date || !filters?.end_date) return "Select a date range";
    if (isExecutive && preset === "all") {
      return `All (${formatPretty(filters.start_date)} → ${formatPretty(filters.end_date)})`;
    }
    if (isExecutive && preset === "tillDate") {
      return `Till Date (${formatPretty(filters.start_date)} → ${formatPretty(filters.end_date)})`;
    }
    return `${formatPretty(filters.start_date)} → ${formatPretty(filters.end_date)}`;
  }, [filters?.start_date, filters?.end_date, isExecutive, preset]);

  return (
    <div className="sticky top-[64px] z-10 bg-white border-b border-slateink-100">
      <div className="mx-auto max-w-7xl px-4 lg:px-6 py-2">
        <Card className="px-3 py-2 rounded-2xl">
          <div className="flex flex-wrap items-end gap-2">
            <div className="flex items-center gap-2">
              <div className="text-xs font-semibold text-slateink-600">Filters</div>
              <Badge variant="default" className="px-2 py-1 text-[11px]">
                {resolvedRangeLabel}
              </Badge>
            </div>

            {isExecutive ? (
              <div className="min-w-[280px]">
                <div className="text-xs font-semibold text-slateink-600">Range</div>
                <div className="mt-1 flex flex-wrap items-center gap-2">
                  {[
                    { key: "all", label: "All" },
                    { key: "month", label: "Month" },
                    { key: "week", label: "Week" },
                    { key: "yesterday", label: "Yesterday" }
                  ].map((c) => {
                    const active = preset === c.key;
                    return (
                      <button
                        key={c.key}
                        type="button"
                        onClick={() => {
                          setPreset(c.key);
                          let start = anchorEnd;
                          if (c.key === "all") start = datasetMinDate || startOfFYISO(anchorEnd);
                          if (c.key === "week") start = startOfWeekFromISO(anchorEnd);
                          if (c.key === "month") start = startOfMonthFromISO(anchorEnd);
                          if (c.key === "qtr") start = startOfQuarterFromISO(anchorEnd);
                          if (c.key === "year") start = startOfYearFromISO(anchorEnd);
                          if (c.key === "yesterday") start = anchorEnd;

                          const next = { ...filters, start_date: start, end_date: anchorEnd };
                          setFilters(next);
                          // Executive UX: range buttons apply immediately (no GO)
                          if (onApply) onApply(next);
                        }}
                        className={
                          "h-8 rounded-full px-2 text-xs font-semibold ring-1 transition " +
                          (active
                            ? "bg-slateink-900 text-white ring-slateink-900"
                            : "bg-white text-slateink-700 ring-slateink-200 hover:ring-slateink-300")
                        }
                      >
                        {c.label}
                      </button>
                    );
                  })}

                  <button
                    type="button"
                    onClick={() => {
                      setPreset("custom");
                      setCustomStart(filters?.start_date || "");
                      setCustomEnd(filters?.end_date || "");
                      setCustomOpen(true);
                    }}
                    className={
                      "h-8 rounded-full px-2 text-xs font-semibold ring-1 transition " +
                      (preset === "custom"
                        ? "bg-slateink-900 text-white ring-slateink-900"
                        : "bg-white text-slateink-700 ring-slateink-200 hover:ring-slateink-300")
                    }
                  >
                    Custom Range
                  </button>
                </div>
              </div>
            ) : (
              <>
                <div>
                  <div className="text-xs font-semibold text-slateink-600">Date range</div>
                  <select
                    value={preset}
                    onChange={(e) => {
                      const key = e.target.value;
                      setPreset(key);
                      const p = presets.find((x) => x.key === key);
                      if (p && p.key !== "custom") {
                        setFilters((s) => ({ ...s, start_date: p.start, end_date: p.end }));
                      }
                    }}
                    className="mt-1 h-9 rounded-xl border border-slateink-200 bg-white px-2 text-xs outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
                  >
                    {presets.map((p) => (
                      <option key={p.key} value={p.key}>
                        {p.label}
                      </option>
                    ))}
                  </select>
                </div>

                {preset === "custom" ? (
                  <>
                    <div>
                      <div className="text-xs font-semibold text-slateink-600">Start</div>
                      <input
                        type="date"
                        value={filters.start_date || ""}
                        onChange={(e) => setFilters((s) => ({ ...s, start_date: e.target.value || null }))}
                        className="mt-1 h-9 rounded-xl border border-slateink-200 bg-white px-2 text-xs outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
                      />
                    </div>

                    <div>
                      <div className="text-xs font-semibold text-slateink-600">End</div>
                      <input
                        type="date"
                        value={filters.end_date || ""}
                        onChange={(e) => setFilters((s) => ({ ...s, end_date: e.target.value || null }))}
                        className="mt-1 h-9 rounded-xl border border-slateink-200 bg-white px-2 text-xs outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
                      />
                    </div>
                  </>
                ) : null}
              </>
            )}

            <div>
              <div className="text-xs font-semibold text-slateink-600">Ward</div>
              <div className="mt-1">
                <MultiSelect
                  value={filters.wards || []}
                  onChange={(wards) => {
                    const next = { ...filters, wards };
                    setFilters(next);
                    if (isExecutive && onApply) onApply(next);
                  }}
                  options={dims.wards || []}
                  placeholder="All wards"
                />
              </div>
            </div>

            {showDataset ? (
              <div>
                <div className="text-xs font-semibold text-slateink-600">Dataset</div>
                <div className="mt-1">
                  <Select
                    value={filters.source}
                    onChange={(source) => {
                      const next = { ...filters, source };
                      setFilters(next);
                      if (isExecutive && onApply) onApply(next);
                    }}
                    options={dims.datasets || []}
                    placeholder="All datasets"
                  />
                </div>
                {datasetSummary ? (
                  <div className="mt-1 text-[11px] text-slateink-500">
                    <span className="font-semibold text-slateink-700">Rows:</span>{" "}
                    {datasetSummary.total != null ? datasetSummary.total.toLocaleString() : "—"}
                    {"  "}•{" "}
                    <span className="font-semibold text-slateink-700">AI:</span>{" "}
                    {datasetSummary.ai != null ? datasetSummary.ai.toLocaleString() : "—"}
                    {datasetSummary.closeRows != null ? (
                      <>
                        {"  "}•{" "}
                        <span className="font-semibold text-slateink-700">Close:</span>{" "}
                        {datasetSummary.closeRows.toLocaleString()}
                      </>
                    ) : null}
                    {datasetSummary.fbRows != null ? (
                      <>
                        {"  "}•{" "}
                        <span className="font-semibold text-slateink-700">Feedback:</span>{" "}
                        {datasetSummary.fbRows.toLocaleString()}
                      </>
                    ) : null}
                  </div>
                ) : null}
              </div>
            ) : null}

            <div>
              <div className="text-xs font-semibold text-slateink-600">Department</div>
              <div className="mt-1">
                <Select
                  value={filters.department}
                  onChange={(department) => {
                    const next = { ...filters, department };
                    setFilters(next);
                    if (isExecutive && onApply) onApply(next);
                  }}
                  options={dims.departments || []}
                  placeholder="All departments"
                />
              </div>
            </div>

            <div>
              <div className="text-xs font-semibold text-slateink-600">Category</div>
              <div className="mt-1">
                <Select
                  value={filters.category}
                  onChange={(category) => {
                    const next = { ...filters, category };
                    setFilters(next);
                    if (isExecutive && onApply) onApply(next);
                  }}
                  options={dims.categories || []}
                  placeholder="All categories"
                />
              </div>
            </div>

            <div className="ml-auto flex items-center gap-2">
              {!isExecutive ? (
                <Button onClick={() => onApply && onApply()} variant="primary" title="Apply filters (single fetch)">
                  GO
                </Button>
              ) : null}
              <Button
                variant="secondary"
                onClick={() => {
                  const next = {
                    start_date: isExecutive ? anchorEnd : daysAgoISO(29),
                    end_date: isExecutive ? anchorEnd : todayISO(),
                    // Keep dataset pinned; "Reset" should reset filters, not unload the dataset.
                    source: filters?.source || null,
                    wards: [],
                    department: null,
                    category: null,
                    ai_category: null
                  };
                  setFilters(next);
                  if (isExecutive) setPreset("yesterday");
                  if (isExecutive && onApply) onApply(next);
                }}
              >
                Reset
              </Button>
            </div>
          </div>
        </Card>
      </div>

      {isExecutive && customOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slateink-900/40 p-4">
          <div className="w-full max-w-md rounded-2xl bg-white shadow-card ring-1 ring-slateink-100 p-5">
            <div className="text-base font-semibold text-slateink-900">Custom Range</div>
            <div className="mt-1 text-sm text-slateink-500">Choose start and end date, then Apply.</div>

            <div className="mt-4 grid grid-cols-2 gap-3">
              <div>
                <div className="text-xs font-semibold text-slateink-600">Start Date</div>
                <input
                  type="date"
                  value={customStart}
                  onChange={(e) => setCustomStart(e.target.value)}
                  className="mt-1 h-10 w-full rounded-xl border border-slateink-200 bg-white px-3 text-sm outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
                />
              </div>
              <div>
                <div className="text-xs font-semibold text-slateink-600">End Date</div>
                <input
                  type="date"
                  value={customEnd}
                  onChange={(e) => setCustomEnd(e.target.value)}
                  className="mt-1 h-10 w-full rounded-xl border border-slateink-200 bg-white px-3 text-sm outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
                />
              </div>
            </div>

            <div className="mt-4 flex items-center justify-end gap-2">
              <button
                type="button"
                onClick={() => {
                  setCustomOpen(false);
                  // keep preset selection, but don't change dates unless Apply is clicked
                }}
                className="rounded-lg bg-white px-3 py-2 text-sm font-semibold text-slateink-800 ring-1 ring-slateink-200 hover:ring-slateink-300"
              >
                Cancel
              </button>
              <button
                type="button"
                disabled={!customStart || !customEnd}
                onClick={() => {
                  const next = { ...filters, start_date: customStart, end_date: customEnd };
                  setFilters(next);
                  if (onApply) onApply(next); // Executive UX: apply immediately
                  setPreset("custom");
                  setCustomOpen(false);
                }}
                className="rounded-lg bg-slateink-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-50 hover:bg-slateink-800"
              >
                Apply
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}


