import { useEffect, useMemo, useState } from "react";
import { api } from "../services/api.js";
import { Card } from "./ui/Card.jsx";
import Button from "./ui/Button.jsx";
import Badge from "./ui/Badge.jsx";

function Select({ value, onChange, options, placeholder }) {
  return (
    <select
      value={value || ""}
      onChange={(e) => onChange(e.target.value || null)}
      className="h-10 rounded-xl border border-slateink-200 bg-white px-3 text-sm outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
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
        className="h-10 rounded-xl border border-slateink-200 bg-white px-3 text-sm text-left min-w-44 hover:border-slateink-300"
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

export default function Filters({ filters, setFilters, onApply }) {
  const [dims, setDims] = useState({ wards: [], departments: [], categories: [] });
  const [preset, setPreset] = useState("last30");

  const presets = useMemo(
    () => [
      { key: "today", label: "Today", start: todayISO(), end: todayISO() },
      { key: "last7", label: "Last 7 days", start: daysAgoISO(6), end: todayISO() },
      { key: "last30", label: "Last 30 days", start: daysAgoISO(29), end: todayISO() },
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

  const rangeChip =
    filters?.start_date && filters?.end_date ? `${filters.start_date} → ${filters.end_date}` : "Select a date range";

  return (
    <div className="sticky top-[64px] z-10 bg-slateink-50/80 backdrop-blur border-b border-slateink-100">
      <div className="mx-auto max-w-7xl px-4 lg:px-6 py-3">
        <Card className="px-4 py-3 rounded-2xl">
          <div className="flex flex-wrap items-end gap-3">
            <div className="flex items-center gap-2">
              <div className="text-xs font-semibold text-slateink-600">Filters</div>
              <Badge variant="default" className="px-2 py-1 text-[11px]">
                {rangeChip}
              </Badge>
            </div>

            <div className="w-full basis-full" />

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
                className="mt-1 h-10 rounded-xl border border-slateink-200 bg-white px-3 text-sm outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
              >
                {presets.map((p) => (
                  <option key={p.key} value={p.key}>
                    {p.label}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <div className="text-xs font-semibold text-slateink-600">Start</div>
              <input
                type="date"
                value={filters.start_date || ""}
                onChange={(e) => setFilters((s) => ({ ...s, start_date: e.target.value || null }))}
                disabled={preset !== "custom"}
                className="mt-1 h-10 rounded-xl border border-slateink-200 bg-white px-3 text-sm outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100 disabled:bg-slateink-50"
              />
            </div>

            <div>
              <div className="text-xs font-semibold text-slateink-600">End</div>
              <input
                type="date"
                value={filters.end_date || ""}
                onChange={(e) => setFilters((s) => ({ ...s, end_date: e.target.value || null }))}
                disabled={preset !== "custom"}
                className="mt-1 h-10 rounded-xl border border-slateink-200 bg-white px-3 text-sm outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100 disabled:bg-slateink-50"
              />
            </div>

            <div>
              <div className="text-xs font-semibold text-slateink-600">Ward</div>
              <div className="mt-1">
                <MultiSelect
                  value={filters.wards || []}
                  onChange={(wards) => setFilters((s) => ({ ...s, wards }))}
                  options={dims.wards || []}
                  placeholder="All wards"
                />
              </div>
            </div>

            <div>
              <div className="text-xs font-semibold text-slateink-600">Department</div>
              <div className="mt-1">
                <Select
                  value={filters.department}
                  onChange={(department) => setFilters((s) => ({ ...s, department }))}
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
                  onChange={(category) => setFilters((s) => ({ ...s, category }))}
                  options={dims.categories || []}
                  placeholder="All categories"
                />
              </div>
            </div>

            <div className="ml-auto flex items-center gap-2">
              <Button
                onClick={() => onApply && onApply()}
                variant="primary"
                title="Apply filters (single fetch)"
              >
                GO
              </Button>
              <Button
                variant="secondary"
                onClick={() =>
                  setFilters({
                    start_date: daysAgoISO(29),
                    end_date: todayISO(),
                    wards: [],
                    department: null,
                    category: null,
                    ai_category: null
                  })
                }
              >
                Reset
              </Button>
            </div>
          </div>
        </Card>
      </div>
    </div>
  );
}


