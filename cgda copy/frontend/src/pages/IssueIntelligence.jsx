import { useContext, useMemo, useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import clsx from "clsx";
import { api } from "../services/api.js";
import { FiltersContext } from "../App.jsx";
import AIBadge from "../components/AIBadge.jsx";

function Card({ title, children, right, ai = false }) {
  return (
    <div
      className={clsx(
        "rounded-xl shadow-card ring-1 p-4 transition-shadow hover:shadow-[0_1px_2px_rgba(15,23,42,0.08),0_12px_36px_rgba(15,23,42,0.10)]",
        ai ? "bg-indigo-50/40 ring-indigo-200" : "bg-white ring-slateink-100"
      )}
    >
      <div className="mb-3 flex items-center justify-between gap-3">
        <div className="text-sm font-semibold text-slateink-800 flex items-center gap-2">{title}</div>
        {right ? <div className="text-xs text-slateink-500">{right}</div> : null}
      </div>
      {children}
    </div>
  );
}

function hashColor(key) {
  const s = String(key || "");
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  const palette = ["#1f3fd0", "#2b54f6", "#4f46e5", "#0ea5e9", "#16a34a", "#f97316", "#a855f7", "#e11d48"];
  return palette[h % palette.length];
}

function pctLabel(p) {
  const v = Number(p || 0) * 100;
  if (!Number.isFinite(v)) return "0%";
  if (v < 0.1) return "<0.1%";
  return `${v.toFixed(1)}%`;
}

function NoData({ text = "No data for selection." }) {
  return <div className="text-sm text-slateink-500">{text}</div>;
}

function HorizontalSubtopicBars({ rows, total, height = 320 }) {
  const data = (rows || []).map((r) => ({
    subTopic: r.subTopic,
    count: r.count,
    pct: r.pct
  }));
  return data.length ? (
    <div style={{ height }}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart
          data={data}
          layout="vertical"
          margin={{ top: 10, right: 20, left: 10, bottom: 0 }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="#eceef2" />
          <XAxis type="number" tick={{ fontSize: 12 }} />
          <YAxis type="category" dataKey="subTopic" width={180} tick={{ fontSize: 12 }} />
          <Tooltip
            formatter={(value, name, props) => {
              if (name === "count") return [value, "Grievances"];
              return [value, name];
            }}
            labelFormatter={(label) => `Sub-Topic: ${label}`}
          />
          <Bar dataKey="count" name="count" radius={[6, 6, 6, 6]}>
            {data.map((d) => (
              <Cell key={d.subTopic} fill={hashColor(d.subTopic)} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  ) : (
    <NoData />
  );
}

export default function IssueIntelligence() {
  const { filters } = useContext(FiltersContext);
  const [dims, setDims] = useState({ wards: [], departments: [], categories: [] });
  const [top, setTop] = useState(null);
  const [unique, setUnique] = useState(null);
  const [ward, setWard] = useState("");
  const [byWard, setByWard] = useState(null);
  const [department, setDepartment] = useState("");
  const [byDepartment, setByDepartment] = useState(null);
  const [subtopic, setSubtopic] = useState("");
  const [trend, setTrend] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const d = await api.dimensions();
        if (!cancelled) setDims(d);
      } catch (e) {
        // ignore dims errors; page can still render if data exists
        if (!cancelled) setDims({ wards: [], departments: [], categories: [] });
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await api.topSubtopics(filters, 10);
        if (cancelled) return;
        // normalize to existing component shape
        setTop({ total: null, rows: res?.rows || [], ai_meta: { ai_provider: "caseA" } });

        // One-of-a-kind complaints (unique sub-topics)
        try {
          const u = await api.oneOfAKind(filters, 25);
          if (!cancelled) setUnique(u);
        } catch {
          if (!cancelled) setUnique(null);
        }

        // default selectors (only if not chosen)
        if (!ward) setWard((dims.wards || [])[0] || "");
        if (!department) setDepartment((dims.departments || [])[0] || "");
        if (!subtopic) setSubtopic((res?.rows || [])[0]?.subTopic || "");
      } catch (e) {
        if (!cancelled) setError(e.message || "Failed to load Issue Intelligence");
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filters.start_date, filters.end_date, (filters.wards || []).join(","), filters.department, filters.category]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        if (!ward) {
          if (!cancelled) setByWard({ ward: "", total: 0, rows: [] });
          return;
        }
        // Use global filters, but override ward for this section.
        const f2 = { ...filters, wards: [ward] };
        const res = await api.topSubtopicsByWard(ward, f2, 5);
        if (!cancelled) setByWard(res);
      } catch (e) {
        if (!cancelled) setByWard(null);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [ward, filters]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        if (!department) {
          if (!cancelled) setByDepartment({ department: "", total: 0, rows: [] });
          return;
        }
        // Use global filters, but override department for this section.
        const f2 = { ...filters, department };
        const res = await api.topSubtopicsByDepartment(department, f2, 10);
        if (!cancelled) setByDepartment(res);
      } catch (e) {
        if (!cancelled) setByDepartment(null);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [department, filters]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        if (!subtopic) {
          if (!cancelled) setTrend({ subTopic: "", total: 0, months: [] });
          return;
        }
        const res = await api.subtopicTrend(subtopic, filters);
        if (!cancelled) setTrend(res);
      } catch (e) {
        if (!cancelled) setTrend(null);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [subtopic, filters]);

  const showAI = true;

  const deptRows = useMemo(() => {
    const rows = byDepartment?.rows || [];
    const total = byDepartment?.total || 0;
    return rows.map((r) => ({
      subTopic: r.subTopic,
      count: r.count,
      pct: total ? r.count / total : 0
    }));
  }, [byDepartment]);

  if (error) {
    return <div className="rounded-xl bg-white p-4 ring-1 ring-slateink-100 text-sm text-rose-700">{error}</div>;
  }

  return (
    <div className="space-y-5">
      {/* SECTION 1 — TOP SUB-TOPICS (OVERALL) */}
      <Card
        ai={showAI}
        title={
          <>
            Top Sub-Topics (Overall) {showAI ? <AIBadge /> : null}
          </>
        }
        right={top?.total != null ? `${top.total} grievances` : null}
      >
        <HorizontalSubtopicBars rows={top?.rows || []} total={top?.total || 0} height={320} />
        {(top?.rows || []).length ? (
          <div className="mt-3 text-xs text-slateink-500">
            Each bar shows count; percentage is relative to total grievances for the current filters.
          </div>
        ) : null}
      </Card>

      {/* NEW — ONE-OF-A-KIND COMPLAINTS */}
      <Card
        ai={showAI}
        title={
          <>
            One-of-a-kind complaints {showAI ? <AIBadge /> : null}
          </>
        }
        right="Unique Sub-Topics"
      >
        {(unique?.rows || []).length ? (
          <>
            <div className="overflow-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-xs uppercase tracking-wide text-slateink-500">
                    <th className="py-2 pr-3">Grievance ID</th>
                    <th className="py-2 pr-3">Date</th>
                    <th className="py-2 pr-3">Sub-Topic</th>
                    <th className="py-2 pr-3">Ward</th>
                    <th className="py-2 pr-3">Subject</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slateink-100">
                  {(unique?.rows || []).slice(0, 25).map((r) => (
                    <tr key={r.grievance_id} className="text-slateink-800">
                      <td className="py-2 pr-3 font-mono text-xs">{r.grievance_id}</td>
                      <td className="py-2 pr-3">{r.created_date}</td>
                      <td className="py-2 pr-3 font-semibold text-slateink-900">{r.ai_subtopic}</td>
                      <td className="py-2 pr-3">{r.ward}</td>
                      <td className="py-2 pr-3">{r.subject}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="mt-3 text-xs text-slateink-500">
              Definition: {unique?.definition || "Sub-Topics with exactly 1 complaint in the selected filters."}
            </div>
          </>
        ) : (
          <NoData text="No one-of-a-kind complaints in this selection (all sub-topics occur multiple times)." />
        )}
      </Card>

      <div className="grid gap-4 lg:grid-cols-2">
        {/* SECTION 2 — TOP SUB-TOPICS BY WARD */}
        <Card
          ai={showAI}
          title={
            <>
              Top Sub-Topics by Ward {showAI ? <AIBadge /> : null}
            </>
          }
          right={
            <div className="flex items-center gap-2">
              <span className="text-xs text-slateink-500">Ward:</span>
              <select
                value={ward}
                onChange={(e) => setWard(e.target.value)}
                className="rounded-lg border border-slateink-200 bg-white px-3 py-1.5 text-xs outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
              >
                {(dims.wards || []).map((w) => (
                  <option key={w} value={w}>
                    {w}
                  </option>
                ))}
              </select>
            </div>
          }
        >
          {byWard?.rows?.length ? (
            <div style={{ height: 260 }}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={byWard.rows} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#eceef2" />
                  <XAxis dataKey="subTopic" tick={{ fontSize: 12 }} interval={0} angle={-18} textAnchor="end" height={60} />
                  <YAxis tick={{ fontSize: 12 }} />
                  <Tooltip
                    formatter={(value, name, props) => {
                      if (name === "count") return [value, "Grievances"];
                      return [value, name];
                    }}
                    labelFormatter={(label) => `Sub-Topic: ${label}`}
                  />
                  <Bar dataKey="count" radius={[6, 6, 0, 0]}>
                    {byWard.rows.map((d) => (
                      <Cell key={d.subTopic} fill={hashColor(d.subTopic)} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <NoData />
          )}
          <div className="mt-3 text-xs text-slateink-500">
            Tooltip shows count and share of the selected ward’s grievances.
          </div>
        </Card>

        {/* SECTION 3 — TOP SUB-TOPICS BY DEPARTMENT */}
        <Card
          ai={showAI}
          title={
            <>
              Top Sub-Topics by Department {showAI ? <AIBadge /> : null}
            </>
          }
          right={
            <div className="flex items-center gap-2">
              <span className="text-xs text-slateink-500">Department:</span>
              <select
                value={department}
                onChange={(e) => setDepartment(e.target.value)}
                className="rounded-lg border border-slateink-200 bg-white px-3 py-1.5 text-xs outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
              >
                {(dims.departments || []).map((d) => (
                  <option key={d} value={d}>
                    {d}
                  </option>
                ))}
              </select>
            </div>
          }
        >
          {deptRows.length ? (
            <>
              <div className="overflow-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-xs uppercase tracking-wide text-slateink-500">
                      <th className="py-2 pr-3">Sub-Topic</th>
                      <th className="py-2 pr-3">Grievances</th>
                      <th className="py-2 pr-3">% of Dept Load</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slateink-100">
                    {deptRows.slice(0, 10).map((r) => (
                      <tr key={r.subTopic} className="text-slateink-800">
                        <td className="py-2 pr-3">
                          <span
                            className="inline-flex items-center gap-2"
                            title="Color is consistent across Issue Intelligence charts"
                          >
                            <span className="h-2.5 w-2.5 rounded-full" style={{ background: hashColor(r.subTopic) }} />
                            {r.subTopic}
                          </span>
                        </td>
                        <td className="py-2 pr-3 font-semibold text-slateink-900">{r.count}</td>
                        <td className="py-2 pr-3">{pctLabel(r.pct)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="mt-3 text-xs text-slateink-500">
                Departments handle multiple issue types; Sub-Topics reveal true workload.
              </div>
            </>
          ) : (
            <NoData />
          )}
        </Card>
      </div>

      {/* SECTION 4 — MONTH-WISE SUB-TOPIC TRENDS */}
      <Card
        ai={showAI}
        title={
          <>
            Month-wise Sub-Topic Trends {showAI ? <AIBadge /> : null}
          </>
        }
        right={
          <div className="flex items-center gap-2">
            <span className="text-xs text-slateink-500">Sub-Topic:</span>
            <select
              value={subtopic}
              onChange={(e) => setSubtopic(e.target.value)}
              className="rounded-lg border border-slateink-200 bg-white px-3 py-1.5 text-xs outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100 max-w-[280px]"
            >
              {(top?.rows || []).map((r) => (
                <option key={r.subTopic} value={r.subTopic}>
                  {r.subTopic}
                </option>
              ))}
            </select>
          </div>
        }
      >
        {trend?.months?.length ? (
          <div style={{ height: 260 }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={trend.months} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#eceef2" />
                <XAxis dataKey="month" tick={{ fontSize: 12 }} />
                <YAxis tick={{ fontSize: 12 }} />
                <Tooltip />
                <Line
                  type="monotone"
                  dataKey="count"
                  stroke={hashColor(subtopic)}
                  strokeWidth={2}
                  dot={false}
                  name="Grievances"
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <NoData />
        )}
      </Card>
    </div>
  );
}


