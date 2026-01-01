import { useContext, useMemo, useEffect, useState } from "react";
import { LineCard, VerticalBarCard } from "../components/Charts.jsx";
import { api } from "../services/api.js";
import { FiltersContext } from "../App.jsx";
import AIBadge from "../components/AIBadge.jsx";
import { Card, CardContent, CardHeader } from "../components/ui/Card.jsx";
import Badge from "../components/ui/Badge.jsx";
import { colorForKey } from "../utils/chartColors.js";

function pctLabel(p) {
  const v = Number(p || 0) * 100;
  if (!Number.isFinite(v)) return "0%";
  if (v < 0.1) return "<0.1%";
  return `${v.toFixed(1)}%`;
}

function NoData({ text = "No data for selection." }) {
  return <div className="text-sm text-slateink-500">{text}</div>;
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
      <VerticalBarCard
        title={
          <span className="inline-flex items-center gap-2">
            Top Sub-Topics (Overall) {showAI ? <AIBadge /> : null}
          </span>
        }
        subtitle="Top 10 standardized issues for the selected filters"
        ai={showAI}
        data={(top?.rows || []).slice(0, 10)}
        yKey="subTopic"
        valueKey="count"
        height={380}
        total={Number((top?.rows || []).reduce((acc, r) => acc + Number(r.count || 0), 0))}
      />

      {/* NEW — ONE-OF-A-KIND COMPLAINTS */}
      <Card className={showAI ? "border-indigo-200/60 bg-indigo-50/30" : undefined}>
        <CardHeader
          title={
            <span className="inline-flex items-center gap-2">
              One-of-a-kind complaints {showAI ? <AIBadge /> : null}
            </span>
          }
          subtitle="Sub-topics that occur exactly once under the selected filters"
          right={<Badge variant="default">Unique Sub-Topics</Badge>}
        />
        <CardContent>
          {(unique?.rows || []).length ? (
            <>
              <div className="overflow-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-xs font-semibold tracking-wide text-slateink-500">
                      <th className="py-2 pr-3">Grievance ID</th>
                      <th className="py-2 pr-3">Date</th>
                      <th className="py-2 pr-3">Sub-Topic</th>
                      <th className="py-2 pr-3">Ward</th>
                      <th className="py-2 pr-3">Subject</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slateink-100">
                    {(unique?.rows || []).slice(0, 25).map((r) => (
                      <tr key={r.grievance_id} className="text-slateink-800 hover:bg-white/40">
                        <td className="py-2 pr-3 font-mono text-xs">{r.grievance_id}</td>
                        <td className="py-2 pr-3">{r.created_date}</td>
                        <td className="py-2 pr-3">
                          <span className="inline-flex items-center gap-2 font-semibold text-slateink-900">
                            <span className="h-2.5 w-2.5 rounded-full" style={{ background: colorForKey(r.ai_subtopic) }} />
                            {r.ai_subtopic}
                          </span>
                        </td>
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
        </CardContent>
      </Card>

      <div className="grid gap-4 lg:grid-cols-2">
        {/* SECTION 2 — TOP SUB-TOPICS BY WARD */}
        <VerticalBarCard
          title={
            <span className="inline-flex items-center gap-2">
              Top Sub-Topics by Ward {showAI ? <AIBadge /> : null}
            </span>
          }
          subtitle="Top 5 issues within the selected ward"
          ai={showAI}
          right={
            <div className="flex items-center gap-2">
              <span className="text-xs text-slateink-500">Ward</span>
              <select
                value={ward}
                onChange={(e) => setWard(e.target.value)}
                className="h-9 rounded-xl border border-slateink-200 bg-white px-3 text-xs font-semibold outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
              >
                {(dims.wards || []).map((w) => (
                  <option key={w} value={w}>
                    {w}
                  </option>
                ))}
              </select>
            </div>
          }
          data={byWard?.rows || []}
          yKey="subTopic"
          valueKey="count"
          height={340}
          total={Number(byWard?.total || 0)}
        />

        {/* SECTION 3 — TOP SUB-TOPICS BY DEPARTMENT */}
        <Card className={showAI ? "border-indigo-200/60 bg-indigo-50/30" : undefined}>
          <CardHeader
            title={
              <span className="inline-flex items-center gap-2">
                Top Sub-Topics by Department {showAI ? <AIBadge /> : null}
              </span>
            }
            subtitle="Departments handle multiple issue types; sub-topics reveal true workload."
            right={
              <div className="flex items-center gap-2">
                <span className="text-xs text-slateink-500">Department</span>
                <select
                  value={department}
                  onChange={(e) => setDepartment(e.target.value)}
                  className="h-9 rounded-xl border border-slateink-200 bg-white px-3 text-xs font-semibold outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100 max-w-[320px]"
                >
                  {(dims.departments || []).map((d) => (
                    <option key={d} value={d}>
                      {d}
                    </option>
                  ))}
                </select>
              </div>
            }
          />
          <CardContent>
          {deptRows.length ? (
            <>
              <div className="overflow-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-xs font-semibold tracking-wide text-slateink-500">
                      <th className="py-2 pr-3">Sub-Topic</th>
                      <th className="py-2 pr-3">Grievances</th>
                      <th className="py-2 pr-3">% of Dept Load</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slateink-100">
                    {deptRows.slice(0, 10).map((r) => (
                      <tr key={r.subTopic} className="text-slateink-800 hover:bg-white/40">
                        <td className="py-2 pr-3">
                          <span
                            className="inline-flex items-center gap-2"
                            title="Color is consistent across Issue Intelligence charts"
                          >
                            <span className="h-2.5 w-2.5 rounded-full" style={{ background: colorForKey(r.subTopic) }} />
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
            </>
          ) : (
            <NoData />
          )}
          </CardContent>
        </Card>
      </div>

      {/* SECTION 4 — MONTH-WISE SUB-TOPIC TRENDS */}
      <LineCard
        title={
          <span className="inline-flex items-center gap-2">
            Month-wise Sub-Topic Trends {showAI ? <AIBadge /> : null}
          </span>
        }
        subtitle="Counts grouped by month (Created Date)"
        ai={showAI}
        right={
          <div className="flex items-center gap-2">
            <span className="text-xs text-slateink-500">Sub-Topic</span>
            <select
              value={subtopic}
              onChange={(e) => setSubtopic(e.target.value)}
              className="h-9 rounded-xl border border-slateink-200 bg-white px-3 text-xs font-semibold outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100 max-w-[320px]"
            >
              {(top?.rows || []).map((r) => (
                <option key={r.subTopic} value={r.subTopic}>
                  {r.subTopic}
                </option>
              ))}
            </select>
          </div>
        }
        data={trend?.months || []}
        xKey="month"
        lines={[{ key: "count", name: "Grievances", color: colorForKey(subtopic) }]}
        height={340}
      />
    </div>
  );
}


