import { useContext, useMemo, useEffect, useState } from "react";
import { LineCard, VerticalBarCard } from "../components/Charts.jsx";
import { api } from "../services/api.js";
import { FiltersContext } from "../App.jsx";
import AIBadge from "../components/AIBadge.jsx";
import { Card, CardContent, CardHeader } from "../components/ui/Card.jsx";
import Badge from "../components/ui/Badge.jsx";
import { colorForKey } from "../utils/chartColors.js";
import { displaySubtopicLabel } from "../utils/labels.js";

function NoData({ text = "No data for selection." }) {
  return <div className="text-sm text-slateink-500">{text}</div>;
}

function Toggle({ value, onChange, options }) {
  return (
    <div className="inline-flex rounded-full bg-white ring-1 ring-slateink-200 p-1">
      {options.map((o) => {
        const active = value === o.key;
        return (
          <button
            key={o.key}
            type="button"
            onClick={() => onChange(o.key)}
            className={
              "h-8 rounded-full px-3 text-xs font-semibold transition " +
              (active ? "bg-slateink-900 text-white" : "text-slateink-700 hover:bg-slateink-50")
            }
          >
            {o.label}
          </button>
        );
      })}
    </div>
  );
}

function showSubtopic(s) {
  return displaySubtopicLabel(s) || "—";
}

export default function IssueIntelligence() {
  const { filters } = useContext(FiltersContext);
  const [payload, setPayload] = useState(null);
  const [wardFocus, setWardFocus] = useState("");
  const [departmentFocus, setDepartmentFocus] = useState("");
  const [subtopicFocus, setSubtopicFocus] = useState("");
  const [modeOverall, setModeOverall] = useState("volume");
  const [modeWard, setModeWard] = useState("volume");
  // Removed: avg actionable score overlay from the month-wise trend chart (keep counts only).
  const [uniquePriorityOnly, setUniquePriorityOnly] = useState(false);
  const [uniqueHighOnly, setUniqueHighOnly] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await api.issueIntelligenceV2({
          ...filters,
          ward_focus: wardFocus || undefined,
          department_focus: departmentFocus || undefined,
          subtopic_focus: subtopicFocus || undefined,
          unique_min_priority: uniquePriorityOnly ? 70 : 0,
          unique_confidence_high_only: uniqueHighOnly
        });
        if (cancelled) return;
        setPayload(res);

        if (!wardFocus) setWardFocus(res?.focus?.ward || "");
        if (!departmentFocus) setDepartmentFocus(res?.focus?.department || "");
        if (!subtopicFocus) setSubtopicFocus(res?.focus?.subtopic || "");
      } catch (e) {
        if (!cancelled) setError(e.message || "Failed to load Issue Intelligence");
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    filters.start_date,
    filters.end_date,
    (filters.wards || []).join(","),
    filters.department,
    filters.category,
    filters.source,
    wardFocus,
    departmentFocus,
    subtopicFocus,
    uniquePriorityOnly,
    uniqueHighOnly
  ]);

  const showAI = true;

  const modeOptions = [
    { key: "volume", label: "Volume" },
    { key: "priority", label: "Priority" }
  ];

  const topRows = useMemo(() => {
    const rows = payload?.top_subtopics || [];
    const sorted = [...rows].sort((a, b) =>
      modeOverall === "priority"
        ? Number(b.priority_sum || 0) - Number(a.priority_sum || 0)
        : Number(b.count || 0) - Number(a.count || 0)
    );
    return sorted;
  }, [payload, modeOverall]);

  const topRowsDisplay = useMemo(() => {
    return (topRows || []).map((r) => ({
      ...r,
      subTopicDisplay: showSubtopic(r?.subTopic)
    }));
  }, [topRows]);

  const byWardRows = useMemo(() => {
    const rows = payload?.by_ward?.rows || [];
    const sorted = [...rows].sort((a, b) =>
      modeWard === "priority"
        ? Number(b.priority_sum || 0) - Number(a.priority_sum || 0)
        : Number(b.count || 0) - Number(a.count || 0)
    );
    return sorted;
  }, [payload, modeWard]);

  const byWardRowsDisplay = useMemo(() => {
    return (byWardRows || []).map((r) => ({
      ...r,
      subTopicDisplay: showSubtopic(r?.subTopic)
    }));
  }, [byWardRows]);

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
        right={<Toggle value={modeOverall} onChange={setModeOverall} options={modeOptions} />}
        data={topRowsDisplay.slice(0, 10)}
        yKey="subTopicDisplay"
        colorKey="subTopic"
        valueKey={modeOverall === "priority" ? "priority_sum" : "count"}
        height={380}
        total={modeOverall === "priority" ? null : Number(topRows.reduce((acc, r) => acc + Number(r.count || 0), 0))}
      />

      {/* ONE-OF-A-KIND COMPLAINTS */}
      <Card className={showAI ? "border-indigo-200/60 bg-indigo-50/30" : undefined}>
        <CardHeader
          title={
            <span className="inline-flex items-center gap-2">
              One-of-a-kind complaints {showAI ? <AIBadge /> : null}
            </span>
          }
          subtitle="Sub-topics that occur exactly once under the selected filters"
          right={
            <div className="flex items-center gap-2">
              <label className="inline-flex items-center gap-2 text-xs text-slateink-600">
                <input type="checkbox" checked={uniquePriorityOnly} onChange={(e) => setUniquePriorityOnly(e.target.checked)} />
                Priority ≥ 70
              </label>
              <label className="inline-flex items-center gap-2 text-xs text-slateink-600">
                <input type="checkbox" checked={uniqueHighOnly} onChange={(e) => setUniqueHighOnly(e.target.checked)} />
                Confidence = High
              </label>
              <Badge variant="default">Unique Sub-Topics</Badge>
            </div>
          }
        />
        <CardContent>
          {(payload?.one_of_a_kind?.rows || []).length ? (
            <>
              <div className="overflow-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-xs font-semibold tracking-wide text-slateink-500">
                      <th className="py-2 pr-3">Grievance ID</th>
                      <th className="py-2 pr-3">Date</th>
                      <th className="py-2 pr-3">Sub-Topic</th>
                      <th className="py-2 pr-3">Ward</th>
                      <th className="py-2 pr-3">Priority</th>
                      <th className="py-2 pr-3">Urgency</th>
                      <th className="py-2 pr-3">Sentiment</th>
                      <th className="py-2 pr-3">Top entity</th>
                      <th className="py-2 pr-3">Subject</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slateink-100">
                    {(payload?.one_of_a_kind?.rows || []).slice(0, 25).map((r) => (
                      <tr key={r.grievance_id} className="text-slateink-800 hover:bg-white/40">
                        <td className="py-2 pr-3 font-mono text-xs">{r.grievance_id}</td>
                        <td className="py-2 pr-3">{r.created_date}</td>
                        <td className="py-2 pr-3">
                          <span className="inline-flex items-center gap-2 font-semibold text-slateink-900">
                            <span className="h-2.5 w-2.5 rounded-full" style={{ background: colorForKey(r.subTopic) }} />
                              {showSubtopic(r.subTopic)}
                          </span>
                        </td>
                        <td className="py-2 pr-3">{r.ward}</td>
                        <td className="py-2 pr-3 font-semibold text-slateink-900">{r.actionable_score}</td>
                        <td className="py-2 pr-3">{r.urgency || "—"}</td>
                        <td className="py-2 pr-3">{r.sentiment || "—"}</td>
                        <td className="py-2 pr-3">{r.top_entity || "—"}</td>
                        <td className="py-2 pr-3">{r.subject}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="mt-3 text-xs text-slateink-500">
                Definition: {payload?.one_of_a_kind?.definition || "Sub-Topics with exactly 1 complaint in the selected filters."}
              </div>
            </>
          ) : (
            <NoData text="No one-of-a-kind complaints in this selection (all sub-topics occur multiple times)." />
          )}
        </CardContent>
      </Card>

      <div className="grid gap-4 lg:grid-cols-2">
        {/* SECTION 2 — TOP SUB-TOPICS BY WARD */}
        <div className="space-y-4">
          <VerticalBarCard
            title={
              <span className="inline-flex items-center gap-2">
                Top Sub-Topics by Ward {showAI ? <AIBadge /> : null}
              </span>
            }
            subtitle="Top issues within the selected ward"
            ai={showAI}
            right={
              <div className="flex flex-wrap items-center gap-2">
                <span className="text-xs text-slateink-500">Ward</span>
                <select
                  value={wardFocus}
                  onChange={(e) => setWardFocus(e.target.value)}
                  className="h-9 rounded-xl border border-slateink-200 bg-white px-3 text-xs font-semibold outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
                >
                  {(payload?.options?.wards || []).map((w) => (
                    <option key={w} value={w}>
                      {w}
                    </option>
                  ))}
                </select>
                <Toggle value={modeWard} onChange={setModeWard} options={modeOptions} />
              </div>
            }
            data={byWardRowsDisplay || []}
            yKey="subTopicDisplay"
            colorKey="subTopic"
            valueKey={modeWard === "priority" ? "priority_sum" : "count"}
            height={340}
            total={modeWard === "priority" ? null : Number((byWardRows || []).reduce((acc, r) => acc + Number(r.count || 0), 0))}
          />

          <Card>
            <CardHeader title="Top entities in ward" subtitle="Most frequent extracted entities (ai_entities_json)" />
            <CardContent>
              {(payload?.ward_entities || []).length ? (
                <div className="grid grid-cols-1 gap-2">
                  {(payload?.ward_entities || []).slice(0, 10).map((e) => (
                    <div
                      key={e.entity}
                      className="flex items-center justify-between rounded-xl bg-white ring-1 ring-slateink-100 px-3 py-2"
                    >
                      <div className="text-sm text-slateink-800">{e.entity}</div>
                      <div className="text-sm font-semibold text-slateink-900">{e.count}</div>
                    </div>
                  ))}
                </div>
              ) : (
                <NoData
                  text={
                    (payload?.ward_entities_coverage?.total || 0) > 0
                      ? `Entities not available yet for this ward (coverage ${payload?.ward_entities_coverage?.known || 0}/${
                          payload?.ward_entities_coverage?.total || 0
                        }).`
                      : "No data for this ward selection."
                  }
                />
              )}
              {(payload?.ward_entities_coverage?.total || 0) > 0 ? (
                <div className="mt-3 text-xs text-slateink-500">
                  Entity coverage: <span className="font-semibold">{payload?.ward_entities_coverage?.pct || 0}%</span> (
                  {payload?.ward_entities_coverage?.known || 0}/{payload?.ward_entities_coverage?.total || 0})
                </div>
              ) : null}
            </CardContent>
          </Card>
        </div>

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
                  value={departmentFocus}
                  onChange={(e) => setDepartmentFocus(e.target.value)}
                  className="h-9 rounded-xl border border-slateink-200 bg-white px-3 text-xs font-semibold outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100 max-w-[320px]"
                >
                  {(payload?.options?.departments || []).map((d) => (
                    <option key={d} value={d}>
                      {d}
                    </option>
                  ))}
                </select>
              </div>
            }
          />
          <CardContent>
            {(payload?.by_department?.rows || []).length ? (
              <>
                <div className="overflow-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-left text-xs font-semibold tracking-wide text-slateink-500">
                        <th className="py-2 pr-3">Sub-Topic</th>
                        <th className="py-2 pr-3">Grievances</th>
                        <th className="py-2 pr-3">Priority</th>
                        <th className="py-2 pr-3">Median SLA</th>
                        <th className="py-2 pr-3">%&gt;30d</th>
                        <th className="py-2 pr-3">Avg rating</th>
                        <th className="py-2 pr-3">Low rating %</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slateink-100">
                      {(payload?.by_department?.rows || []).slice(0, 10).map((r) => (
                        <tr key={r.subTopic} className="text-slateink-800 hover:bg-white/40">
                          <td className="py-2 pr-3">
                            <span className="inline-flex items-center gap-2" title="Color is consistent across Issue Intelligence charts">
                              <span className="h-2.5 w-2.5 rounded-full" style={{ background: colorForKey(r.subTopic) }} />
                              {showSubtopic(r.subTopic)}
                            </span>
                          </td>
                          <td className="py-2 pr-3 font-semibold text-slateink-900">{r.count}</td>
                          <td className="py-2 pr-3">{r.priority_sum}</td>
                          <td className="py-2 pr-3">{r.median_resolution_days ?? "—"}</td>
                          <td className="py-2 pr-3">{r.pct_over_30d != null ? `${r.pct_over_30d}%` : "—"}</td>
                          <td className="py-2 pr-3">{r.avg_rating != null ? `${r.avg_rating} / 5` : "—"}</td>
                          <td className="py-2 pr-3">{r.low_rating_pct != null ? `${r.low_rating_pct}%` : "—"}</td>
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
        subtitle="Counts grouped by month (Created Date)."
        ai={showAI}
        right={
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-xs text-slateink-500">Sub-Topic</span>
            <select
              value={subtopicFocus}
              onChange={(e) => setSubtopicFocus(e.target.value)}
              className="h-9 rounded-xl border border-slateink-200 bg-white px-3 text-xs font-semibold outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100 max-w-[320px]"
            >
              {(payload?.options?.subtopics || []).map((s) => (
                <option key={s} value={s}>
                  {showSubtopic(s)}
                </option>
              ))}
            </select>
          </div>
        }
        data={payload?.trend?.months || []}
        xKey="month"
        lines={[
          { key: "count", name: "Grievances", color: colorForKey(subtopicFocus) }
        ]}
        height={340}
      />
    </div>
  );
}


