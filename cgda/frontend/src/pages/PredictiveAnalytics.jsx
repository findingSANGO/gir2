import { useContext, useEffect, useMemo, useState } from "react";
import { api } from "../services/api.js";
import { FiltersContext } from "../App.jsx";
import AIBadge from "../components/AIBadge.jsx";
import { Card, CardContent, CardHeader } from "../components/ui/Card.jsx";
import Badge from "../components/ui/Badge.jsx";
import Button from "../components/ui/Button.jsx";
import Skeleton from "../components/ui/Skeleton.jsx";

function Section({ title, subtitle, right, ai = false, children }) {
  return (
    <Card className={ai ? "border-indigo-200/60 bg-indigo-50/30" : undefined}>
      <CardHeader title={title} subtitle={subtitle} right={right} />
      <CardContent>{children}</CardContent>
    </Card>
  );
}

function RiskBadge({ risk }) {
  const r = (risk || "").toUpperCase();
  const variant = r === "HIGH" ? "danger" : r === "MEDIUM" ? "warning" : "default";
  return <Badge variant={variant}>{r || "—"}</Badge>;
}

function StatusBadge({ status }) {
  const s = status || "";
  const isRising = s.toLowerCase().includes("rising");
  return <Badge variant={isRising ? "warning" : "default"}>{s || "—"}</Badge>;
}

function NoData({ text }) {
  return <div className="text-sm text-slateink-500">{text || "No data for selection."}</div>;
}

export default function PredictiveAnalytics() {
  const { filters } = useContext(FiltersContext); // applied-only (GO)
  const [rising, setRising] = useState(null);
  const [risk, setRisk] = useState(null);
  const [chronic, setChronic] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const [explainBusy, setExplainBusy] = useState(false);
  const [explainError, setExplainError] = useState("");
  const [explain, setExplain] = useState(null);

  const windowDays = 30;

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setBusy(true);
      setError("");
      try {
        const [a, b, c] = await Promise.all([
          api.predictiveRisingSubtopics(filters, { windowDays, minVolume: 10, growthThreshold: 0.5, topN: 15 }),
          api.predictiveWardRisk(filters, { windowDays, minWardVolume: 30 }),
          api.predictiveChronicIssues(filters, { period: "month", topNPerPeriod: 5, minPeriods: 3, limit: 20 })
        ]);
        if (cancelled) return;
        setRising(a);
        setRisk(b);
        setChronic(c);
      } catch (e) {
        if (!cancelled) setError(e.message || "Failed to load predictive analytics");
      } finally {
        if (!cancelled) setBusy(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [filters]);

  const risingRows = rising?.rows || [];
  const riskRows = risk?.rows || [];
  const chronicRows = chronic?.rows || [];

  const subtitle = useMemo(() => {
    const s = filters?.start_date || "—";
    const e = filters?.end_date || "—";
    return `Date range: ${s} → ${e}`;
  }, [filters]);

  async function explainSubtopic(row) {
    setExplainBusy(true);
    setExplainError("");
    setExplain(null);
    try {
      const payload = {
        kind: "subtopic",
        name: row.subTopic,
        trend: row.status,
        window_days: windowDays,
        previous_count: row.previous,
        recent_count: row.recent,
        pct_change: row.pct_change
      };
      const res = await api.predictiveExplain(payload);
      setExplain({ title: `AI explanation — ${row.subTopic}`, text: res?.explanation || "", meta: res });
    } catch (e) {
      setExplainError(e.message || "Failed to generate explanation");
    } finally {
      setExplainBusy(false);
    }
  }

  async function explainWard(row) {
    setExplainBusy(true);
    setExplainError("");
    setExplain(null);
    try {
      const payload = {
        kind: "ward",
        name: row.ward,
        risk: row.risk,
        window_days: windowDays,
        previous_count: row.previous,
        recent_count: row.recent,
        pct_change: row.pct_change,
        distinct_subtopics_recent: row.distinct_subtopics_recent,
        repeat_density: row.repeat_density
      };
      const res = await api.predictiveExplain(payload);
      setExplain({ title: `AI explanation — ${row.ward}`, text: res?.explanation || "", meta: res });
    } catch (e) {
      setExplainError(e.message || "Failed to generate explanation");
    } finally {
      setExplainBusy(false);
    }
  }

  return (
    <div className="space-y-5">
      {error ? <div className="rounded-xl bg-white p-4 ring-1 ring-slateink-100 text-sm text-rose-700">{error}</div> : null}

      <div className="grid gap-4 lg:grid-cols-3">
        <Section title="Predictive Analytics" subtitle="Early Warning & Risk Signals" right={<span className="text-xs text-slateink-500">{subtitle}</span>}>
          <div className="text-sm text-slateink-700 leading-relaxed">
            This view uses <span className="font-semibold">trend comparisons</span>, repetition, and persistence signals. It does{" "}
            <span className="font-semibold">not</span> forecast numbers and does not run AI during aggregation.
          </div>
          <div className="mt-3 text-xs text-slateink-500">Click GO after selecting dates to refresh all sections.</div>
        </Section>
        <Section title="Signal window" right={<Badge variant="default">{windowDays} days</Badge>}>
          <div className="text-sm text-slateink-700 leading-relaxed">
            Rising signals compare the <span className="font-semibold">last {windowDays} days</span> vs the{" "}
            <span className="font-semibold">previous {windowDays} days</span> within your selected date range.
          </div>
        </Section>
        <Section title="AI explanatory insights" right={<AIBadge />} ai={true}>
          <div className="text-sm text-slateink-700 leading-relaxed">
            Gemini is used only to <span className="font-semibold">explain</span> computed trends. It never calculates metrics or predicts counts.
          </div>
        </Section>
      </div>

      {/* SECTION 1 — RISING SUB-TOPIC ALERTS */}
      <Section title="Rising sub-topic alerts" subtitle="Accelerating issues in the selected range" right={busy ? <span className="text-xs text-slateink-500">Loading…</span> : null} ai={true}>
        {busy && !rising ? (
          <div className="space-y-2">
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
          </div>
        ) : risingRows.length ? (
          <div className="overflow-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-xs font-semibold tracking-wide text-slateink-500">
                  <th className="py-2 pr-3">Sub-Topic</th>
                  <th className="py-2 pr-3">Previous</th>
                  <th className="py-2 pr-3">Recent</th>
                  <th className="py-2 pr-3">% Change</th>
                  <th className="py-2 pr-3">Status</th>
                  <th className="py-2 pr-3"></th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slateink-100">
                {risingRows.map((r) => (
                  <tr key={r.subTopic} className="text-slateink-800 hover:bg-slateink-50/60">
                    <td className="py-2 pr-3 font-semibold text-slateink-900">{r.subTopic}</td>
                    <td className="py-2 pr-3">{r.previous}</td>
                    <td className="py-2 pr-3">{r.recent}</td>
                    <td className="py-2 pr-3">{r.pct_change}%</td>
                    <td className="py-2 pr-3">
                      <StatusBadge status={r.status} />
                    </td>
                    <td className="py-2 pr-3">
                      <Button size="sm" variant="secondary" onClick={() => explainSubtopic(r)}>
                        Explain
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <NoData text="No rising issues in selected period (or range too short for two windows)." />
        )}
      </Section>

      {/* SECTION 2 — AT-RISK WARDS */}
      <Section title="At-risk wards (trend-based)" subtitle="Rule-based risk labels (no forecasting)" right={busy ? <span className="text-xs text-slateink-500">Loading…</span> : null} ai={true}>
        {busy && !risk ? (
          <div className="space-y-2">
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
          </div>
        ) : riskRows.length ? (
          <div className="overflow-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-xs font-semibold tracking-wide text-slateink-500">
                  <th className="py-2 pr-3">Ward</th>
                  <th className="py-2 pr-3">Risk</th>
                  <th className="py-2 pr-3">Previous</th>
                  <th className="py-2 pr-3">Recent</th>
                  <th className="py-2 pr-3">% Change</th>
                  <th className="py-2 pr-3">Distinct Sub-Topics</th>
                  <th className="py-2 pr-3">Repeat Density</th>
                  <th className="py-2 pr-3"></th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slateink-100">
                {riskRows.slice(0, 20).map((r) => (
                  <tr key={r.ward} className="text-slateink-800 hover:bg-slateink-50/60">
                    <td className="py-2 pr-3 font-semibold text-slateink-900">{r.ward}</td>
                    <td className="py-2 pr-3">
                      <RiskBadge risk={r.risk} />
                    </td>
                    <td className="py-2 pr-3">{r.previous}</td>
                    <td className="py-2 pr-3">{r.recent}</td>
                    <td className="py-2 pr-3">{r.pct_change}%</td>
                    <td className="py-2 pr-3">{r.distinct_subtopics_recent}</td>
                    <td className="py-2 pr-3">{r.repeat_density}</td>
                    <td className="py-2 pr-3">
                      <Button size="sm" variant="secondary" onClick={() => explainWard(r)}>
                        Explain
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <NoData text="No wards meet the minimum volume threshold in the selected period." />
        )}
      </Section>

      {/* SECTION 3 — CHRONIC ISSUES */}
      <Section
        title="Chronic issue continuation risk"
        subtitle="Issues that persist across multiple periods"
        right={
          busy ? <span className="text-xs text-slateink-500">Loading…</span> : chronic?.period ? <Badge variant="default">Period: {chronic.period}</Badge> : null
        }
        ai={true}
      >
        {busy && !chronic ? (
          <div className="space-y-2">
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
          </div>
        ) : chronicRows.length ? (
          <div className="overflow-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-xs font-semibold tracking-wide text-slateink-500">
                  <th className="py-2 pr-3">Sub-Topic</th>
                  <th className="py-2 pr-3">Periods Active</th>
                  <th className="py-2 pr-3">Total Count</th>
                  <th className="py-2 pr-3">Affected Wards</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slateink-100">
                {chronicRows.map((r) => (
                  <tr key={r.subTopic} className="text-slateink-800 hover:bg-slateink-50/60">
                    <td className="py-2 pr-3 font-semibold text-slateink-900">{r.subTopic}</td>
                    <td className="py-2 pr-3">{r.periods_active}</td>
                    <td className="py-2 pr-3">{r.total_count}</td>
                    <td className="py-2 pr-3">
                      {(r.affected_wards || []).slice(0, 6).join(", ")}
                      {(r.affected_wards || []).length > 6 ? "…" : ""}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <NoData text="No chronic issues detected for the selected period." />
        )}
      </Section>

      {/* SECTION 4 — AI EXPLANATORY INSIGHTS */}
      <Section
        title={
          <span className="inline-flex items-center gap-2">
            AI explanatory insights <AIBadge />
          </span>
        }
        subtitle="Gemini explains computed signals (no metrics, no predictions)"
        right={explainBusy ? <span className="text-xs text-slateink-500">Generating…</span> : null}
        ai={true}
      >
        {explainError ? <div className="text-sm text-rose-700">{explainError}</div> : null}
        {explain?.text ? (
          <div className="text-sm text-slateink-800 leading-relaxed">{explain.text}</div>
        ) : (
          <div className="text-sm text-slateink-500">
            Click “Explain” on a Rising Sub-Topic or At-Risk Ward to generate a short governance-safe explanation (no predictions).
          </div>
        )}
      </Section>
    </div>
  );
}


