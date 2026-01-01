import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";

import clsx from "clsx";
import { colorForKey } from "../utils/chartColors.js";
import { Card, CardContent, CardHeader } from "./ui/Card.jsx";
import Skeleton from "./ui/Skeleton.jsx";

function ChartShell({ title, subtitle, right, children, ai = false }) {
  return (
    <Card className={clsx(ai && "border-indigo-200/60 bg-indigo-50/30")}>
      <CardHeader title={title} subtitle={subtitle} right={right} />
      <CardContent>{children}</CardContent>
    </Card>
  );
}

function EmptyState({ text = "No data for selection." }) {
  return (
    <div className="flex items-center justify-center rounded-xl border border-dashed border-slateink-200 bg-white/40 px-4 py-10">
      <div className="text-sm text-slateink-500">{text}</div>
    </div>
  );
}

function ChartTooltip({ active, payload, label, valueFormatter, total }) {
  if (!active || !payload?.length) return null;
  const row = payload[0] || {};
  const value = row.value;
  const pct = total ? (Number(value || 0) / Number(total)) * 100 : null;
  return (
    <div className="rounded-xl border border-slateink-200 bg-white/95 shadow-card px-3 py-2">
      {label != null ? <div className="text-xs font-semibold text-slateink-700">{label}</div> : null}
      <div className="mt-1 text-sm font-semibold text-slateink-900">
        {valueFormatter ? valueFormatter(value) : value}
        {pct != null && Number.isFinite(pct) ? (
          <span className="ml-2 text-xs font-semibold text-slateink-500">({pct.toFixed(1)}%)</span>
        ) : null}
      </div>
    </div>
  );
}

export function BarCard({ title, subtitle, right, data, xKey, bars, height = 260, ai = false }) {
  return (
    <ChartShell title={title} subtitle={subtitle} right={right} ai={ai}>
      {!data ? (
        <div style={{ height }} className="space-y-3">
          <Skeleton className="h-6 w-48" />
          <Skeleton className="h-[220px] w-full" />
        </div>
      ) : !data.length ? (
        <EmptyState />
      ) : (
        <div style={{ height }}>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data} margin={{ top: 6, right: 10, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="4 8" stroke="#eceef2" />
              <XAxis dataKey={xKey} tick={{ fontSize: 12 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontSize: 12 }} axisLine={false} tickLine={false} />
              <Tooltip content={<ChartTooltip total={null} />} />
              <Legend />
              {bars.map((b) => (
                <Bar
                  key={b.key}
                  dataKey={b.key}
                  name={b.name || b.key}
                  fill={b.color || "#2b54f6"}
                  radius={[10, 10, 0, 0]}
                />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </ChartShell>
  );
}

export function VerticalBarCard({
  title,
  subtitle,
  right,
  data,
  yKey,
  valueKey,
  height = 320,
  ai = false,
  total
}) {
  // Expect [{[yKey]: string, [valueKey]: number}]
  return (
    <ChartShell title={title} subtitle={subtitle} right={right} ai={ai}>
      {!data ? (
        <div style={{ height }} className="space-y-3">
          <Skeleton className="h-6 w-56" />
          <Skeleton className="h-[260px] w-full" />
        </div>
      ) : !data.length ? (
        <EmptyState />
      ) : (
        <div style={{ height }}>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={data}
              layout="vertical"
              margin={{ top: 6, right: 18, left: 8, bottom: 0 }}
              barCategoryGap={10}
            >
              <CartesianGrid strokeDasharray="4 8" stroke="#eceef2" horizontal={false} />
              <XAxis type="number" tick={{ fontSize: 12 }} axisLine={false} tickLine={false} />
              <YAxis
                type="category"
                dataKey={yKey}
                width={180}
                tick={{ fontSize: 12 }}
                axisLine={false}
                tickLine={false}
              />
              <Tooltip
                content={
                  <ChartTooltip
                    total={total}
                    valueFormatter={(v) => `${v}`}
                  />
                }
              />
              <Bar dataKey={valueKey} radius={[10, 10, 10, 10]}>
                {(data || []).map((d) => (
                  <Cell key={String(d?.[yKey])} fill={colorForKey(d?.[yKey])} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </ChartShell>
  );
}

export function LineCard({ title, subtitle, right, data, xKey, lines, height = 260, ai = false }) {
  return (
    <ChartShell title={title} subtitle={subtitle} right={right} ai={ai}>
      {!data ? (
        <div style={{ height }} className="space-y-3">
          <Skeleton className="h-6 w-52" />
          <Skeleton className="h-[220px] w-full" />
        </div>
      ) : !data.length ? (
        <EmptyState />
      ) : (
        <div style={{ height }}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} margin={{ top: 6, right: 10, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="4 8" stroke="#eceef2" />
              <XAxis dataKey={xKey} tick={{ fontSize: 12 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontSize: 12 }} axisLine={false} tickLine={false} />
              <Tooltip content={<ChartTooltip total={null} />} />
              <Legend />
              {lines.map((l) => (
                <Line
                  key={l.key}
                  type="monotone"
                  dataKey={l.key}
                  name={l.name || l.key}
                  stroke={l.color || "#2b54f6"}
                  strokeWidth={2.5}
                  dot={false}
                  activeDot={{ r: 4 }}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </ChartShell>
  );
}

export function PieCard({ title, data, nameKey, valueKey, colors, height = 260, ai = false }) {
  return (
    <ChartShell title={title} ai={ai}>
      {!data ? (
        <div style={{ height }} className="space-y-3">
          <Skeleton className="h-6 w-40" />
          <Skeleton className="h-[220px] w-full" />
        </div>
      ) : !data.length ? (
        <EmptyState />
      ) : (
        <div style={{ height }}>
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Tooltip content={<ChartTooltip total={null} />} />
              <Legend />
              <Pie data={data} dataKey={valueKey} nameKey={nameKey} innerRadius={58} outerRadius={88} paddingAngle={2}>
                {data.map((d, i) => (
                  <Cell
                    key={i}
                    fill={(colors && colors[i % colors.length]) || colorForKey(d?.[nameKey])}
                  />
                ))}
              </Pie>
            </PieChart>
          </ResponsiveContainer>
        </div>
      )}
    </ChartShell>
  );
}

export function SimpleTable({ title, columns, rows, ai = false }) {
  return (
    <ChartShell title={title} ai={ai}>
      {!rows ? (
        <div className="space-y-2">
          <Skeleton className="h-10 w-full" />
          <Skeleton className="h-10 w-full" />
          <Skeleton className="h-10 w-full" />
        </div>
      ) : !rows.length ? (
        <EmptyState />
      ) : (
        <div className="overflow-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs font-semibold tracking-wide text-slateink-500">
                {columns.map((c) => (
                  <th key={c.key} className="py-2 pr-3">
                    {c.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-slateink-100">
              {rows.map((r, idx) => (
                <tr key={idx} className="text-slateink-800 hover:bg-slateink-50/60">
                  {columns.map((c) => (
                    <td key={c.key} className="py-2 pr-3">
                      {r[c.key]}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </ChartShell>
  );
}

export function WordCloudCard({ title, words, right }) {
  const items = words || [];
  const counts = items.map((w) => w.count || 0);
  const max = Math.max(1, ...counts);
  const min = Math.min(max, ...counts);

  function size(c) {
    if (max === min) return 22;
    const t = (c - min) / (max - min);
    return Math.round(14 + t * 20);
  }

  const colors = ["text-indigo-700", "text-gov-700", "text-slateink-700", "text-indigo-900"];

  return (
    <ChartShell title={title} right={right}>
      <div className="flex flex-wrap gap-2">
        {items.map((w, idx) => (
          <span
            key={w.text + idx}
            className={
              "rounded-full bg-slateink-50 ring-1 ring-slateink-200 px-3 py-1 " +
              colors[idx % colors.length]
            }
            style={{ fontSize: `${size(w.count)}px`, lineHeight: 1.15 }}
            title={`${w.text}: ${w.count}`}
          >
            {w.text}
          </span>
        ))}
        {!items.length ? <div className="text-sm text-slateink-500">No words available.</div> : null}
      </div>
      <div className="mt-3 text-xs text-slateink-500">Top terms from the currently loaded dataset (filters applied).</div>
    </ChartShell>
  );
}


