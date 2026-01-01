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

export function BarCard({ title, data, xKey, bars, height = 260, ai = false }) {
  return (
    <Card title={title} ai={ai}>
      <div style={{ height }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#eceef2" />
            <XAxis dataKey={xKey} tick={{ fontSize: 12 }} />
            <YAxis tick={{ fontSize: 12 }} />
            <Tooltip />
            <Legend />
            {bars.map((b) => (
              <Bar key={b.key} dataKey={b.key} name={b.name || b.key} fill={b.color || "#2b54f6"} radius={[6, 6, 0, 0]} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}

export function LineCard({ title, data, xKey, lines, height = 260, ai = false }) {
  return (
    <Card title={title} ai={ai}>
      <div style={{ height }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#eceef2" />
            <XAxis dataKey={xKey} tick={{ fontSize: 12 }} />
            <YAxis tick={{ fontSize: 12 }} />
            <Tooltip />
            <Legend />
            {lines.map((l) => (
              <Line
                key={l.key}
                type="monotone"
                dataKey={l.key}
                name={l.name || l.key}
                stroke={l.color || "#2b54f6"}
                strokeWidth={2}
                dot={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}

export function PieCard({ title, data, nameKey, valueKey, colors, height = 260, ai = false }) {
  return (
    <Card title={title} ai={ai}>
      <div style={{ height }}>
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Tooltip />
            <Legend />
            <Pie data={data} dataKey={valueKey} nameKey={nameKey} innerRadius={55} outerRadius={85} paddingAngle={2}>
              {data.map((_, i) => (
                <Cell key={i} fill={(colors && colors[i % colors.length]) || "#2b54f6"} />
              ))}
            </Pie>
          </PieChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}

export function SimpleTable({ title, columns, rows, ai = false }) {
  return (
    <Card title={title} ai={ai}>
      <div className="overflow-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-xs uppercase tracking-wide text-slateink-500">
              {columns.map((c) => (
                <th key={c.key} className="py-2 pr-3">
                  {c.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slateink-100">
            {rows.map((r, idx) => (
              <tr key={idx} className="text-slateink-800">
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
    </Card>
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
    <Card title={title} right={right}>
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
    </Card>
  );
}


