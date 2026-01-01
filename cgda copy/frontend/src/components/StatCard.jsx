import clsx from "clsx";

const ragStyles = {
  GREEN: "bg-emerald-50 text-emerald-800 ring-emerald-200",
  AMBER: "bg-amber-50 text-amber-800 ring-amber-200",
  RED: "bg-rose-50 text-rose-800 ring-rose-200"
};

export default function StatCard({ title, value, subtitle, rag, ai = false }) {
  return (
    <div
      className={clsx(
        "rounded-xl shadow-card ring-1 p-4 transition-shadow hover:shadow-[0_1px_2px_rgba(15,23,42,0.08),0_12px_36px_rgba(15,23,42,0.10)]",
        ai ? "bg-indigo-50/40 ring-indigo-200" : "bg-white ring-slateink-100"
      )}
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-xs font-semibold uppercase tracking-wide text-slateink-500">{title}</div>
          <div className="mt-2 text-2xl font-semibold text-slateink-900">{value}</div>
          {subtitle ? <div className="mt-1 text-sm text-slateink-500">{subtitle}</div> : null}
        </div>
        {rag ? (
          <span
            className={clsx(
              "inline-flex items-center rounded-full px-2.5 py-1 text-xs font-semibold ring-1",
              ragStyles[rag] || ragStyles.AMBER
            )}
            title="Red/Amber/Green indicator"
          >
            {rag}
          </span>
        ) : null}
      </div>
    </div>
  );
}


