import clsx from "clsx";

export function Card({ className, children }) {
  return (
    <div
      className={clsx(
        "rounded-2xl border border-slateink-200/50 bg-white shadow-sm",
        "transition-shadow hover:shadow-card",
        className
      )}
    >
      {children}
    </div>
  );
}

export function CardHeader({ className, title, right, subtitle, children }) {
  return (
    <div className={clsx("px-5 pt-5", className)}>
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          {title ? <div className="text-lg font-semibold text-slateink-900">{title}</div> : null}
          {subtitle ? <div className="mt-1 text-sm text-slateink-500">{subtitle}</div> : null}
        </div>
        {right ? <div className="shrink-0">{right}</div> : null}
      </div>
      {children ? <div className="mt-3">{children}</div> : null}
    </div>
  );
}

export function CardContent({ className, children }) {
  return <div className={clsx("px-5 pb-5", className)}>{children}</div>;
}


