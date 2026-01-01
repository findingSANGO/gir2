import clsx from "clsx";

const variants = {
  default: "bg-slateink-50 text-slateink-700 ring-slateink-200",
  ai: "bg-indigo-50 text-indigo-700 ring-indigo-200",
  success: "bg-emerald-50 text-emerald-700 ring-emerald-200",
  warning: "bg-amber-50 text-amber-700 ring-amber-200",
  danger: "bg-rose-50 text-rose-700 ring-rose-200"
};

export default function Badge({ className, variant = "default", children, ...props }) {
  return (
    <span
      className={clsx(
        "inline-flex items-center rounded-full px-2.5 py-1 text-xs font-semibold ring-1",
        variants[variant],
        className
      )}
      {...props}
    >
      {children}
    </span>
  );
}


