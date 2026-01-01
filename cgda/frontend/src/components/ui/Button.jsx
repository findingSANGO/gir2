import clsx from "clsx";

const variants = {
  primary: "bg-gov-600 text-white hover:bg-gov-700 focus-visible:ring-gov-200",
  secondary: "bg-white text-slateink-900 ring-1 ring-slateink-200 hover:ring-slateink-300 focus-visible:ring-slateink-200",
  ghost: "bg-transparent text-slateink-900 hover:bg-slateink-50 focus-visible:ring-slateink-200",
  dark: "bg-slateink-900 text-white hover:bg-slateink-800 focus-visible:ring-slateink-300"
};

const sizes = {
  sm: "h-9 px-3 text-sm",
  md: "h-10 px-4 text-sm",
  lg: "h-11 px-5 text-sm"
};

export default function Button({
  className,
  variant = "secondary",
  size = "md",
  disabled,
  type = "button",
  ...props
}) {
  return (
    <button
      type={type}
      disabled={disabled}
      className={clsx(
        "inline-flex items-center justify-center gap-2 rounded-xl font-semibold",
        "outline-none transition-colors focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-slateink-50",
        "disabled:opacity-50 disabled:cursor-not-allowed",
        variants[variant],
        sizes[size],
        className
      )}
      {...props}
    />
  );
}


