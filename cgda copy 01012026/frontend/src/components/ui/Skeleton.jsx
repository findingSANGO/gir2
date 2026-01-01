import clsx from "clsx";

export default function Skeleton({ className }) {
  return (
    <div
      className={clsx(
        "animate-pulse rounded-xl bg-slateink-100",
        className
      )}
    />
  );
}


