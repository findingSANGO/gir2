import clsx from "clsx";
import { Card, CardContent } from "./ui/Card.jsx";
import Badge from "./ui/Badge.jsx";

const ragStyles = {
  GREEN: "success",
  AMBER: "warning",
  RED: "danger"
};

export default function StatCard({ title, value, subtitle, rag, ai = false }) {
  return (
    <Card className={clsx(ai && "border-indigo-200/60 bg-indigo-50/30")}>
      <CardContent className="pt-5">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <div className="text-xs font-semibold uppercase tracking-wide text-slateink-500">{title}</div>
            <div className="mt-2 text-3xl font-semibold text-slateink-900 leading-tight">{value}</div>
            {subtitle ? <div className="mt-1 text-sm text-slateink-500">{subtitle}</div> : null}
          </div>
          {rag ? (
            <Badge variant={ragStyles[rag] || "warning"} title="Red/Amber/Green indicator">
              {rag}
            </Badge>
          ) : null}
        </div>
      </CardContent>
    </Card>
  );
}


