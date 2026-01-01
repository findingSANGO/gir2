import Badge from "./ui/Badge.jsx";

export default function AIBadge({ text = "Powered by caseA", tiny = "CaseA.ai" }) {
  return (
    <Badge
      variant="ai"
      className="gap-2 px-3 py-1"
      title="AI-derived labels/insights powered by caseA (Gemini)"
    >
      <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-white text-indigo-700 ring-1 ring-indigo-200 text-[11px] font-extrabold">
        AI
      </span>
      <span>{text}</span>
      <span className="text-[10px] font-semibold text-indigo-700/70 tracking-tight">{tiny}</span>
    </Badge>
  );
}


