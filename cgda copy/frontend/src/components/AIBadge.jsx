export default function AIBadge({ text = "Powered by caseA", tiny = "CaseA.ai" }) {
  return (
    <span
      className="inline-flex items-center gap-2 rounded-full bg-indigo-50 text-indigo-700 ring-1 ring-indigo-200 px-2.5 py-1 text-xs font-semibold"
      title="AI-derived fields powered by caseA.ai (Gemini)"
    >
      <span className="inline-flex items-center justify-center h-5 w-5 rounded-full bg-white text-indigo-700 ring-1 ring-indigo-200 text-xs font-extrabold">
        i
      </span>
      <span>{text}</span>
      <span className="text-[10px] font-semibold text-indigo-700/70 tracking-tight">{tiny}</span>
    </span>
  );
}


