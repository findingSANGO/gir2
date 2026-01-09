// UI-only label normalization helpers.
// Important: keep stored values unchanged; use these only for display.

function _norm(s) {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

/**
 * Display-friendly Sub-Topic label.
 * - Keeps underlying raw value intact (do NOT use this for filters / keys to backend).
 */
export function displaySubtopicLabel(raw) {
  const n = _norm(raw);
  if (!n) return raw;

  // Bucket for missing/uncertain classifications.
  if (n === "generic civic issue" || n === "general civic issue") {
    return "Needs further Investigation";
  }

  return raw;
}


