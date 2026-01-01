function hashStringToInt(s) {
  const str = String(s || "");
  let h = 2166136261;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

export function colorForKey(key) {
  // Theme-ish color generation (no hardcoded palette), stable across sessions.
  // Produces visually distinct hues for categories/subtopics.
  const h = hashStringToInt(key);
  const hue = h % 360;
  const sat = 62; // balanced saturation for charts
  const light = 44; // readable on white
  return `hsl(${hue} ${sat}% ${light}%)`;
}

export function softColorForKey(key) {
  const h = hashStringToInt(key);
  const hue = h % 360;
  return `hsl(${hue} 70% 92%)`;
}


