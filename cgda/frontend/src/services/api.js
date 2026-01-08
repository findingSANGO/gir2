function detectApiBaseUrl() {
  const env = (import.meta.env.VITE_API_BASE_URL || "").trim();
  if (env) {
    // If the build-time env points to localhost, that breaks external users on a public URL.
    // For tunnel / reverse-proxy origins, force same-origin /api instead.
    try {
      const { hostname, port } = window.location;
      const isLocalBrowser = hostname === "localhost" || hostname === "127.0.0.1";
      const envIsLocalhost = env.includes("localhost") || env.includes("127.0.0.1");
      if (envIsLocalhost && (!isLocalBrowser || port !== "3000")) {
        return "";
      }
    } catch {
      // ignore
    }
    return env;
  }

  // Default behavior:
  // - When accessed via the combined reverse proxy (e.g., Cloudflare Tunnel -> :8080), use same-origin "/api/*".
  // - When accessing the frontend directly on :3000 locally, fall back to ":8000" on the same host.
  try {
    const { hostname, port, protocol } = window.location;
    if (port === "3000") {
      return `${protocol}//${hostname}:8000`;
    }
  } catch {
    // ignore
  }
  return "";
}

const API_BASE_URL = detectApiBaseUrl();

function getToken() {
  return localStorage.getItem("cgda_token");
}

function clearAuth() {
  localStorage.removeItem("cgda_token");
  localStorage.removeItem("cgda_username");
  localStorage.removeItem("cgda_role");
}

async function request(path, { method = "GET", body, headers } = {}) {
  const token = getToken();
  const res = await fetch(`${API_BASE_URL}${path}`, {
    method,
    headers: {
      ...(body instanceof FormData ? {} : { "Content-Type": "application/json" }),
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(headers || {})
    },
    body: body ? (body instanceof FormData ? body : JSON.stringify(body)) : undefined
  });

  if (!res.ok) {
    if (res.status === 401) {
      // Token is missing/expired/invalid (e.g., backend restarted). Clear auth and force re-login.
      clearAuth();
      if (!window.location.pathname.startsWith("/login")) window.location.href = "/login";
      throw new Error("Session expired. Please sign in again.");
    }
    const text = await res.text();
    throw new Error(text || `Request failed: ${res.status}`);
  }
  const contentType = res.headers.get("content-type") || "";
  if (contentType.includes("application/json")) return res.json();
  return res.text();
}

async function requestBlob(path, { method = "GET" } = {}) {
  const token = getToken();
  const res = await fetch(`${API_BASE_URL}${path}`, {
    method,
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {})
    }
  });
  if (!res.ok) {
    if (res.status === 401) {
      clearAuth();
      if (!window.location.pathname.startsWith("/login")) window.location.href = "/login";
      throw new Error("Session expired. Please sign in again.");
    }
    const text = await res.text();
    throw new Error(text || `Request failed: ${res.status}`);
  }
  return res.blob();
}

export const api = {
  baseUrl: API_BASE_URL,
  login: (username, password) => request("/api/auth/login", { method: "POST", body: { username, password } }),

  // Dimensions for filter dropdowns (sourced from grievances_processed for date-range analytics)
  dimensions: () => request("/api/analytics/dimensions_processed"),
  datasetsProcessed: () => request("/api/analytics/datasets_processed"),
  datasetQuality: (source) => request(`/api/analytics/dataset_quality${toQuery({ source })}`),
  aiCoverage: (source) => request(`/api/analytics/ai_coverage${toQuery({ source })}`),
  closureSlaSnapshot: (params) => request(`/api/analytics/closure_sla_snapshot${toQuery(params)}`),
  forwardingSnapshot: (params) => request(`/api/analytics/forwarding_snapshot${toQuery(params)}`),
  forwardingImpactResolution: (params) => request(`/api/analytics/forwarding_impact_resolution${toQuery(params)}`),
  retrospective: (params) => request(`/api/analytics/retrospective${toQuery(params)}`),
  inferential: (params) => request(`/api/analytics/inferential${toQuery(params)}`),
  feedback: (params) => request(`/api/analytics/feedback${toQuery(params)}`),
  closure: (params) => request(`/api/analytics/closure${toQuery(params)}`),
  predictive: (params) => request(`/api/analytics/predictive${toQuery(params)}`),
  wordcloud: (params, topN = 60) => request(`/api/analytics/wordcloud${toQuery({ ...(params || {}), top_n: topN })}`),

  // Date-range analytics (NO Gemini calls; reads grievances_processed)
  executiveOverview: (params) => request(`/api/analytics/executive-overview${toQuery(params)}`),
  executiveOverviewV2: (params) => request(`/api/executive_overview${toQuery(params)}`),
  issueIntelligenceV2: (params) => request(`/api/issue_intelligence${toQuery(params)}`),
  // pipelineStatus removed from production UI (debug-only).
  topSubtopics: (params, topN = 10) => request(`/api/analytics/top-subtopics${toQuery({ ...(params || {}), top_n: topN })}`),
  topSubtopicsByWard: (ward, params, topN = 5) =>
    request(`/api/analytics/top-subtopics/by-ward${toQuery({ ...(params || {}), ward, top_n: topN })}`),
  topSubtopicsByDepartment: (department, params, topN = 10) =>
    request(`/api/analytics/top-subtopics/by-department${toQuery({ ...(params || {}), department, top_n: topN })}`),
  subtopicTrend: (subtopic, params) => request(`/api/analytics/subtopic-trend${toQuery({ ...(params || {}), subtopic })}`),
  oneOfAKind: (params, limit = 25) => request(`/api/analytics/one-of-a-kind${toQuery({ ...(params || {}), limit })}`),

  // Predictive analytics (trend-based early warning; no Gemini except /explain)
  predictiveRisingSubtopics: (params, { windowDays = 14, minVolume = 10, growthThreshold = 0.5, topN = 15 } = {}) =>
    request(
      `/api/analytics/predictive/rising-subtopics${toQuery({
        ...(params || {}),
        window_days: windowDays,
        min_volume: minVolume,
        growth_threshold: growthThreshold,
        top_n: topN
      })}`
    ),
  predictiveWardRisk: (params, { windowDays = 14, minWardVolume = 30 } = {}) =>
    request(
      `/api/analytics/predictive/ward-risk${toQuery({
        ...(params || {}),
        window_days: windowDays,
        min_ward_volume: minWardVolume
      })}`
    ),
  predictiveChronicIssues: (params, { period = "week", topNPerPeriod = 5, minPeriods = 4, limit = 20 } = {}) =>
    request(
      `/api/analytics/predictive/chronic-issues${toQuery({
        ...(params || {}),
        period,
        top_n_per_period: topNPerPeriod,
        min_periods: minPeriods,
        limit
      })}`
    ),
  predictiveExplain: (payload) => request("/api/analytics/predictive/explain", { method: "POST", body: payload }),

  // Sub-topic intelligence (stored AI_SubTopic / GrievanceStructured.sub_issue)
  subtopicsTop: (params, limit = 10) =>
    request(`/api/analytics/subtopics/top${toQuery({ ...(params || {}), limit })}`),
  subtopicsByWard: (ward, params, limit = 5) => {
    const q = new URLSearchParams();
    if (ward) q.set("ward", ward);
    const base = toQuery(params || {});
    const baseParams = base.startsWith("?") ? base.slice(1) : base;
    if (baseParams) {
      for (const [k, v] of new URLSearchParams(baseParams).entries()) q.set(k, v);
    }
    if (limit) q.set("limit", String(limit));
    const s = q.toString();
    return request(`/api/analytics/subtopics/by-ward${s ? `?${s}` : ""}`);
  },
  subtopicsByDepartment: (department, params, limit = 10) => {
    const q = new URLSearchParams();
    if (department) q.set("department", department);
    const base = toQuery(params || {});
    const baseParams = base.startsWith("?") ? base.slice(1) : base;
    if (baseParams) {
      for (const [k, v] of new URLSearchParams(baseParams).entries()) q.set(k, v);
    }
    if (limit) q.set("limit", String(limit));
    const s = q.toString();
    return request(`/api/analytics/subtopics/by-department${s ? `?${s}` : ""}`);
  },
  subtopicsTrend: (subtopic, params) => {
    const q = new URLSearchParams();
    if (subtopic) q.set("subtopic", subtopic);
    const base = toQuery(params || {});
    const baseParams = base.startsWith("?") ? base.slice(1) : base;
    if (baseParams) {
      for (const [k, v] of new URLSearchParams(baseParams).entries()) q.set(k, v);
    }
    const s = q.toString();
    return request(`/api/analytics/subtopics/trend${s ? `?${s}` : ""}`);
  },

  uploadCsv: (file) => {
    const fd = new FormData();
    fd.append("file", file);
    return request(`/api/grievances/upload_csv`, {
      method: "POST",
      body: fd
    });
  },

  processPending: (batchSize = 8, maxBatches = 2) =>
    request(`/api/grievances/process_pending?batch_size=${encodeURIComponent(batchSize)}&max_batches=${encodeURIComponent(maxBatches)}`, {
      method: "POST"
    }),

  exportStructuredCsv: () => request("/api/grievances/export_structured_csv"),
  commissionerPdf: (params) => request(`/api/reports/commissioner_pdf${toQuery(params)}`)
  ,

  // NMMC/IES enrichment pipeline (drop file into data/raw)
  dataLatest: (rawDir = null) => request(`/api/data/latest${rawDir ? `?raw_dir=${encodeURIComponent(rawDir)}` : ""}`),
  dataFiles: (rawDir = null) => request(`/api/data/files${rawDir ? `?raw_dir=${encodeURIComponent(rawDir)}` : ""}`),
  dataPreprocess: ({ rawFilename = null, rawDir = null, limitRows = null } = {}) =>
    request(
      `/api/data/preprocess${toQuery({
        raw_filename: rawFilename || undefined,
        raw_dir: rawDir || undefined,
        limit_rows: limitRows == null ? undefined : limitRows
      })}`,
      { method: "POST" }
    ),
  dataEnrichTickets: ({ source, limitRows = null, forceReprocess = false } = {}) =>
    request(
      `/api/data/enrich_tickets${toQuery({
        source,
        limit_rows: limitRows == null ? undefined : limitRows,
        force_reprocess: forceReprocess ? "true" : "false"
      })}`,
      { method: "POST" }
    ),
  dataIngest: (limitRows = 100, { extraFeatures = false, rawFilename = null, rawDir = null, resetAnalytics = false } = {}) =>
    request(
      `/api/data/ingest?limit_rows=${encodeURIComponent(limitRows)}&extra_features=${encodeURIComponent(extraFeatures)}${
        rawFilename ? `&raw_filename=${encodeURIComponent(rawFilename)}` : ""
      }${rawDir ? `&raw_dir=${encodeURIComponent(rawDir)}` : ""}&reset_analytics=${encodeURIComponent(resetAnalytics)}`,
      { method: "POST" }
    ),
  dataRuns: () => request("/api/data/runs"),
  dataRun: (runId) => request(`/api/data/runs/${encodeURIComponent(runId)}`),
  downloadEnrichedCsv: () => requestBlob("/api/data/enriched/download"),
  downloadPreprocessedCsv: () => requestBlob("/api/data/preprocessed/download"),
  preprocessLatest: (rawFilename = null) =>
    request(`/api/data/preprocess${rawFilename ? `?raw_filename=${encodeURIComponent(rawFilename)}` : ""}`, { method: "POST" }),
  preprocessStatus: () => request("/api/data/preprocess/status"),
  buildAiOutputDataset: ({ baseSource, sampleSize = 100, outputSource = "ai_output_dataset", forceReprocess = true } = {}) =>
    request(
      `/api/data/build_ai_output_dataset${toQuery({
        base_source: baseSource,
        sample_size: sampleSize,
        output_source: outputSource,
        force_reprocess: forceReprocess ? "true" : "false"
      })}`,
      { method: "POST" }
    ),
  dataResults: (limit = 50, offset = 0, { source = null } = {}) =>
    request(
      `/api/data/results?limit=${encodeURIComponent(limit)}&offset=${encodeURIComponent(offset)}${
        source ? `&source=${encodeURIComponent(source)}` : ""
      }`
    ),
  // Manual evidence reports (no GenAI)
  reportsUpload: async ({ file, periodType, periodStart, periodEnd, notes }) => {
    const body = new FormData();
    body.append("file", file);
    body.append("period_type", periodType);
    body.append("period_start", periodStart);
    body.append("period_end", periodEnd);
    if (notes) body.append("notes", notes);
    return request("/api/reports/upload", { method: "POST", body });
  },
  reportsLatest: (periodType = "weekly") => request(`/api/reports/latest?period_type=${encodeURIComponent(periodType)}`),
  reportsList: (periodType = "monthly", limit = 30) =>
    request(`/api/reports?period_type=${encodeURIComponent(periodType)}&limit=${encodeURIComponent(limit)}`),
  reportsDownloadUrl: (id) => `${API_BASE_URL}/api/reports/download/${encodeURIComponent(id)}`
};

function toQuery(params) {
  if (!params) return "";
  const q = new URLSearchParams();
  if (params.start_date) q.set("start_date", params.start_date);
  if (params.end_date) q.set("end_date", params.end_date);
  if (params.source) q.set("source", params.source);
  if (params.wards && params.wards.length) q.set("wards", params.wards.join(","));
  if (params.department) q.set("department", params.department);
  if (params.category) q.set("category", params.category);
  if (params.ai_category) q.set("ai_category", params.ai_category);
  if (params.ward_focus) q.set("ward_focus", params.ward_focus);
  if (params.department_focus) q.set("department_focus", params.department_focus);
  if (params.subtopic_focus) q.set("subtopic_focus", params.subtopic_focus);
  if (params.unique_min_priority != null) q.set("unique_min_priority", String(params.unique_min_priority));
  if (params.unique_confidence_high_only != null)
    q.set("unique_confidence_high_only", String(params.unique_confidence_high_only));
  if (params.top_n) q.set("top_n", params.top_n);
  if (params.limit) q.set("limit", params.limit);
  if (params.ward) q.set("ward", params.ward);
  if (params.subtopic) q.set("subtopic", params.subtopic);
  if (params.window_days != null) q.set("window_days", params.window_days);
  if (params.min_volume != null) q.set("min_volume", params.min_volume);
  if (params.growth_threshold != null) q.set("growth_threshold", params.growth_threshold);
  if (params.min_ward_volume != null) q.set("min_ward_volume", params.min_ward_volume);
  if (params.period) q.set("period", params.period);
  if (params.top_n_per_period != null) q.set("top_n_per_period", params.top_n_per_period);
  if (params.min_periods != null) q.set("min_periods", params.min_periods);
  const s = q.toString();
  return s ? `?${s}` : "";
}


