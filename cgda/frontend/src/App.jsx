import React, { createContext, useEffect, useMemo, useState } from "react";
import { Navigate, Route, Routes, useLocation } from "react-router-dom";
import Sidebar from "./components/Sidebar.jsx";
import Navbar from "./components/Navbar.jsx";
import Filters from "./components/Filters.jsx";
import { api } from "./services/api.js";

import Login from "./pages/Login.jsx";
import Dashboard from "./pages/Dashboard.jsx";
import IssueIntelligence from "./pages/IssueIntelligence.jsx";
import IssueIntelligence2 from "./pages/IssueIntelligence2.jsx";
import FeedbackAnalytics from "./pages/FeedbackAnalytics.jsx";
import ClosureAnalytics from "./pages/ClosureAnalytics.jsx";
import PredictiveView from "./pages/PredictiveView.jsx";
import PredictiveAnalytics from "./pages/PredictiveAnalytics.jsx";
import Evidence from "./pages/Evidence.jsx";
import UploadEnrich from "./pages/UploadEnrich.jsx";
import Datasets from "./pages/Datasets.jsx";

function getAuth() {
  const token = localStorage.getItem("cgda_token");
  const username = localStorage.getItem("cgda_username");
  const role = localStorage.getItem("cgda_role");
  return { token, username, role };
}

function clearAuth() {
  localStorage.removeItem("cgda_token");
  localStorage.removeItem("cgda_username");
  localStorage.removeItem("cgda_role");
  localStorage.removeItem("cgda_dataset_loaded");
  localStorage.removeItem("cgda_dataset_source");
}

function Protected({ children }) {
  const { token } = getAuth();
  const location = useLocation();
  if (!token) return <Navigate to="/login" replace state={{ from: location.pathname }} />;
  return children;
}

export const FiltersContext = createContext({ filters: {}, setFilters: () => {} });

function pickOldDataset(datasets) {
  if (!Array.isArray(datasets) || !datasets.length) return null;
  // Old = strongest AI coverage (Gemini-enriched)
  const sorted = [...datasets].sort((a, b) => {
    const aa = a.ai_subtopic_rows || 0;
    const ba = b.ai_subtopic_rows || 0;
    if (ba !== aa) return ba - aa;
    return (b.count || 0) - (a.count || 0);
  });
  return sorted[0]?.source || null;
}

function pickNewDataset(datasets) {
  if (!Array.isArray(datasets) || !datasets.length) return null;
  const isIdUnique = (s) => String(s || "").endsWith("__id_unique");
  // Prefer FULL dataset once AI coverage exists (post-enrichment).
  // If enrichment is still in-progress, fall back to the largest __run1_ sample.
  const isRun1 = (s) => String(s || "").includes("__run1_");

  const fullAiReady = [...datasets]
    .filter((d) => !isRun1(d.source) && (d.ai_subtopic_rows || 0) > 0)
    .sort((a, b) => (b.count || 0) - (a.count || 0));
  // If an id-level unique dataset exists, prefer it as “new” default.
  const idUnique = fullAiReady.filter((d) => isIdUnique(d.source));
  if (idUnique.length) return idUnique[0]?.source || null;
  if (fullAiReady.length) return fullAiReady[0]?.source || null;

  const run1 = [...datasets]
    .filter((d) => isRun1(d.source))
    .sort((a, b) => (b.count || 0) - (a.count || 0));
  if (run1.length) return run1[0]?.source || null;

  // Else: strongest "new columns" signal, then most recent coverage
  const sorted = [...datasets].sort((a, b) => {
    const an = a.new_signal_rows || 0;
    const bn = b.new_signal_rows || 0;
    if (bn !== an) return bn - an;
    const ad = a.max_created_date || "";
    const bd = b.max_created_date || "";
    if (bd !== ad) return bd.localeCompare(ad);
    return String(b.source || "").localeCompare(String(a.source || ""));
  });
  return sorted[0]?.source || null;
}

function Shell({ title, children, onUpload, mode = null }) {
  const user = useMemo(() => {
    const a = getAuth();
    return { username: a.username, role: a.role };
  }, []);

  const location = useLocation();

  // Draft filters are edited in the UI; Applied filters are used for API calls (GO button).
  function daysAgo(n) {
    const d = new Date();
    d.setDate(d.getDate() - n);
    return d.toISOString().slice(0, 10);
  }
  function today() {
    return new Date().toISOString().slice(0, 10);
  }

  const defaultDraft = {
    start_date: daysAgo(29),
    end_date: today(),
    // Fixed file-pipeline default: app runs on processed_data_500 without user selection.
    source: "processed_data_500",
    wards: [],
    department: null,
    category: null,
    ai_category: null
  };

  const [draftFilters, setDraftFilters] = useState(defaultDraft);
  const [filters, setFilters] = useState(defaultDraft); // applied
  const gateOpen = false;
  const gateLoading = false;
  const gateDatasets = [];
  const gateSelected = null;
  const gateRecommended = null;

  // /old vs /new modes: pick a default dataset source automatically (no user guessing).
  useEffect(() => {
    let cancelled = false;
    (async () => {
      if (!mode) return;
      try {
        const res = await api.datasetsProcessed();
        const ds = res?.datasets || [];
        const recommended =
          mode === "old" ? (res?.recommended_old_source || null) : (res?.recommended_new_source || null);
        const chosen = recommended || (mode === "old" ? pickOldDataset(ds) : pickNewDataset(ds));
        if (!chosen) return;
        if (cancelled) return;
        setDraftFilters((s) => (s.source ? s : { ...s, source: chosen }));
        setFilters((s) => (s.source ? s : { ...s, source: chosen }));
      } catch {
        // ignore
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [mode]);

  // No dataset gate in the file-pipeline mode: always run on processed_data_500.

  return (
    <div className="min-h-screen bg-slateink-50">
      <Sidebar />
      <div className="lg:pl-72">
        <Navbar
          title={title}
          user={user}
          onLogout={() => {
            clearAuth();
            window.location.href = "/login";
          }}
          onUpload={null}
        />
        <Filters
          filters={draftFilters}
          setFilters={setDraftFilters}
          onApply={(next) => setFilters(next || draftFilters)}
          showDataset={false}
        />
        <main className="mx-auto max-w-7xl px-4 lg:px-6 py-6">
          <FiltersContext.Provider value={{ filters, draftFilters, setDraftFilters, applyFilters: () => setFilters(draftFilters) }}>
            {children}
          </FiltersContext.Provider>
        </main>
      </div>
    </div>
  );
}

function UploadDialog({ open, onClose }) {
  const [file, setFile] = useState(null);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState("");
  if (!open) return null;

  async function submit() {
    if (!file) return;
    setBusy(true);
    setMsg("");
    try {
      const res = await api.uploadCsv(file);
      setMsg(`Upload OK. Inserted: ${res.inserted}, duplicates skipped: ${res.skipped_duplicates}. Processing job: ${res.process_job_id}`);
    } catch (e) {
      setMsg(e.message || "Upload failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slateink-900/40 p-4">
      <div className="w-full max-w-lg rounded-xl bg-white shadow-card ring-1 ring-slateink-100 p-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="text-lg font-semibold text-slateink-900">Upload grievance CSV</div>
            <div className="text-sm text-slateink-500">Raw file is stored untouched; analytics are generated from processed outputs.</div>
          </div>
          <button
            onClick={onClose}
            className="rounded-lg px-3 py-2 text-sm font-semibold text-slateink-700 hover:bg-slateink-100"
          >
            Close
          </button>
        </div>

        <div className="mt-4">
          <input
            type="file"
            accept=".csv"
            onChange={(e) => setFile(e.target.files?.[0] || null)}
            className="block w-full text-sm text-slateink-700 file:mr-3 file:rounded-lg file:border-0 file:bg-gov-600 file:px-3 file:py-2 file:text-sm file:font-semibold file:text-white hover:file:bg-gov-700"
          />
          <div className="mt-4 flex items-center justify-end gap-2">
            <button
              onClick={onClose}
              className="rounded-lg bg-white px-3 py-2 text-sm font-semibold text-slateink-800 ring-1 ring-slateink-200 hover:ring-slateink-300"
            >
              Cancel
            </button>
            <button
              disabled={!file || busy}
              onClick={submit}
              className="rounded-lg bg-slateink-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-50 hover:bg-slateink-800"
            >
              {busy ? "Uploading..." : "Upload & Process"}
            </button>
          </div>
          {msg ? <div className="mt-3 text-sm text-slateink-700">{msg}</div> : null}
        </div>
      </div>
    </div>
  );
}

export default function App() {
  const [uploadOpen, setUploadOpen] = useState(false);

  return (
    <>
      <Routes>
        <Route path="/login" element={<Login />} />
        {/* OLD system: pinned to baseline dataset (largest). */}
        <Route
          path="/old"
          element={
            <Protected>
              <Shell title="Executive Overview (Old)" onUpload={() => setUploadOpen(true)} mode="old">
                <Dashboard />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/old/issue-intelligence"
          element={
            <Protected>
              <Shell title="Issue Intelligence (Old)" onUpload={() => setUploadOpen(true)} mode="old">
                <IssueIntelligence />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/old/issue-intelligence2"
          element={
            <Protected>
              <Shell title="Issue Intelligence 2 (Old)" onUpload={() => setUploadOpen(true)} mode="old">
                <IssueIntelligence2 />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/old/feedback-analytics"
          element={
            <Protected>
              <Shell title="Citizen Feedback Analytics (Old)" onUpload={() => setUploadOpen(true)} mode="old">
                <FeedbackAnalytics />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/old/closure-analytics"
          element={
            <Protected>
              <Shell title="Closure Time Analytics (Old)" onUpload={() => setUploadOpen(true)} mode="old">
                <ClosureAnalytics />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/old/predictive"
          element={
            <Protected>
              <Shell title="Predictive Analytics (Old)" onUpload={() => setUploadOpen(true)} mode="old">
                <PredictiveAnalytics />
              </Shell>
            </Protected>
          }
        />

        {/* NEW system: defaults to latest dataset (by max date), user can switch dataset. */}
        <Route
          path="/new"
          element={
            <Protected>
              <Shell title="Executive Overview (New)" onUpload={() => setUploadOpen(true)} mode="new">
                <Dashboard />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/new/issue-intelligence"
          element={
            <Protected>
              <Shell title="Issue Intelligence (New)" onUpload={() => setUploadOpen(true)} mode="new">
                <IssueIntelligence />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/new/issue-intelligence2"
          element={
            <Protected>
              <Shell title="Issue Intelligence 2 (New)" onUpload={() => setUploadOpen(true)} mode="new">
                <IssueIntelligence2 />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/new/feedback-analytics"
          element={
            <Protected>
              <Shell title="Citizen Feedback Analytics (New)" onUpload={() => setUploadOpen(true)} mode="new">
                <FeedbackAnalytics />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/new/closure-analytics"
          element={
            <Protected>
              <Shell title="Closure Time Analytics (New)" onUpload={() => setUploadOpen(true)} mode="new">
                <ClosureAnalytics />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/new/predictive"
          element={
            <Protected>
              <Shell title="Predictive Analytics (New)" onUpload={() => setUploadOpen(true)} mode="new">
                <PredictiveAnalytics />
              </Shell>
            </Protected>
          }
        />

        <Route
          path="/"
          element={
            <Protected>
              <Shell title="Executive Overview" onUpload={() => setUploadOpen(true)}>
                <Dashboard />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/datasets"
          element={
            <Protected>
              <Shell title="Datasets" onUpload={() => setUploadOpen(true)}>
                <Datasets />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/issue-intelligence"
          element={
            <Protected>
              <Shell title="Issue Intelligence" onUpload={() => setUploadOpen(true)}>
                <IssueIntelligence />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/issue-intelligence2"
          element={
            <Protected>
              <Shell title="Issue Intelligence 2" onUpload={() => setUploadOpen(true)}>
                <IssueIntelligence2 />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/feedback-analytics"
          element={
            <Protected>
              <Shell title="Citizen Feedback Analytics" onUpload={() => setUploadOpen(true)}>
                <FeedbackAnalytics />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/closure-analytics"
          element={
            <Protected>
              <Shell title="Closure Time Analytics" onUpload={() => setUploadOpen(true)}>
                <ClosureAnalytics />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/predictive"
          element={
            <Protected>
              <Shell title="Predictive Analytics" onUpload={() => setUploadOpen(true)}>
                <PredictiveAnalytics />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/predictive-analytics"
          element={
            <Protected>
              <Shell title="Predictive Analytics" onUpload={() => setUploadOpen(true)}>
                <PredictiveAnalytics />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/evidence"
          element={
            <Protected>
              <Shell title="Evidence & Exports" onUpload={() => setUploadOpen(true)}>
                <Evidence />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/upload-enrich"
          element={
            <Protected>
              <Shell title="Upload & Enrich" onUpload={() => setUploadOpen(true)}>
                <UploadEnrich />
              </Shell>
            </Protected>
          }
        />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
      <UploadDialog open={uploadOpen} onClose={() => setUploadOpen(false)} />
    </>
  );
}


