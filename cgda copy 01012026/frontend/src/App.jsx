import React, { createContext, useMemo, useState } from "react";
import { Navigate, Route, Routes, useLocation } from "react-router-dom";
import Sidebar from "./components/Sidebar.jsx";
import Navbar from "./components/Navbar.jsx";
import Filters from "./components/Filters.jsx";
import { api } from "./services/api.js";

import Login from "./pages/Login.jsx";
import Dashboard from "./pages/Dashboard.jsx";
import IssueIntelligence from "./pages/IssueIntelligence.jsx";
import FeedbackAnalytics from "./pages/FeedbackAnalytics.jsx";
import ClosureAnalytics from "./pages/ClosureAnalytics.jsx";
import PredictiveView from "./pages/PredictiveView.jsx";
import PredictiveAnalytics from "./pages/PredictiveAnalytics.jsx";
import Evidence from "./pages/Evidence.jsx";
import UploadEnrich from "./pages/UploadEnrich.jsx";

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
}

function Protected({ children }) {
  const { token } = getAuth();
  const location = useLocation();
  if (!token) return <Navigate to="/login" replace state={{ from: location.pathname }} />;
  return children;
}

export const FiltersContext = createContext({ filters: {}, setFilters: () => {} });

function Shell({ title, children, onUpload }) {
  const user = useMemo(() => {
    const a = getAuth();
    return { username: a.username, role: a.role };
  }, []);

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
    wards: [],
    department: null,
    category: null,
    ai_category: null
  };

  const [draftFilters, setDraftFilters] = useState(defaultDraft);
  const [filters, setFilters] = useState(defaultDraft); // applied

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
          onUpload={onUpload}
        />
        <Filters
          filters={draftFilters}
          setFilters={setDraftFilters}
          onApply={() => setFilters(draftFilters)}
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


