import React, { createContext, useEffect, useMemo, useState } from "react";
import { Navigate, Route, Routes, useLocation } from "react-router-dom";
import Sidebar from "./components/Sidebar.jsx";
import Navbar from "./components/Navbar.jsx";
import Filters from "./components/Filters.jsx";

import Login from "./pages/Login.jsx";
import Dashboard from "./pages/Dashboard.jsx";
import IssueIntelligence from "./pages/IssueIntelligence.jsx";
import IssueIntelligence2 from "./pages/IssueIntelligence2.jsx";
import FeedbackAnalytics from "./pages/FeedbackAnalytics.jsx";
import ClosureAnalytics from "./pages/ClosureAnalytics.jsx";
import PredictiveAnalytics from "./pages/PredictiveAnalytics.jsx";
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

function Shell({ title, children }) {
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
    // Fixed file-pipeline default: app runs on processed_data_raw4 (full 11280) without user selection.
    source: "processed_data_raw4",
    wards: [],
    department: null,
    category: null,
    ai_category: null
  };

  const [draftFilters, setDraftFilters] = useState(defaultDraft);
  const [filters, setFilters] = useState(defaultDraft); // applied

  // File-pipeline mode: always run on the configured default dataset source.

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

export default function App() {
  return (
    <>
      <Routes>
        <Route path="/login" element={<Login />} />

        <Route
          path="/"
          element={
            <Protected>
              <Shell title="Executive Overview">
                <Dashboard />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/datasets"
          element={
            <Protected>
              <Shell title="Datasets">
                <Datasets />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/issue-intelligence"
          element={
            <Protected>
              <Shell title="Issue Intelligence">
                <IssueIntelligence />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/issue-intelligence2"
          element={
            <Protected>
              <Shell title="Issue Intelligence 2">
                <IssueIntelligence2 />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/feedback-analytics"
          element={
            <Protected>
              <Shell title="Citizen Feedback Analytics">
                <FeedbackAnalytics />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/closure-analytics"
          element={
            <Protected>
              <Shell title="Closure Time Analytics">
                <ClosureAnalytics />
              </Shell>
            </Protected>
          }
        />
        <Route
          path="/predictive-analytics"
          element={
            <Protected>
              <Shell title="Predictive Analytics">
                <PredictiveAnalytics />
              </Shell>
            </Protected>
          }
        />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </>
  );
}


