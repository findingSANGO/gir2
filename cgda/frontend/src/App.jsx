import React, { createContext, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { Navigate, Route, Routes } from "react-router-dom";
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

export const FiltersContext = createContext({ filters: {}, setFilters: () => {} });

function Shell({ title, children, filtersProps = null, minimalChrome = false }) {
  const user = useMemo(() => {
    const a = getAuth();
    return { username: a.username, role: a.role };
  }, []);

  const navbarWrapRef = useRef(null);
  const filtersWrapRef = useRef(null);
  const [chromeHeights, setChromeHeights] = useState({ navbar: 64, filters: 64 });

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

  useLayoutEffect(() => {
    const navbarEl = navbarWrapRef.current;
    const filtersEl = filtersWrapRef.current;
    if (!navbarEl || !filtersEl) return;

    function measure() {
      const navbar = navbarEl.getBoundingClientRect().height || 0;
      const filters = filtersEl.getBoundingClientRect().height || 0;
      setChromeHeights((prev) => {
        // avoid unnecessary renders
        if (Math.abs(prev.navbar - navbar) < 0.5 && Math.abs(prev.filters - filters) < 0.5) return prev;
        return { navbar, filters };
      });
    }

    measure();

    const ro = new ResizeObserver(() => measure());
    ro.observe(navbarEl);
    ro.observe(filtersEl);
    return () => ro.disconnect();
  }, []);

  return (
    <div
      className="min-h-screen bg-slateink-50"
      style={{
        "--cgda-navbar-h": `${chromeHeights.navbar}px`,
        "--cgda-chrome-h": `${chromeHeights.navbar + chromeHeights.filters}px`
      }}
    >
      {!minimalChrome && <Sidebar />}
      <div className={minimalChrome ? "" : "lg:pl-72"}>
        <Navbar
          ref={navbarWrapRef}
          title={title}
          user={user}
          showUserControls={!minimalChrome}
          onLogout={() => {
            clearAuth();
            window.location.href = "/login";
          }}
        />
        <Filters
          ref={filtersWrapRef}
          filters={draftFilters}
          setFilters={setDraftFilters}
          onApply={(next) => setFilters(next || draftFilters)}
          showDataset={false}
          {...(filtersProps || {})}
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
        {/* Auth disabled: keep /login for compatibility but redirect to landing */}
        <Route path="/login" element={<Navigate to="/" replace />} />

        {/* Make Deep Dive the landing page */}
        <Route
          path="/"
          element={
            <Shell title="Deep Dive" filtersProps={{ showCategory: false }} minimalChrome>
              <IssueIntelligence2 />
            </Shell>
          }
        />

        {/* Keep Executive Overview accessible (hidden from landing) */}
        <Route
          path="/executive"
          element={
            <Shell title="Executive Overview" minimalChrome>
              <Dashboard />
            </Shell>
          }
        />
        <Route
          path="/datasets"
          element={
            <Shell title="Datasets" minimalChrome>
              <Datasets />
            </Shell>
          }
        />
        <Route
          path="/issue-intelligence"
          element={
            <Shell title="Issue Intelligence" minimalChrome>
              <IssueIntelligence />
            </Shell>
          }
        />
        <Route
          path="/issue-intelligence2"
          element={
            <Shell title="Deep Dive" filtersProps={{ showCategory: false }} minimalChrome>
              <IssueIntelligence2 />
            </Shell>
          }
        />
        <Route
          path="/feedback-analytics"
          element={
            <Shell title="Citizen Feedback Analytics" minimalChrome>
              <FeedbackAnalytics />
            </Shell>
          }
        />
        <Route
          path="/closure-analytics"
          element={
            <Shell title="Closure Time Analytics" minimalChrome>
              <ClosureAnalytics />
            </Shell>
          }
        />
        <Route
          path="/predictive-analytics"
          element={
            <Shell title="Predictive Analytics" minimalChrome>
              <PredictiveAnalytics />
            </Shell>
          }
        />
        <Route path="*" element={<Navigate to="/issue-intelligence2" replace />} />
      </Routes>
    </>
  );
}


