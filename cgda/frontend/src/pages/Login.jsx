import { useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { Shield } from "lucide-react";
import { api } from "../services/api.js";

export default function Login() {
  const nav = useNavigate();
  const loc = useLocation();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  async function submit(e) {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      const res = await api.login(username, password);
      localStorage.setItem("cgda_token", res.access_token);
      localStorage.setItem("cgda_username", res.username);
      localStorage.setItem("cgda_role", res.role);
      // Default system auto-loads the recommended dataset on first load.
      localStorage.removeItem("cgda_dataset_loaded");
      nav(loc.state?.from || "/", { replace: true });
    } catch (err) {
      setError("Login failed. Please check username/password.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="min-h-screen bg-slateink-50 flex items-center justify-center p-4">
      <div className="w-full max-w-md rounded-2xl bg-white shadow-card ring-1 ring-slateink-100 p-6">
        <div className="flex items-center gap-3">
          <div className="h-11 w-11 rounded-xl bg-gov-600 flex items-center justify-center text-white shadow">
            <Shield className="h-5 w-5" />
          </div>
          <div>
            <div className="text-xl font-semibold text-slateink-900">CGDA Portal</div>
            <div className="text-sm text-slateink-500">Municipal Corporation • Analytics only</div>
          </div>
        </div>

        <div className="mt-5 rounded-xl bg-gov-50 ring-1 ring-gov-100 p-4 text-sm text-slateink-700">
          This system is <span className="font-semibold">read-only</span>. It does not create or modify grievances.
        </div>

        <form onSubmit={submit} className="mt-5 space-y-3">
          <div>
            <label className="text-sm font-semibold text-slateink-700">Username</label>
            <input
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="mt-1 w-full rounded-lg border border-slateink-200 bg-white px-3 py-2 text-sm outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
              placeholder="commissioner / admin"
              autoComplete="username"
              required
            />
          </div>
          <div>
            <label className="text-sm font-semibold text-slateink-700">Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="mt-1 w-full rounded-lg border border-slateink-200 bg-white px-3 py-2 text-sm outline-none focus:border-gov-500 focus:ring-2 focus:ring-gov-100"
              autoComplete="current-password"
              required
            />
          </div>
          {error ? <div className="text-sm text-rose-700">{error}</div> : null}
          <button
            disabled={busy}
            className="w-full rounded-lg bg-slateink-900 py-2.5 text-sm font-semibold text-white hover:bg-slateink-800 disabled:opacity-50"
          >
            {busy ? "Signing in..." : "Sign in"}
          </button>
        </form>

        <div className="mt-4 text-xs text-slateink-500">
          Demo users: <span className="font-semibold">commissioner</span> / commissioner123,{" "}
          <span className="font-semibold">admin</span> / admin123
        </div>
      </div>
    </div>
  );
}


