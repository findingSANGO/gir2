import { NavLink } from "react-router-dom";
import { Activity, BarChart3, Brain, Clock, Database, LayoutDashboard, Shield, Star } from "lucide-react";
import clsx from "clsx";

const items = [
  { to: "/", label: "Executive Overview", icon: LayoutDashboard },
  { to: "/datasets", label: "Datasets", icon: Database },
  { to: "/issue-intelligence", label: "Issue Intelligence", icon: BarChart3 },
  { to: "/issue-intelligence2", label: "Issue Intelligence 2", icon: BarChart3 },
  { to: "/feedback-analytics", label: "Citizen Feedback", icon: Star },
  { to: "/closure-analytics", label: "Closure Analytics", icon: Clock },
  { to: "/predictive-analytics", label: "Predictive Analytics", icon: Brain }
];

export default function Sidebar() {
  const role = localStorage.getItem("cgda_role") || "";
  return (
    <aside className="hidden lg:flex lg:w-72 lg:flex-col lg:fixed lg:inset-y-0">
      <div className="flex flex-col gap-4 h-full bg-slateink-900 text-slateink-50 px-4 py-5">
        <div className="flex items-center gap-3 px-2">
          <div className="h-10 w-10 rounded-lg bg-gov-600 flex items-center justify-center shadow">
            <Shield className="h-5 w-5" />
          </div>
          <div>
            <div className="text-sm font-semibold leading-tight">CGDA Portal</div>
            <div className="text-xs text-slateink-300">Municipal Analytics</div>
          </div>
        </div>

        <nav className="mt-2 flex-1">
          <div className="text-xs uppercase tracking-wide text-slateink-400 px-2 mb-2">Dashboards</div>
          <div className="flex flex-col gap-1">
            {items
              .filter((it) => !it.roles || it.roles.includes(role))
              .map((it) => {
              const Icon = it.icon;
              return (
                <NavLink
                  key={it.to}
                  to={it.to}
                  className={({ isActive }) =>
                    clsx(
                      "flex items-center gap-3 rounded-lg px-3 py-2 text-sm transition",
                      isActive ? "bg-slateink-800 text-white" : "text-slateink-200 hover:bg-slateink-800/70"
                    )
                  }
                >
                  <Icon className="h-4 w-4 opacity-90" />
                  <span>{it.label}</span>
                </NavLink>
              );
            })}
          </div>
        </nav>

        <div className="rounded-lg bg-slateink-800/60 p-3 text-xs text-slateink-200">
          <div className="flex items-center gap-2">
            <Activity className="h-4 w-4" />
            <span>Read-only analytics</span>
          </div>
          <div className="mt-1 text-slateink-300">Does not manage grievances or workflows.</div>
        </div>
      </div>
    </aside>
  );
}


