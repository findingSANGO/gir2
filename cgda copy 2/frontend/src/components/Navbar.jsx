import { LogOut, Upload } from "lucide-react";

export default function Navbar({ title, user, onLogout, onUpload }) {
  return (
    <header className="sticky top-0 z-10 bg-slateink-50/80 backdrop-blur border-b border-slateink-100">
      <div className="mx-auto max-w-7xl px-4 lg:px-6 py-3 flex items-center justify-between gap-4">
        <div>
          <div className="text-lg font-semibold text-slateink-900">{title}</div>
          <div className="text-xs text-slateink-500">Citizen’s Grievance Data Analytics (CGDA)</div>
        </div>
        <div className="flex items-center gap-2">
          {onUpload ? (
            <button
              onClick={onUpload}
              className="inline-flex items-center gap-2 rounded-lg bg-white px-3 py-2 text-sm font-semibold text-slateink-800 ring-1 ring-slateink-200 hover:ring-slateink-300"
            >
              <Upload className="h-4 w-4" />
              Upload CSV
            </button>
          ) : null}
          <div className="hidden sm:flex items-center gap-2 rounded-lg bg-white px-3 py-2 ring-1 ring-slateink-200">
            <div className="h-8 w-8 rounded-full bg-gov-100 text-gov-700 flex items-center justify-center text-xs font-bold">
              {(user?.username || "U").slice(0, 1).toUpperCase()}
            </div>
            <div className="leading-tight">
              <div className="text-sm font-semibold text-slateink-900">{user?.username}</div>
              <div className="text-xs text-slateink-500 capitalize">{user?.role}</div>
            </div>
          </div>
          <button
            onClick={onLogout}
            className="inline-flex items-center gap-2 rounded-lg bg-slateink-900 px-3 py-2 text-sm font-semibold text-white hover:bg-slateink-800"
          >
            <LogOut className="h-4 w-4" />
            <span className="hidden sm:inline">Logout</span>
          </button>
        </div>
      </div>
    </header>
  );
}


