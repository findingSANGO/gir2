import { LogOut } from "lucide-react";
import Button from "./ui/Button.jsx";

export default function Navbar({ title, user, onLogout }) {
  return (
    <header className="sticky top-0 z-10 bg-slateink-50/80 backdrop-blur border-b border-slateink-100">
      <div className="mx-auto max-w-7xl px-4 lg:px-6 py-3 flex items-center justify-between gap-4">
        <div>
          <div className="text-2xl font-semibold text-slateink-900 leading-tight">{title}</div>
          <div className="text-sm text-slateink-500">Citizen’s Grievance Data Analytics (CGDA)</div>
        </div>
        <div className="flex items-center gap-2">
          <div className="hidden sm:flex items-center gap-2 rounded-lg bg-white px-3 py-2 ring-1 ring-slateink-200">
            <div className="h-8 w-8 rounded-full bg-gov-100 text-gov-700 flex items-center justify-center text-xs font-bold">
              {(user?.username || "U").slice(0, 1).toUpperCase()}
            </div>
            <div className="leading-tight">
              <div className="text-sm font-semibold text-slateink-900">{user?.username}</div>
              <div className="text-xs text-slateink-500 capitalize">{user?.role}</div>
            </div>
          </div>
          <Button
            onClick={onLogout}
            variant="dark"
            size="md"
          >
            <LogOut className="h-4 w-4" />
            <span className="hidden sm:inline">Logout</span>
          </Button>
        </div>
      </div>
    </header>
  );
}


