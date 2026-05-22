import { Activity, BriefcaseBusiness, Database, FlaskConical, Search } from "lucide-react";
import { NavLink, Outlet } from "react-router-dom";

const navItems = [
  { to: "/dashboard", label: "Dashboard", icon: Activity },
  { to: "/screeners", label: "Screeners", icon: FlaskConical },
  { to: "/research", label: "Research", icon: Search },
  { to: "/jobs", label: "Jobs", icon: Database },
];

export function AppShell() {
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <BriefcaseBusiness size={22} />
          <div>
            <strong>Trade Research</strong>
            <span>Market agent</span>
          </div>
        </div>
        <nav className="nav-list">
          {navItems.map((item) => (
            <NavLink key={item.to} to={item.to} className="nav-link">
              <item.icon size={18} />
              <span>{item.label}</span>
            </NavLink>
          ))}
        </nav>
      </aside>
      <main className="main">
        <Outlet />
      </main>
    </div>
  );
}
