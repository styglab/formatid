if (!window.React || !window.ReactDOM) {
  document.getElementById("root").innerHTML =
    '<div class="app"><main class="main"><div class="error">React runtime could not be loaded.</div></main></div>';
  throw new Error("React runtime could not be loaded");
}

const { useEffect, useMemo, useState } = React;

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json();
}

function App() {
  const [summary, setSummary] = useState(null);
  const [apps, setApps] = useState([]);
  const [activeView, setActiveView] = useState("platform");
  const [refreshMs, setRefreshMs] = useState(10000);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  async function load() {
    setLoading(true);
    setError(null);
    try {
      const [summaryRes, appsRes] = await Promise.all([
        fetchJson("/api/dashboard/summary"),
        fetchJson("/api/dashboard/apps"),
      ]);
      const nextApps = appsRes.apps || [];
      setSummary(summaryRes);
      setApps(nextApps);
      setActiveView((current) => current || "platform");
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, []);

  useEffect(() => {
    if (!refreshMs) return undefined;
    const id = setInterval(load, refreshMs);
    return () => clearInterval(id);
  }, [refreshMs]);

  const currentApp = useMemo(
    () => apps.find((app) => app.app === activeView) || apps[0] || null,
    [apps, activeView]
  );
  const isPlatformView = activeView === "platform";

  return React.createElement("div", { className: "app" },
    React.createElement("header", { className: "topbar" },
      React.createElement("div", { className: "title" },
        React.createElement("h1", null, "Platform Dashboard"),
        React.createElement("div", { className: "subtitle" }, formatDateTime(summary?.evaluated_at) || "Loading")
      ),
      React.createElement("div", { className: "toolbar" },
        React.createElement("select", {
          className: "select",
          value: String(refreshMs),
          onChange: (event) => setRefreshMs(Number(event.target.value)),
          "aria-label": "Refresh interval",
        },
          React.createElement("option", { value: "0" }, "Auto refresh off"),
          React.createElement("option", { value: "10000" }, "Refresh 10s"),
          React.createElement("option", { value: "30000" }, "Refresh 30s"),
          React.createElement("option", { value: "60000" }, "Refresh 60s")
        ),
        React.createElement("button", { className: "btn", onClick: load, disabled: loading }, loading ? "Refreshing" : "Refresh")
      )
    ),
    React.createElement("div", { className: "shell" },
      React.createElement(Sidebar, { apps, activeView, onSelect: setActiveView }),
      React.createElement("main", { className: "main grid" },
        error && React.createElement("div", { className: "error" }, error),
        isPlatformView
          ? React.createElement(PlatformView, { summary, apps })
          : React.createElement(AppView, { app: currentApp })
      )
    )
  );
}

function Sidebar({ apps, activeView, onSelect }) {
  return React.createElement("aside", { className: "sidebar" },
    React.createElement("div", { className: "side-brand" },
      React.createElement("div", { className: "brand-mark" }, "F"),
      React.createElement("div", null,
        React.createElement("div", { className: "brand-name" }, "formatid"),
        React.createElement("div", { className: "brand-caption" }, "AI-ready data platform")
      )
    ),
    React.createElement("button", {
      className: `nav-item ${activeView === "platform" ? "active" : ""}`,
      onClick: () => onSelect("platform"),
    },
      React.createElement("span", { className: "nav-icon" }, "P"),
      React.createElement("span", { className: "nav-label" }, "Platform"),
      React.createElement("span", { className: "nav-status healthy" })
    ),
    React.createElement("div", { className: "side-section" }, "Apps"),
    apps.length === 0
      ? React.createElement("div", { className: "side-empty" }, "No app dashboards")
      : apps.map((app) =>
          React.createElement("button", {
            className: `nav-item ${activeView === app.app ? "active" : ""}`,
            key: app.app,
            onClick: () => onSelect(app.app),
          },
            React.createElement("span", { className: "nav-icon" }, "A"),
            React.createElement("span", { className: "nav-label" }, app.title || app.app),
            React.createElement("span", { className: `nav-status ${app.status || "unknown"}` })
          )
        )
  );
}

function PlatformView({ summary, apps }) {
  const services = summary?.service_health || [];
  const platformServices = services.filter((service) => service.scope !== "app");
  const appServices = services.filter((service) => service.scope === "app");
  const platformHealthy = platformServices.filter((service) => service.status === "healthy").length;
  const appHealthy = appServices.filter((service) => service.status === "healthy").length;

  return React.createElement(DashboardPage, {
    leftKpis: [
      { label: "Platform Services", value: platformServices.length },
      { label: "Platform Healthy", value: platformHealthy, detail: `${platformServices.length - platformHealthy} unhealthy` },
    ],
    rightKpis: [
      { label: "App Services", value: appServices.length },
      { label: "App Healthy", value: appHealthy, detail: `${appServices.length - appHealthy} unhealthy` },
    ],
    leftCard: React.createElement(ServiceHealthCard, { title: "Platform Services", services: platformServices }),
    rightCard: React.createElement(ServiceHealthCard, { title: "App Services", services: appServices }),
  });
}

function AppView({ app }) {
  const dataTableSection = findSection(app, "Data Tables");
  const pipelineSection = findSection(app, "Pipeline Runs");
  const metrics = Array.isArray(app?.metrics) ? app.metrics : [];

  return React.createElement(React.Fragment, null,
    app?.error && React.createElement("div", { className: "error" }, app.error),
    React.createElement(DashboardPage, {
      leftKpis: metrics.slice(0, 2),
      rightKpis: metrics.slice(2, 4),
      leftCard: React.createElement(DataTablesCard, { section: dataTableSection }),
      rightCard: React.createElement(PipelineRunsCard, { section: pipelineSection }),
    })
  );
}

function DashboardPage({ leftKpis, rightKpis, leftCard, rightCard }) {
  return React.createElement("section", { className: "dashboard-page" },
    React.createElement(DashboardKpis, { leftKpis, rightKpis }),
    React.createElement("section", { className: "grid dashboard-layout" }, leftCard, rightCard)
  );
}

function DashboardKpis({ leftKpis, rightKpis }) {
  return React.createElement("section", { className: "grid paired-kpis" },
    React.createElement("div", { className: "grid kpi-pair" },
      leftKpis.map((metric) =>
        React.createElement(Kpi, {
          key: metric.label,
          label: metric.label,
          value: metric.value,
          detail: metric.detail,
        })
      )
    ),
    React.createElement("div", { className: "grid kpi-pair" },
      rightKpis.map((metric) =>
        React.createElement(Kpi, {
          key: metric.label,
          label: metric.label,
          value: metric.value,
          detail: metric.detail,
        })
      )
    )
  );
}

function DataTablesCard({ section }) {
  const rows = Array.isArray(section?.rows) ? section.rows : [];
  return React.createElement(Card, { title: "Data Tables" },
    rows.length === 0
      ? React.createElement("div", { className: "muted" }, "No table stats")
      : React.createElement("div", { className: "table-wrap" },
          React.createElement("table", { className: "dense-table" },
            React.createElement("thead", null,
              React.createElement("tr", null, ["Table", "Rows", "Freshness"].map((text) => React.createElement("th", { key: text }, text)))
            ),
            React.createElement("tbody", null,
              rows.map((row) =>
                React.createElement("tr", { key: row.name },
                  React.createElement("td", { className: "wrap" }, row.name),
                  React.createElement("td", null, formatValue(row.value)),
                  React.createElement("td", { className: "muted" }, formatDateTime(row.freshness))
                )
              )
            )
          )
        )
  );
}

function PipelineRunsCard({ section }) {
  const rows = Array.isArray(section?.rows) ? section.rows : [];
  return React.createElement(Card, { title: "Pipeline Runs" },
    rows.length === 0
      ? React.createElement("div", { className: "muted" }, "No pipeline runs")
      : React.createElement("div", { className: "table-wrap" },
          React.createElement("table", { className: "dense-table" },
            React.createElement("thead", null,
              React.createElement("tr", null, ["Pipeline", "Status", "Last run"].map((text) => React.createElement("th", { key: text }, text)))
            ),
            React.createElement("tbody", null,
              rows.map((row) =>
                React.createElement("tr", { key: row.name },
                  React.createElement("td", { className: "wrap" }, row.name),
                  React.createElement("td", null, React.createElement("span", { className: `badge ${String(row.value || "unknown").toLowerCase()}` }, row.value || "unknown")),
                  React.createElement("td", { className: "muted" }, formatDateTime(row.last_run_at))
                )
              )
            )
          )
        )
  );
}

function ServiceHealthCard({ title, services }) {
  return React.createElement(Card, { title },
    services.length === 0
      ? React.createElement("div", { className: "muted" }, "No service health checks")
      : React.createElement("div", { className: "table-wrap" },
          React.createElement("table", { className: "dense-table" },
            React.createElement("thead", null,
              React.createElement("tr", null, ["Service", "Status", "Address", "Role"].map((text) => React.createElement("th", { key: text }, text)))
            ),
            React.createElement("tbody", null,
              services.map((service) =>
                React.createElement("tr", { key: service.service },
                  React.createElement("td", { className: "wrap" }, service.service),
                  React.createElement("td", null, React.createElement("span", { className: `badge ${service.status || "unknown"}` }, service.status || "unknown")),
                  React.createElement("td", { className: "muted wrap" }, service.address || service.detail || "-"),
                  React.createElement("td", { className: "muted wrap" }, service.role || service.kind || "-")
                )
              )
            )
          )
        )
  );
}

function Kpi({ label, value, badge, detail }) {
  return React.createElement("div", { className: "card kpi card-body" },
    React.createElement("div", { className: "kpi-label" }, label),
    React.createElement("div", { className: "kpi-value" }, formatValue(value)),
    badge && React.createElement("div", { className: "kpi-detail" }, React.createElement("span", { className: `badge ${badge}` }, badge)),
    detail && React.createElement("div", { className: "kpi-detail" }, formatDisplayValue(detail))
  );
}

function Card({ title, action, children, className }) {
  return React.createElement("div", { className: className ? `card ${className}` : "card" },
    React.createElement("div", { className: "card-head" },
      React.createElement("div", { className: "card-title" }, title),
      action
    ),
    React.createElement("div", { className: "card-body" }, children)
  );
}

function formatValue(value) {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "number") return value.toLocaleString();
  return String(value);
}

function formatDisplayValue(value) {
  if (looksLikeIsoDate(value)) return formatDateTime(value);
  return formatValue(value);
}

function findSection(app, title) {
  const sections = Array.isArray(app?.sections) ? app.sections : [];
  return sections.find((section) => section.title === title) || null;
}

function formatDateTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString("ko-KR", {
    timeZone: "Asia/Seoul",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }) + " KST";
}

function shortTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleTimeString("ko-KR", {
    timeZone: "Asia/Seoul",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }) + " KST";
}

function looksLikeIsoDate(value) {
  return typeof value === "string" && /^\d{4}-\d{2}-\d{2}T/.test(value);
}

ReactDOM.createRoot(document.getElementById("root")).render(React.createElement(App));
