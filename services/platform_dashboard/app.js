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
  const [logs, setLogs] = useState([]);
  const [selectedApp, setSelectedApp] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  async function load() {
    setLoading(true);
    setError(null);
    try {
      const [summaryRes, appsRes, logsRes] = await Promise.all([
        fetchJson("/api/dashboard/summary"),
        fetchJson("/api/dashboard/apps"),
        fetchJson("/api/logs/services"),
      ]);
      const nextApps = appsRes.apps || [];
      setSummary(summaryRes);
      setApps(nextApps);
      setLogs(logsRes.sources || []);
      setSelectedApp((current) => current || nextApps[0]?.app || null);
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    const id = setInterval(load, 10000);
    return () => clearInterval(id);
  }, []);

  const currentApp = useMemo(
    () => apps.find((app) => app.app === selectedApp) || apps[0] || null,
    [apps, selectedApp]
  );

  return React.createElement("div", { className: "app" },
    React.createElement("header", { className: "topbar" },
      React.createElement("div", { className: "title" },
        React.createElement("h1", null, "Platform Dashboard"),
        React.createElement("div", { className: "subtitle" }, formatDateTime(summary?.evaluated_at) || "Loading")
      ),
      React.createElement("div", { className: "toolbar" },
        React.createElement("button", { className: "btn", onClick: load, disabled: loading }, loading ? "Refreshing" : "Refresh")
      )
    ),
    React.createElement("div", { className: "shell" },
      React.createElement(Sidebar, { apps, selectedApp: currentApp?.app, onSelect: setSelectedApp }),
      React.createElement("main", { className: "main grid" },
        error && React.createElement("div", { className: "error" }, error),
        React.createElement(PlatformOverview, { summary, apps }),
        React.createElement("section", { className: "grid app-overview" },
          React.createElement(AppSummaryCard, { app: currentApp }),
          React.createElement(LogSourcesCard, { sources: logs })
        ),
        React.createElement("section", { className: "grid two-col" },
          React.createElement(ServiceRunsCard, { serviceRuns: summary?.service_runs || [] }),
          React.createElement(AppSectionsCard, { app: currentApp })
        )
      )
    )
  );
}

function Sidebar({ apps, selectedApp, onSelect }) {
  return React.createElement("aside", { className: "sidebar" },
    React.createElement("div", { className: "side-brand" },
      React.createElement("div", { className: "brand-mark" }, "F"),
      React.createElement("div", null,
        React.createElement("div", { className: "brand-name" }, "formatid"),
        React.createElement("div", { className: "brand-caption" }, "AI-ready data platform")
      )
    ),
    React.createElement("div", { className: "side-section" }, "Apps"),
    apps.length === 0
      ? React.createElement("div", { className: "side-empty" }, "No app dashboards")
      : apps.map((app) =>
          React.createElement("button", {
            className: `nav-item ${selectedApp === app.app ? "active" : ""}`,
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

function PlatformOverview({ summary, apps }) {
  const appServices = summary?.app_services || {};
  return React.createElement("section", { className: "grid runtime-kpis" },
    React.createElement(Kpi, { label: "Platform", value: summary?.health?.status || "unknown", badge: summary?.health?.status || "unknown" }),
    React.createElement(Kpi, { label: "Redis", value: summary?.health?.redis?.ok ? "ok" : "down", badge: summary?.health?.redis?.ok ? "healthy" : "failed" }),
    React.createElement(Kpi, { label: "App Heartbeats", value: appServices.service_count ?? 0, badge: appServices.status || "unknown" }),
    React.createElement(Kpi, { label: "Dashboards", value: apps.length, badge: apps.length ? "healthy" : "unknown" }),
    React.createElement(Kpi, { label: "Service Runs", value: summary?.service_runs?.length ?? 0, badge: "queued" }),
    React.createElement(Kpi, { label: "Updated", value: shortTime(summary?.evaluated_at), detail: summary?.evaluated_at || "-" })
  );
}

function AppSummaryCard({ app }) {
  if (!app) {
    return React.createElement(Card, { title: "App Summary" },
      React.createElement("div", { className: "muted" }, "No app dashboard registered")
    );
  }
  const metrics = Array.isArray(app.metrics) ? app.metrics : [];
  return React.createElement(Card, {
    title: app.title || app.app,
    action: React.createElement("span", { className: `badge ${app.status || "unknown"}` }, app.status || "unknown"),
  },
    app.description && React.createElement("div", { className: "muted card-intro" }, app.description),
    app.error && React.createElement("div", { className: "error" }, app.error),
    metrics.length === 0
      ? React.createElement("div", { className: "muted" }, "No metrics")
      : React.createElement("div", { className: "grid app-metrics" },
          metrics.map((metric) =>
            React.createElement("div", { className: "metric-tile", key: metric.label },
              React.createElement("div", { className: "kpi-label" }, metric.label),
              React.createElement("div", { className: "kpi-value" }, formatValue(metric.value)),
              metric.detail && React.createElement("div", { className: "kpi-detail" }, formatDateTime(metric.detail))
            )
          )
        )
  );
}

function AppSectionsCard({ app }) {
  const sections = Array.isArray(app?.sections) ? app.sections : [];
  return React.createElement(Card, { title: "App Sections" },
    sections.length === 0
      ? React.createElement("div", { className: "muted" }, "No sections")
      : React.createElement("div", { className: "section-stack" },
          sections.map((section) =>
            React.createElement("section", { className: "summary-section", key: section.title },
              React.createElement("h3", null, section.title),
              React.createElement("div", { className: "metric-stack" },
                (section.rows || []).map((row) =>
                  React.createElement("div", { className: "metric-line", key: row.name },
                    React.createElement("span", null, row.name),
                    React.createElement("strong", null, formatDisplayValue(row.value))
                  )
                )
              )
            )
          )
        )
  );
}

function ServiceRunsCard({ serviceRuns }) {
  return React.createElement(Card, { title: "Service Runs" },
    serviceRuns.length === 0
      ? React.createElement("div", { className: "muted" }, "No service runs")
      : React.createElement("div", { className: "table-wrap" },
          React.createElement("table", { className: "dense-table" },
            React.createElement("thead", null,
              React.createElement("tr", null, ["Name", "Status", "Last run", "Duration"].map((text) => React.createElement("th", { key: text }, text)))
            ),
            React.createElement("tbody", null,
              serviceRuns.map((run) =>
                React.createElement("tr", { key: run.name },
                  React.createElement("td", { className: "wrap" }, run.name),
                  React.createElement("td", null, React.createElement("span", { className: `badge ${run.last_run?.status || "unknown"}` }, run.last_run?.status || "unknown")),
                  React.createElement("td", { className: "muted" }, formatDateTime(run.last_run?.created_at)),
                  React.createElement("td", { className: "muted" }, run.last_run?.duration_ms == null ? "-" : `${Math.round(run.last_run.duration_ms)} ms`)
                )
              )
            )
          )
        )
  );
}

function LogSourcesCard({ sources }) {
  return React.createElement(Card, { title: "Log Sources" },
    sources.length === 0
      ? React.createElement("div", { className: "muted" }, "No log sources")
      : React.createElement("div", { className: "health-list" },
          sources.map((source) =>
            React.createElement("div", { className: "health-row", key: source.service_name },
              React.createElement("span", { className: "health-name" }, source.service_name),
              React.createElement("span", { className: `badge ${source.status || "unknown"}` }, source.status || "logged"),
              React.createElement("span", { className: "muted" }, formatDateTime(source.last_seen_at))
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

function Card({ title, action, children }) {
  return React.createElement("div", { className: "card" },
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
