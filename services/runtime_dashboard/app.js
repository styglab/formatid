if (!window.React || !window.ReactDOM) {
  document.getElementById("root").innerHTML =
    '<div class="app"><main class="main"><div class="error">React runtime could not be loaded. Check network access to unpkg.com or vendor React into the dashboard image.</div></main></div>';
  throw new Error("React runtime could not be loaded");
}

const { useEffect, useState } = React;

const api = {
  summary: "/api/dashboard/summary",
  trends: "/api/dashboard/task-trends?hours=24",
  durations: "/api/dashboard/task-duration-stats?hours=24",
  apps: "/api/dashboard/apps",
  logSources: "/api/logs/services",
};

function App() {
  const [activeTab, setActiveTab] = useState("runtime");
  const [summary, setSummary] = useState(null);
  const [trends, setTrends] = useState(null);
  const [durations, setDurations] = useState(null);
  const [apps, setApps] = useState([]);
  const [logSources, setLogSources] = useState([]);
  const [logs, setLogs] = useState([]);
  const [logLevel, setLogLevel] = useState("");
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [refreshSeconds, setRefreshSeconds] = useState(10);

  async function load() {
    setLoading(true);
    setError(null);
    try {
      const [summaryRes, trendsRes, durationsRes, appsRes, logSourcesRes] = await Promise.all([
        fetchJson(api.summary),
        fetchJson(api.trends),
        fetchJson(api.durations),
        fetchJson(api.apps),
        fetchJson(api.logSources),
      ]);
      setSummary(summaryRes);
      setTrends(trendsRes);
      setDurations(durationsRes);
      setApps(appsRes.apps || []);
      setLogSources(logSourcesRes.sources || []);
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
    if (!autoRefresh) return undefined;
    const id = setInterval(() => {
      load();
      if (isLogTab(activeTab)) loadLogs();
    }, refreshSeconds * 1000);
    return () => clearInterval(id);
  }, [autoRefresh, refreshSeconds, activeTab, logLevel]);

  useEffect(() => {
    if (!isLogTab(activeTab)) return undefined;
    loadLogs();
    return undefined;
  }, [activeTab, logLevel]);

  async function loadLogs() {
    const source = parseLogTab(activeTab);
    if (!source) return;
    try {
      const params = new URLSearchParams({ service_name: source.serviceName, limit: "300" });
      if (source.workerId) params.set("worker_id", source.workerId);
      if (logLevel) params.set("level", logLevel);
      const response = await fetchJson(`/api/logs?${params.toString()}`);
      setLogs(response.logs || []);
    } catch (err) {
      setError(err.message || String(err));
    }
  }

  const selectedApp = apps.find((item) => item.app === activeTab);
  const selectedLogSource = parseLogTab(activeTab);

  return React.createElement(
    "div",
    { className: "app" },
    React.createElement(Header, {
      loading,
      evaluatedAt: summary?.evaluated_at,
      autoRefresh,
      setAutoRefresh,
      refreshSeconds,
      setRefreshSeconds,
      onRefresh: load,
    }),
    React.createElement(
      "div",
      { className: "shell" },
      React.createElement(Sidebar, { activeTab, setActiveTab, apps, logSources }),
      React.createElement(
        "main",
        { className: "main grid" },
        error && React.createElement("div", { className: "error" }, error),
        selectedLogSource
          ? React.createElement(LogsView, {
              source: selectedLogSource,
              logs,
              logLevel,
              setLogLevel,
              onRefresh: loadLogs,
            })
          : activeTab === "runtime"
          ? React.createElement(RuntimeView, { summary, trends, durations })
          : React.createElement(AppView, { appInfo: selectedApp })
      )
    )
  );
}

function Sidebar({ activeTab, setActiveTab, apps, logSources }) {
  return React.createElement(
    "aside",
    { className: "sidebar", "aria-label": "Dashboard navigation" },
    React.createElement(
      "div",
      { className: "side-brand" },
      React.createElement("div", { className: "brand-mark" }, "W"),
      React.createElement("div", null,
        React.createElement("div", { className: "brand-name" }, "Worker Runtime"),
        React.createElement("div", { className: "brand-caption" }, "Operations")
      )
    ),
    React.createElement(
      "button",
      {
        className: `nav-item ${activeTab === "runtime" ? "active" : ""}`,
        onClick: () => setActiveTab("runtime"),
      },
      React.createElement("span", { className: "nav-icon" }, "R"),
      React.createElement("span", null, "Runtime")
    ),
    apps.map((appInfo) =>
      React.createElement(
        "button",
        {
          key: appInfo.app,
          className: `nav-item ${activeTab === appInfo.app ? "active" : ""}`,
          onClick: () => setActiveTab(appInfo.app),
        },
        React.createElement("span", { className: "nav-icon" }, appInfo.app.slice(0, 1).toUpperCase()),
        React.createElement("span", null, appInfo.app),
        appInfo.error && React.createElement("span", { className: "nav-dot", title: appInfo.error })
      )
    ),
    React.createElement("div", { className: "side-section" }, "Logs"),
    logSources.length === 0
      ? React.createElement("div", { className: "side-empty" }, "No active services")
      : logSources.map((source) => {
          const tab = buildLogTab(source);
          const label = source.worker_id ? `${source.service_name} / ${source.queue_name || "worker"}` : source.service_name;
          return React.createElement(
            "button",
            {
              key: tab,
              className: `nav-item log-nav ${activeTab === tab ? "active" : ""}`,
              onClick: () => setActiveTab(tab),
            },
            React.createElement("span", { className: "nav-icon" }, source.source_type === "worker" ? "W" : "S"),
            React.createElement("span", { className: "nav-label" }, label),
            source.status && React.createElement("span", { className: `nav-status ${source.status}` })
          );
        })
  );
}

function RuntimeView({ summary, durations }) {
  return React.createElement(
    React.Fragment,
    null,
    React.createElement(RuntimeKpiGrid, { summary }),
    React.createElement(
      "section",
      { className: "grid two-col" },
      React.createElement(ServiceRunsCard, { serviceRuns: summary?.service_runs || [] }),
      React.createElement(QueuesCard, { queues: summary?.queues || {}, workers: summary?.workers })
    ),
    React.createElement(
      "section",
      { className: "grid two-col" },
      React.createElement(TaskStatusCard, { counts: summary?.tasks?.last_24h || {} }),
      React.createElement(RecentFailuresCard, { failures: summary?.tasks?.recent_failures || [] })
    ),
    React.createElement(
      "section",
      { className: "grid two-col" },
      React.createElement(DurationCard, { durations: durations?.duration_stats || [] }),
      React.createElement(RuntimeHealthCard, { summary })
    ),
    React.createElement(
      "section",
      { className: "grid" },
      React.createElement(RecentTasksCard, { tasks: summary?.tasks?.recent || [] })
    )
  );
}

function AppView({ appInfo }) {
  if (!appInfo) {
    return React.createElement(Card, { title: "App" }, React.createElement("div", { className: "muted" }, "No app dashboard selected"));
  }
  const counts = appInfo.counts || {};
  const states = appInfo.task_states || {};
  return React.createElement(
    React.Fragment,
    null,
    React.createElement(
      "section",
      { className: "grid kpis app-kpis" },
      React.createElement(Kpi, { label: "App", value: appInfo.app, badge: appInfo.error ? "failed" : "healthy", detail: appInfo.error || "registered" }),
      React.createElement(Kpi, { label: "Quota", value: appInfo.quota_block ? "blocked" : "clear", badge: appInfo.quota_block ? "blocked" : "healthy", detail: appInfo.quota_block?.blocked_until || "No quota block" }),
      React.createElement(Kpi, { label: "Open issues", value: sumObjectValues(states, ["failed", "blocked", "dead_lettered"]), badge: sumObjectValues(states, ["failed", "blocked", "dead_lettered"]) > 0 ? "failed" : "healthy", detail: "failed + blocked + DLQ" }),
      React.createElement(Kpi, { label: "Retry due", value: appInfo.retry_due?.due_now ?? 0, badge: (appInfo.retry_due?.due_now || 0) > 0 ? "retrying" : "healthy", detail: `${appInfo.retry_due?.future || 0} due later` })
    ),
    React.createElement(
      "section",
      { className: "grid two-col" },
      React.createElement(ObjectBarsCard, { title: "Tables", values: counts }),
      React.createElement(ObjectBarsCard, { title: "Task States", values: states })
    ),
    React.createElement(
      "section",
      { className: "grid two-col" },
      React.createElement(KeyValueCard, { title: "Backfill", value: appInfo.backfill || {} }),
      React.createElement(KeyValueCard, { title: "Retry Control", value: appInfo.retry_due || {} })
    ),
    React.createElement(
      "section",
      { className: "grid" },
      React.createElement(GenericRowsCard, { title: "Oldest Open Tasks", rows: appInfo.oldest_open_tasks || [] })
    ),
    React.createElement(
      "section",
      { className: "grid" },
      React.createElement(GenericRowsCard, { title: "Recent Transactions", rows: appInfo.recent_tasks || [] })
    )
  );
}

function Header(props) {
  return React.createElement(
    "header",
    { className: "topbar" },
    React.createElement(
      "div",
      { className: "title" },
      React.createElement("h1", null, "Worker Runtime Dashboard"),
      React.createElement("div", { className: "subtitle" }, `Last refreshed: ${formatDate(props.evaluatedAt)}`)
    ),
    React.createElement(
      "div",
      { className: "toolbar" },
      React.createElement(
        "select",
        {
          className: "select",
          value: props.refreshSeconds,
          onChange: (event) => props.setRefreshSeconds(Number(event.target.value)),
        },
        [5, 10, 30, 60].map((seconds) =>
          React.createElement("option", { key: seconds, value: seconds }, `${seconds}s`)
        )
      ),
      React.createElement(
        "button",
        { className: "btn", onClick: () => props.setAutoRefresh(!props.autoRefresh) },
        props.autoRefresh ? "Auto refresh on" : "Auto refresh off"
      ),
      React.createElement(
        "button",
        { className: "btn", disabled: props.loading, onClick: props.onRefresh },
        props.loading ? "Refreshing" : "Refresh"
      )
    )
  );
}

function RuntimeKpiGrid({ summary }) {
  const health = summary?.health?.status || "unknown";
  const appServices = summary?.app_services?.status || "unknown";
  const queueSize = sumValues(summary?.queues || {}, "size");
  const dlqSize = sumValues(summary?.queues || {}, "dlq_size");
  const oldestQueueAge = maxValues(summary?.queues || {}, "oldest_age_seconds");
  const failures = (summary?.tasks?.last_24h?.failed || 0) + (summary?.tasks?.last_24h?.dead_lettered || 0);

  return React.createElement(
    "section",
    { className: "grid kpis runtime-kpis" },
    React.createElement(Kpi, { label: "System", value: health, badge: health, detail: "API, Redis, workers, services" }),
    React.createElement(Kpi, { label: "Services", value: appServices, badge: appServices, detail: `${summary?.app_services?.service_count || 0} heartbeat(s)` }),
    React.createElement(Kpi, { label: "Queue size", value: queueSize, detail: "All worker queues" }),
    React.createElement(Kpi, { label: "Oldest queued", value: formatAge(oldestQueueAge), badge: oldestQueueAge > 300 ? "retrying" : "healthy", detail: "Max queue age" }),
    React.createElement(Kpi, { label: "DLQ size", value: dlqSize, badge: dlqSize > 0 ? "failed" : "healthy", detail: "Dead-letter backlog" }),
    React.createElement(Kpi, { label: "Runtime failures 24h", value: failures, badge: failures > 0 ? "failed" : "healthy", detail: "execution failed + dead_lettered" })
  );
}

function Kpi({ label, value, detail, badge }) {
  return React.createElement(
    "div",
    { className: "card kpi" },
    React.createElement(
      "div",
      { className: "card-body" },
      React.createElement("div", { className: "kpi-label" }, label),
      React.createElement(
        "div",
        { className: "kpi-value" },
        badge ? React.createElement(Badge, { value, status: badge }) : String(value ?? "-")
      ),
      React.createElement("div", { className: "kpi-detail" }, detail || "")
    )
  );
}

function ServiceRunsCard({ serviceRuns }) {
  return React.createElement(
    Card,
    { title: "Service Runs" },
    React.createElement(
      "div",
      { className: "table-wrap" },
      React.createElement(
        "table",
        null,
        React.createElement("thead", null, React.createElement("tr", null,
          ["Name", "Status", "Updated", "Task", "Skip"].map((text) => React.createElement("th", { key: text }, text))
        )),
        React.createElement(
          "tbody",
          null,
          serviceRuns.map((item) =>
            React.createElement("tr", { key: item.name },
              React.createElement("td", null, item.name),
              React.createElement("td", null, React.createElement(Badge, { value: item.last_run?.status || "none", status: item.last_run?.status || "unknown" })),
              React.createElement("td", null, formatDate(item.last_run?.created_at)),
              React.createElement("td", null, item.task_name),
              React.createElement("td", { className: "wrap muted" }, item.last_run?.skip_reason || "")
            )
          )
        )
      )
    )
  );
}

function QueuesCard({ queues, workers }) {
  const rows = Object.entries(queues);
  return React.createElement(
    Card,
    { title: "Queues" },
    React.createElement(
      "div",
      { className: "table-wrap" },
      React.createElement("table", null,
        React.createElement("thead", null, React.createElement("tr", null,
          ["Queue", "Size", "Oldest", "DLQ", "DLQ oldest", "Workers", "Status", "Pause"].map((text) => React.createElement("th", { key: text }, text))
        )),
        React.createElement("tbody", null,
          rows.map(([name, details]) => {
            const workerDetails = workers?.queues?.[name] || {};
            return React.createElement("tr", { key: name },
              React.createElement("td", null, name),
              React.createElement("td", null, details.size),
              React.createElement("td", { className: "muted" }, formatAge(details.oldest_age_seconds)),
              React.createElement("td", null, details.dlq_size),
              React.createElement("td", { className: "muted" }, formatAge(details.oldest_dlq_age_seconds)),
              React.createElement("td", null, workerDetails.observed_workers ?? 0),
              React.createElement("td", null, React.createElement(Badge, { value: workerDetails.status || "unknown", status: workerDetails.status || "unknown" })),
              React.createElement("td", null, details.paused ? React.createElement(Badge, { value: "paused", status: "blocked" }) : "-")
            );
          })
        )
      )
    )
  );
}

function TaskStatusCard({ counts }) {
  return React.createElement(
    Card,
    { title: "Task Status 24h" },
    React.createElement(ObjectBars, { values: counts, emptyText: "No task execution data" })
  );
}

function RuntimeHealthCard({ summary }) {
  const health = summary?.health || {};
  const workers = summary?.workers?.queues || {};
  const rows = [
    ["Redis", health.redis?.ok ? "healthy" : "down", health.redis?.url || ""],
    ["Services", summary?.app_services?.status || "unknown", `${summary?.app_services?.healthy_services || 0} healthy`],
    ...Object.entries(workers).map(([queue, details]) => [
      queue,
      details.status || "unknown",
      `${details.healthy_workers || 0}/${details.expected_workers || 0} workers`,
    ]),
  ];

  return React.createElement(
    Card,
    { title: "Runtime Health" },
    React.createElement(
      "div",
      { className: "health-list" },
      rows.map(([name, status, detail]) =>
        React.createElement(
          "div",
          { className: "health-row", key: name },
          React.createElement("span", { className: "health-name" }, name),
          React.createElement(Badge, { value: status, status }),
          React.createElement("span", { className: "muted" }, detail)
        )
      )
    )
  );
}

function RecentTasksCard({ tasks }) {
  return React.createElement(
    Card,
    { title: "Recent Runtime Transactions" },
    React.createElement(GenericRows, { rows: tasks, emptyText: "No recent task transactions" })
  );
}

function RecentFailuresCard({ failures }) {
  return React.createElement(
    Card,
    { title: "Recent Failures" },
    React.createElement(GenericRows, { rows: failures, emptyText: "No recent failures" })
  );
}

function DurationCard({ durations }) {
  return React.createElement(
    Card,
    { title: "Task Duration 24h" },
    durations.length === 0
      ? React.createElement("div", { className: "muted" }, "No duration data")
      : React.createElement("div", { className: "table-wrap" },
        React.createElement("table", null,
          React.createElement("thead", null, React.createElement("tr", null,
            ["Task", "Count", "Avg s", "P95 s"].map((text) => React.createElement("th", { key: text }, text))
          )),
          React.createElement("tbody", null,
            durations.map((item) =>
              React.createElement("tr", { key: item.task_name },
                React.createElement("td", null, item.task_name),
                React.createElement("td", null, item.count),
                React.createElement("td", null, formatSeconds(item.avg_duration_ms)),
                React.createElement("td", null, formatSeconds(item.p95_duration_ms))
              )
            )
          )
        )
      )
  );
}

function LogsView({ source, logs, logLevel, setLogLevel, onRefresh }) {
  const title = source.workerId ? `${source.serviceName} / ${source.workerId}` : source.serviceName;
  return React.createElement(
    React.Fragment,
    null,
    React.createElement(
      "section",
      { className: "log-toolbar" },
      React.createElement("div", null,
        React.createElement("h2", null, title),
        React.createElement("div", { className: "subtitle" }, "Service log tail")
      ),
      React.createElement("div", { className: "toolbar" },
        React.createElement(
          "select",
          {
            className: "select",
            value: logLevel,
            onChange: (event) => setLogLevel(event.target.value),
          },
          [
            ["", "All levels"],
            ["debug", "Debug"],
            ["info", "Info"],
            ["warning", "Warning"],
            ["error", "Error"],
          ].map(([value, label]) => React.createElement("option", { key: value, value }, label))
        ),
        React.createElement("button", { className: "btn", onClick: onRefresh }, "Refresh")
      )
    ),
    React.createElement(
      "section",
      { className: "log-panel" },
      logs.length === 0
        ? React.createElement("div", { className: "muted" }, "No log entries")
        : logs.map((entry) => React.createElement(LogLine, { key: entry.id, entry }))
    )
  );
}

function LogLine({ entry }) {
  const text = [
    formatDate(entry.created_at),
    entry.level.toUpperCase().padEnd(7, " "),
    entry.event_name || entry.logger_name || "-",
    entry.message,
  ].join("  ");
  return React.createElement(
    "details",
    { className: `log-line ${entry.level}` },
    React.createElement("summary", null,
      React.createElement("span", { className: "log-text" }, text)
    ),
    React.createElement("pre", null, JSON.stringify(entry.details || {}, null, 2))
  );
}

function ObjectBarsCard({ title, values }) {
  return React.createElement(Card, { title }, React.createElement(ObjectBars, { values, emptyText: "No data" }));
}

function ObjectBars({ values, emptyText }) {
  const rows = Object.entries(values || {});
  const max = Math.max(1, ...rows.map(([, value]) => Number(value || 0)));
  if (rows.length === 0) return React.createElement("div", { className: "muted" }, emptyText);
  return React.createElement("div", { className: "bars" },
    rows.map(([key, value]) =>
      React.createElement("div", { className: "bar-row", key },
        React.createElement("span", null, key),
        React.createElement("div", { className: "bar-track" },
          React.createElement("div", { className: "bar-fill", style: { width: `${Math.max(4, (Number(value || 0) / max) * 100)}%` } })
        ),
        React.createElement("strong", null, value ?? 0)
      )
    )
  );
}

function KeyValueCard({ title, value }) {
  return React.createElement(Card, { title }, React.createElement(KeyValueRows, { value }));
}

function KeyValueRows({ value }) {
  const rows = Object.entries(flattenObject(value || {}));
  if (rows.length === 0) return React.createElement("div", { className: "muted" }, "No data");
  return React.createElement("div", { className: "metric-stack" },
    rows.map(([key, item]) =>
      React.createElement("div", { className: "metric-line", key },
        React.createElement("span", null, key),
        React.createElement("strong", { className: "wrap" }, formatCell(item))
      )
    )
  );
}

function GenericRowsCard({ title, rows }) {
  return React.createElement(Card, { title }, React.createElement(GenericRows, { rows, emptyText: "No rows" }));
}

function GenericRows({ rows, emptyText }) {
  if (!rows || rows.length === 0) return React.createElement("div", { className: "muted" }, emptyText);
  const columns = Array.from(rows.reduce((set, row) => {
    Object.keys(row || {}).slice(0, 8).forEach((key) => set.add(key));
    return set;
  }, new Set()));
  return React.createElement("div", { className: "table-wrap" },
    React.createElement("table", { className: "dense-table" },
      React.createElement("thead", null, React.createElement("tr", null,
        columns.map((column) => React.createElement("th", { key: column }, column))
      )),
      React.createElement("tbody", null,
        rows.map((row, index) =>
          React.createElement("tr", { key: row.id || row.task_id || index },
            columns.map((column) =>
              React.createElement("td", { key: column, className: "wrap muted" }, formatCell(row[column]))
            )
          )
        )
      )
    )
  );
}

function Card({ title, children }) {
  return React.createElement(
    "div",
    { className: "card" },
    React.createElement("div", { className: "card-head" }, React.createElement("div", { className: "card-title" }, title)),
    React.createElement("div", { className: "card-body" }, children)
  );
}

function Badge({ value, status }) {
  return React.createElement("span", { className: `badge ${status || "unknown"}` }, String(value ?? "-"));
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`${url} returned ${response.status}`);
  }
  return response.json();
}

function sumValues(object, key) {
  return Object.values(object).reduce((sum, item) => sum + Number(item?.[key] || 0), 0);
}

function sumObjectValues(object, keys) {
  return keys.reduce((sum, key) => sum + Number(object?.[key] || 0), 0);
}

function maxValues(object, key) {
  const values = Object.values(object).reduce((items, item) => {
    const value = Number(item?.[key]);
    return Number.isFinite(value) ? [...items, value] : items;
  }, []);
  return values.length === 0 ? null : Math.max(...values);
}

function flattenObject(value, prefix = "") {
  return Object.entries(value || {}).reduce((result, [key, item]) => {
    const nextKey = prefix ? `${prefix}.${key}` : key;
    if (item && typeof item === "object" && !Array.isArray(item)) {
      return { ...result, ...flattenObject(item, nextKey) };
    }
    return { ...result, [nextKey]: item };
  }, {});
}

function formatDate(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString();
}

function formatSeconds(value) {
  if (value === null || value === undefined) return "-";
  return (Number(value) / 1000).toFixed(1);
}

function formatAge(value) {
  if (value === null || value === undefined) return "-";
  const seconds = Number(value);
  if (!Number.isFinite(seconds)) return "-";
  if (seconds < 60) return `${Math.floor(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.floor(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

function formatCell(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") return String(value);
  return JSON.stringify(value);
}

function buildLogTab(source) {
  return `log:${source.source_type}:${encodeURIComponent(source.service_name)}:${encodeURIComponent(source.worker_id || "")}`;
}

function isLogTab(value) {
  return typeof value === "string" && value.startsWith("log:");
}

function parseLogTab(value) {
  if (!isLogTab(value)) return null;
  const [, sourceType, serviceName, workerId] = value.split(":");
  return {
    sourceType,
    serviceName: decodeURIComponent(serviceName || ""),
    workerId: workerId ? decodeURIComponent(workerId) : "",
  };
}

ReactDOM.createRoot(document.getElementById("root")).render(React.createElement(App));
