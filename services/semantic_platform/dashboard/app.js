if (!window.React || !window.ReactDOM) {
  document.getElementById("root").innerHTML =
    '<div class="app"><main class="main"><div class="error">React runtime could not be loaded.</div></main></div>';
  throw new Error("React runtime could not be loaded");
}

const { useEffect, useMemo, useRef, useState } = React;

const SECTIONS = [
  { key: "fields", label: "Fields", icon: "F" },
  { key: "entities", label: "Entities", icon: "E" },
  { key: "relationships", label: "Relations", icon: "R" },
  { key: "capabilities", label: "Capabilities", icon: "C" },
  { key: "vocabulary", label: "Vocabulary", icon: "V" },
  { key: "proposals", label: "Proposals", icon: "P" },
  { key: "sources", label: "Sources", icon: "S" },
];

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json();
}

function App() {
  const [catalog, setCatalog] = useState(null);
  const [meta, setMeta] = useState(null);
  const [sources, setSources] = useState(null);
  const [sourceSummary, setSourceSummary] = useState(null);
  const [proposals, setProposals] = useState(null);
  const [activeSection, setActiveSection] = useState("fields");
  const [selectedKey, setSelectedKey] = useState(null);
  const [query, setQuery] = useState("");
  const [refreshMs, setRefreshMs] = useState(30000);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  async function load() {
    setLoading(true);
    setError(null);
    try {
      const [catalogRes, metaRes, sourcesRes, sourceSummaryRes, proposalsRes] = await Promise.all([
        fetchJson("api/catalog"),
        fetchJson("api/catalog/meta"),
        fetchJson("api/sources"),
        fetchJson("api/sources/summary"),
        fetchJson("api/proposals"),
      ]);
      setCatalog(catalogRes);
      setMeta(metaRes);
      setSources(sourcesRes);
      setSourceSummary(sourceSummaryRes);
      setProposals(proposalsRes);
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

  const rows = useMemo(() => sectionRows(catalog, sources, proposals, activeSection), [catalog, sources, proposals, activeSection]);
  const globalRows = useMemo(() => allRows(catalog, sources, proposals), [catalog, sources, proposals]);
  const searchResults = useMemo(() => filterRows(globalRows, query), [globalRows, query]);
  const selected = useMemo(() => {
    if (selectedKey) {
      return rows.find((row) => row.key === selectedKey) || rows[0] || null;
    }
    return rows[0] || null;
  }, [rows, selectedKey]);
  const counts = useMemo(() => sectionCounts(catalog, sources, proposals), [catalog, sources, proposals]);

  async function applyProposal(proposalId) {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`api/proposals/${encodeURIComponent(proposalId)}/apply`, { method: "POST" });
      if (!response.ok) {
        throw new Error(`${response.status} ${response.statusText}`);
      }
      await load();
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
    }
  }

  async function rejectProposal(proposalId) {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`api/proposals/${encodeURIComponent(proposalId)}/reject`, { method: "POST" });
      if (!response.ok) {
        throw new Error(`${response.status} ${response.statusText}`);
      }
      await load();
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    setSelectedKey(null);
  }, [activeSection]);

  return React.createElement("div", { className: "app" },
    React.createElement("header", { className: "topbar" },
      React.createElement("div", { className: "title" },
        React.createElement("h1", null, "Semantic Platform"),
        React.createElement("div", { className: "subtitle" }, meta ? `${meta.name || "catalog"} · ${meta.source || "source"}` : "Loading")
      ),
      React.createElement(GlobalSearch, {
        query,
        setQuery,
        results: searchResults,
        onSelect: (row) => {
          setActiveSection(row.section);
          setSelectedKey(row.key);
        },
      }),
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
      React.createElement(Sidebar, { activeSection, setActiveSection, counts }),
      React.createElement("main", { className: "main" },
        error && React.createElement("div", { className: "error" }, error),
        React.createElement(Kpis, { counts, catalog, meta, sourceSummary }),
        React.createElement("section", { className: "grid layout" },
          React.createElement(ListCard, {
            activeSection,
            rows,
            selectedKey: selected?.key,
            onSelect: setSelectedKey,
          }),
          React.createElement(DetailCard, {
            activeSection,
            selected,
            onApply: applyProposal,
            onReject: rejectProposal,
            loading,
          })
        )
      )
    )
  );
}

function GlobalSearch({ query, setQuery, results, onSelect }) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef(null);
  const showResults = open && query.trim().length > 0;

  useEffect(() => {
    function onPointerDown(event) {
      if (rootRef.current && !rootRef.current.contains(event.target)) {
        setOpen(false);
      }
    }
    document.addEventListener("pointerdown", onPointerDown);
    return () => document.removeEventListener("pointerdown", onPointerDown);
  }, []);

  return React.createElement("div", { className: "global-search", ref: rootRef },
    React.createElement("input", {
      className: "search",
      value: query,
      onChange: (event) => {
        setQuery(event.target.value);
        setOpen(true);
      },
      onFocus: () => setOpen(true),
      onClick: () => setOpen(true),
      placeholder: "Search catalog",
      "aria-label": "Search catalog",
    }),
    showResults && React.createElement("div", { className: "search-popover" },
      results.length === 0
        ? React.createElement("div", { className: "search-empty" }, "No results")
        : results.slice(0, 12).map((row) =>
            React.createElement("button", {
              className: "search-result",
              key: `${row.section}:${row.key}`,
              onClick: () => {
                onSelect(row);
                setOpen(false);
              },
            },
              React.createElement("span", { className: "search-result-name" }, row.key),
              React.createElement("span", { className: "search-result-meta" }, `${sectionLabel(row.section)} · ${row.type || row.entity || row.kind || "item"}`)
            )
          ),
      results.length > 12 && React.createElement("div", { className: "search-more" }, `${results.length - 12} more results`)
    )
  );
}

function Sidebar({ activeSection, setActiveSection, counts }) {
  return React.createElement("aside", { className: "sidebar" },
    React.createElement("div", { className: "side-brand" },
      React.createElement("div", { className: "brand-mark" }, "C"),
      React.createElement("div", null,
        React.createElement("div", { className: "brand-name" }, "catalog"),
        React.createElement("div", { className: "brand-caption" }, "Semantic registry")
      )
    ),
    React.createElement("div", { className: "side-section" }, "Sections"),
    SECTIONS.map((section) =>
      React.createElement("button", {
        className: `nav-item ${activeSection === section.key ? "active" : ""}`,
        key: section.key,
        onClick: () => setActiveSection(section.key),
      },
        React.createElement("span", { className: "nav-icon" }, section.icon),
        React.createElement("span", { className: "nav-label" }, section.label),
        React.createElement("span", { className: "nav-count" }, counts[section.key] || 0)
      )
    )
  );
}

function Kpis({ counts, catalog, meta, sourceSummary }) {
  const providerCount = countProviders(catalog?.fields || {});
  return React.createElement("section", { className: "grid kpis" },
    React.createElement(Kpi, { label: "Fields", value: counts.fields || 0, detail: `${providerCount} providers mapped` }),
    React.createElement(Kpi, { label: "Entities", value: counts.entities || 0, detail: "Canonical objects" }),
    React.createElement(Kpi, { label: "Relations", value: counts.relationships || 0, detail: "Entity links" }),
    React.createElement(Kpi, { label: "Capabilities", value: counts.capabilities || 0, detail: `${countObject(catalog?.workflows)} workflows` }),
    React.createElement(Kpi, { label: "Sources", value: counts.sources || 0, detail: `${sourceSummary?.processing?.pending || 0} pending` })
  );
}

function Kpi({ label, value, detail }) {
  return React.createElement("div", { className: "kpi" },
    React.createElement("div", { className: "kpi-label" }, label),
    React.createElement("div", { className: "kpi-value" }, value),
    React.createElement("div", { className: "kpi-detail" }, detail || "")
  );
}

function ListCard({ activeSection, rows, selectedKey, onSelect }) {
  const title = SECTIONS.find((section) => section.key === activeSection)?.label || activeSection;
  return React.createElement("section", { className: "card" },
    React.createElement("div", { className: "card-head" },
      React.createElement("div", null,
        React.createElement("div", { className: "card-title" }, title),
        React.createElement("div", { className: "subtitle" }, `${rows.length} items`)
      )
    ),
    React.createElement("div", { className: "table-wrap" },
      rows.length === 0
        ? React.createElement("div", { className: "card-body muted" }, "No catalog items")
        : React.createElement("table", null,
            React.createElement("thead", null,
              React.createElement("tr", null,
                React.createElement("th", null, "Name"),
                React.createElement("th", null, "Type"),
                React.createElement("th", null, "Summary")
              )
            ),
            React.createElement("tbody", null,
              rows.map((row) =>
                React.createElement("tr", {
                  className: `clickable ${selectedKey === row.key ? "selected" : ""}`,
                  key: row.key,
                  onClick: () => onSelect(row.key),
                },
                  React.createElement("td", { className: "wrap" }, row.key),
                  React.createElement("td", null, React.createElement("span", { className: badgeClass(row) }, row.type || row.entity || row.kind || "-")),
                  React.createElement("td", { className: "wrap muted" }, row.summary || "-")
                )
              )
            )
          )
    )
  );
}

function DetailCard({ activeSection, selected, onApply, onReject, loading }) {
  const isPendingProposal = activeSection === "proposals" && selected?.value?.status === "pending_review";
  return React.createElement("section", { className: "card" },
    React.createElement("div", { className: "card-head" },
      React.createElement("div", null,
        React.createElement("div", { className: "card-title" }, "Detail"),
        React.createElement("div", { className: "subtitle" }, selected ? activeSection : "No selection")
      ),
      isPendingProposal && React.createElement("div", { className: "actions" },
        React.createElement("button", {
          className: "btn danger",
          disabled: loading,
          onClick: () => onReject(selected.value.proposal_id),
        }, loading ? "Working" : "Reject"),
        React.createElement("button", {
          className: "btn primary",
          disabled: loading,
          onClick: () => onApply(selected.value.proposal_id),
        }, loading ? "Applying" : "Apply")
      )
    ),
    React.createElement("div", { className: "card-body" },
      selected
        ? React.createElement(Detail, { item: selected })
        : React.createElement("div", { className: "muted" }, "Select an item")
    )
  );
}

function Detail({ item }) {
  const data = item.value || {};
  return React.createElement("div", { className: "detail" },
    React.createElement("div", { className: "detail-title" },
      React.createElement("div", { className: "detail-name" }, item.key),
      React.createElement("span", { className: badgeClass(item) }, item.type || item.entity || item.kind || "item")
    ),
    data.description && React.createElement("div", { className: "muted" }, data.description),
    React.createElement(DefinitionList, { data: compactObject({
      entity: data.entity,
      type: data.type,
      from: data.from,
      to: data.to,
      primary_entity: data.primary_entity,
      field: data.field,
      status: data.status,
      action: data.action,
      direction: data.direction,
      provider: data.provider,
      tool_name: data.tool_name,
      path: data.path,
      current_sha256: data.current_sha256,
      proposal_builder: data.proposal_builder,
      source_path: data.source_path,
    }) }),
    data.counts && React.createElement(DefinitionList, { title: "Counts", data: data.counts }),
    data.review && Object.keys(data.review).length > 0 && React.createElement(DefinitionList, { title: "Review", data: compactObject({
      status: data.review.status,
      action: data.review.action,
      changed_count: Array.isArray(data.review.changed) ? data.review.changed.length : undefined,
    }) }),
    Array.isArray(data.aliases) && data.aliases.length > 0 && React.createElement(ChipsSection, { title: "Aliases", values: data.aliases }),
    data.provider_mappings && React.createElement(ProviderMappings, { providerMappings: data.provider_mappings }),
    Array.isArray(data.identifiers) && data.identifiers.length > 0 && React.createElement(ChipsSection, { title: "Identifiers", values: data.identifiers }),
    Array.isArray(data.outputs) && data.outputs.length > 0 && React.createElement(ChipsSection, { title: "Outputs", values: data.outputs }),
    Array.isArray(data.tools) && data.tools.length > 0 && React.createElement(ChipsSection, { title: "Tools", values: data.tools }),
    Array.isArray(data.join_keys) && data.join_keys.length > 0 && React.createElement(ChipsSection, { title: "Join Keys", values: data.join_keys }),
    React.createElement("div", { className: "detail-section" },
      React.createElement("h3", null, "Raw"),
      React.createElement("pre", null, JSON.stringify(data, null, 2))
    )
  );
}

function DefinitionList({ title = "Definition", data }) {
  const entries = Object.entries(data || {});
  if (entries.length === 0) return null;
  return React.createElement("div", { className: "detail-section" },
    React.createElement("h3", null, title),
    React.createElement("dl", { className: "kv" },
      entries.flatMap(([key, value]) => [
        React.createElement("dt", { key: `${key}-dt` }, key),
        React.createElement("dd", { key: `${key}-dd` }, String(value)),
      ])
    )
  );
}

function ChipsSection({ title, values }) {
  return React.createElement("div", { className: "detail-section" },
    React.createElement("h3", null, title),
    React.createElement("div", { className: "chips" }, values.map((value) =>
      React.createElement("span", { className: "chip", key: String(value) }, String(value))
    ))
  );
}

function ProviderMappings({ providerMappings }) {
  return React.createElement("div", { className: "detail-section" },
    React.createElement("h3", null, "Provider Mappings"),
    React.createElement("div", { className: "chips" },
      Object.entries(providerMappings).flatMap(([provider, values]) =>
        (Array.isArray(values) ? values : []).map((value) =>
          React.createElement("span", { className: "chip", key: `${provider}:${value}` }, `${provider}:${value}`)
        )
      )
    )
  );
}

function sectionRows(catalog, sources, proposals, section) {
  if (section === "sources") {
    return sourceRows(sources);
  }
  if (section === "proposals") {
    return proposalRows(proposals);
  }
  if (!catalog) return [];
  const source = catalog[section] || {};
  return Object.entries(source).map(([key, value]) => {
    const data = value && typeof value === "object" ? value : {};
    return {
      key,
      section,
      value: data,
      type: data.type,
      entity: data.entity,
      kind: section.slice(0, -1),
      summary: summaryFor(section, data),
    };
  });
}

function sourceRows(sources) {
  const documents = Array.isArray(sources?.documents) ? sources.documents : [];
  return documents.map((document) => ({
    key: document.document_id || document.path,
    section: "sources",
    value: document,
    type: document.status,
    entity: document.provider,
    kind: "source",
    summary: document.path,
  }));
}

function proposalRows(proposals) {
  const rows = Array.isArray(proposals?.proposals) ? proposals.proposals : [];
  return rows.map((proposal) => ({
    key: proposal.proposal_id || `${proposal.section}:${proposal.key}`,
    section: "proposals",
    value: proposal,
    type: proposal.action,
    entity: proposal.section,
    kind: "proposal",
    summary: proposal.reason,
  }));
}

function allRows(catalog, sources, proposals) {
  return SECTIONS.flatMap((section) => sectionRows(catalog, sources, proposals, section.key));
}

function sectionLabel(sectionKey) {
  return SECTIONS.find((section) => section.key === sectionKey)?.label || sectionKey;
}

function summaryFor(section, data) {
  if (!data || typeof data !== "object") return "";
  if (data.description) return data.description;
  if (section === "proposals") return data.reason || `${data.action || "proposal"} ${data.key || ""}`;
  if (section === "relationships") return `${data.from || "-"} -> ${data.to || "-"}`;
  if (section === "capabilities") return `${data.primary_entity || "-"} capability`;
  if (section === "vocabulary") return data.field ? `field: ${data.field}` : data.entity ? `entity: ${data.entity}` : "";
  return "";
}

function filterRows(rows, query) {
  const text = query.trim().toLowerCase();
  if (!text) return rows;
  return rows.filter((row) => JSON.stringify({ key: row.key, value: row.value }).toLowerCase().includes(text));
}

function sectionCounts(catalog, sources, proposals) {
  return Object.fromEntries(SECTIONS.map((section) => [
    section.key,
    section.key === "sources"
      ? (Array.isArray(sources?.documents) ? sources.documents.length : 0)
      : section.key === "proposals"
        ? (Array.isArray(proposals?.proposals) ? proposals.proposals.length : 0)
        : countObject(catalog?.[section.key]),
  ]));
}

function countObject(value) {
  return value && typeof value === "object" ? Object.keys(value).length : 0;
}

function countProviders(fields) {
  const providers = new Set();
  Object.values(fields || {}).forEach((field) => {
    Object.keys(field.provider_mappings || {}).forEach((provider) => providers.add(provider));
  });
  return providers.size;
}

function compactObject(value) {
  return Object.fromEntries(Object.entries(value).filter(([, item]) => item !== undefined && item !== null && item !== ""));
}

function badgeClass(row) {
  if (row.entity || row.value?.entity) return "badge green";
  if (row.value?.from || row.value?.to) return "badge amber";
  return "badge";
}

ReactDOM.createRoot(document.getElementById("root")).render(React.createElement(App));
