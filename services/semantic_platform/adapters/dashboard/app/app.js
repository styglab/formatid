if (!window.React || !window.ReactDOM) {
  document.getElementById("root").innerHTML =
    '<div class="app"><main class="main"><div class="error">React runtime could not be loaded.</div></main></div>';
  throw new Error("React runtime could not be loaded");
}

const { useEffect, useMemo, useRef, useState } = React;

const SECTIONS = [
  { key: "capabilities", label: "Capabilities", icon: "C" },
  { key: "semantic_graph", label: "Semantic Graph", icon: "S" },
  { key: "execution", label: "Execution", icon: "E" },
  { key: "proposals", label: "Governance", icon: "G" },
];

const GRAPH_SECTIONS = [
  { key: "entities", label: "Entities" },
  { key: "entity_identifiers", label: "Identifiers" },
  { key: "semantic_types", label: "Semantic Types" },
  { key: "semantic_join_rules", label: "Join Rules" },
  { key: "capability_entity_links", label: "Capability Entities" },
  { key: "capability_dependencies", label: "Dependencies" },
  { key: "planning_examples", label: "Examples" },
];

const EXECUTION_SECTIONS = [
  { key: "resources", label: "Resources" },
  { key: "operations", label: "Operations" },
  { key: "operation_contracts", label: "Contracts" },
  { key: "operation_variants", label: "Variants" },
  { key: "field_mappings", label: "Mappings" },
  { key: "capability_implementations", label: "Implementations" },
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
  const [endpointChecks, setEndpointChecks] = useState(null);
  const [activeSection, setActiveSection] = useState("capabilities");
  const [activeGraphSection, setActiveGraphSection] = useState("entities");
  const [activeExecutionSection, setActiveExecutionSection] = useState("operations");
  const [selectedKey, setSelectedKey] = useState(null);
  const [pageData, setPageData] = useState(null);
  const [pageOffset, setPageOffset] = useState(0);
  const [pageLoading, setPageLoading] = useState(false);
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const pageSize = 25;

  async function load() {
    setLoading(true);
    setError(null);
    try {
      const [catalogRes, metaRes, sourcesRes, sourceSummaryRes, proposalsRes, checksRes] = await Promise.all([
        fetchJson("api/catalog"),
        fetchJson("api/catalog/meta"),
        fetchJson("api/sources"),
        fetchJson("api/sources/summary"),
        fetchJson("api/proposals"),
        fetchJson("api/semantic/execution/checks?limit=500").catch(() => ({ endpoint_checks: [] })),
      ]);
      setCatalog(catalogRes);
      setMeta(metaRes);
      setSources(sourcesRes);
      setSourceSummary(sourceSummaryRes);
      setProposals(proposalsRes);
      setEndpointChecks(checksRes);
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, []);

  async function loadPage(section, offset) {
    if (section === "proposals") {
      setPageData(null);
      return;
    }
    const catalogSection = section === "execution"
      ? activeExecutionSection
      : section === "semantic_graph"
        ? activeGraphSection
        : section;
    setPageLoading(true);
    try {
      const data = await fetchJson(`api/catalog/sections/${encodeURIComponent(catalogSection)}?limit=${pageSize}&offset=${offset}`);
      setPageData(data);
    } catch (err) {
      setError(err.message || String(err));
      setPageData(null);
    } finally {
      setPageLoading(false);
    }
  }

  useEffect(() => {
    setSelectedKey(null);
    setPageData(null);
    setPageOffset(0);
  }, [activeSection, activeGraphSection, activeExecutionSection]);

  useEffect(() => {
    loadPage(activeSection, pageOffset);
  }, [activeSection, activeGraphSection, activeExecutionSection, pageOffset]);

  const rows = useMemo(
    () => pageData ? pageRows(pageData, activeSection, activeGraphSection, activeExecutionSection) : sectionRows(catalog, sources, proposals, activeSection, activeGraphSection, activeExecutionSection),
    [catalog, sources, proposals, pageData, activeSection, activeGraphSection, activeExecutionSection]
  );
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
          if (row.executionSection) setActiveExecutionSection(row.executionSection);
          if (row.graphSection) setActiveGraphSection(row.graphSection);
          setSelectedKey(row.key);
        },
      }),
      React.createElement("div", { className: "toolbar" })
    ),
    React.createElement("div", { className: "shell" },
      React.createElement(Sidebar, { activeSection, setActiveSection, counts }),
      React.createElement("main", { className: "main" },
        error && React.createElement("div", { className: "error" }, error),
        React.createElement(Kpis, { counts, catalog, meta, sourceSummary }),
        React.createElement("section", { className: "grid layout" },
          React.createElement(ListCard, {
            activeSection,
            activeGraphSection,
            setActiveGraphSection,
            activeExecutionSection,
            setActiveExecutionSection,
            counts,
            rows,
            pageData: activeSection === "proposals" ? null : pageData,
            pageSize,
            pageOffset,
            pageLoading,
            onPage: setPageOffset,
            selectedKey: selected?.key,
            onSelect: setSelectedKey,
          }),
          React.createElement(DetailCard, {
            activeSection,
            selected,
            catalog,
            endpointChecks,
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
  return React.createElement("section", { className: "grid kpis" },
    React.createElement(Kpi, { label: "Capabilities", value: counts.capabilities || 0, detail: `${countObject(catalog?.capability_documents)} documents` }),
    React.createElement(Kpi, { label: "Entities", value: counts.entities || 0, detail: `${counts.semantic_join_rules || 0} join rules` }),
    React.createElement(Kpi, { label: "Operations", value: counts.operations || 0, detail: `${countObject(catalog?.operation_contracts)} contracts` }),
    React.createElement(Kpi, { label: "Variants", value: counts.operation_variants || 0, detail: `${countObject(catalog?.field_mappings)} mappings` }),
    React.createElement(Kpi, { label: "Proposals", value: counts.proposals || 0, detail: "Pending review" })
  );
}

function Kpi({ label, value, detail }) {
  return React.createElement("div", { className: "kpi" },
    React.createElement("div", { className: "kpi-label" }, label),
    React.createElement("div", { className: "kpi-value" }, value),
    React.createElement("div", { className: "kpi-detail" }, detail || "")
  );
}

function ListCard({
  activeSection,
  activeGraphSection,
  setActiveGraphSection,
  activeExecutionSection,
  setActiveExecutionSection,
  counts,
  rows,
  pageData,
  pageSize,
  pageOffset,
  pageLoading,
  onPage,
  selectedKey,
  onSelect,
}) {
  const title = SECTIONS.find((section) => section.key === activeSection)?.label || activeSection;
  const total = pageData?.total ?? rows.length;
  const start = total === 0 ? 0 : pageOffset + 1;
  const end = Math.min(pageOffset + rows.length, total);
  return React.createElement("section", { className: "card" },
    React.createElement("div", { className: "card-head" },
      React.createElement("div", null,
        React.createElement("div", { className: "card-title" }, title),
        React.createElement("div", { className: "subtitle" }, pageData ? `${start}-${end} of ${total} items` : `${rows.length} items`)
      ),
      pageData && React.createElement(Pagination, {
        total,
        pageSize,
        offset: pageOffset,
        loading: pageLoading,
        onPage,
      })
    ),
    activeSection === "semantic_graph" && React.createElement(SectionTabs, {
      sections: GRAPH_SECTIONS,
      activeKey: activeGraphSection,
      setActiveKey: setActiveGraphSection,
      counts,
    }),
    activeSection === "execution" && React.createElement(ExecutionTabs, {
      activeExecutionSection,
      setActiveExecutionSection,
      counts,
    }),
    React.createElement("div", { className: "table-wrap" },
      rows.length === 0
        ? React.createElement("div", { className: "card-body muted" }, "No catalog items")
        : React.createElement("table", null,
            React.createElement("thead", null,
              React.createElement("tr", null,
                React.createElement("th", null, "Name"),
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
                  React.createElement("td", { className: "wrap muted" }, row.summary || "-")
                )
              )
            )
          )
    )
  );
}

function Pagination({ total, pageSize, offset, loading, onPage }) {
  const pageCount = Math.max(1, Math.ceil(total / pageSize));
  const currentPage = Math.floor(offset / pageSize) + 1;
  const pages = visiblePages(currentPage, pageCount);
  return React.createElement("div", { className: "pagination" },
    React.createElement("button", {
      className: "page-btn",
      disabled: loading || currentPage <= 1,
      onClick: () => onPage(Math.max(0, offset - pageSize)),
    }, "Prev"),
    pages.map((page) =>
      React.createElement("button", {
        className: `page-btn ${page === currentPage ? "active" : ""}`,
        key: page,
        disabled: loading,
        onClick: () => onPage((page - 1) * pageSize),
      }, String(page))
    ),
    React.createElement("button", {
      className: "page-btn",
      disabled: loading || currentPage >= pageCount,
      onClick: () => onPage(offset + pageSize),
    }, "Next")
  );
}

function visiblePages(currentPage, pageCount) {
  const start = Math.max(1, currentPage - 2);
  const end = Math.min(pageCount, start + 4);
  const adjustedStart = Math.max(1, end - 4);
  const pages = [];
  for (let page = adjustedStart; page <= end; page += 1) {
    pages.push(page);
  }
  return pages;
}

function ExecutionTabs({ activeExecutionSection, setActiveExecutionSection, counts }) {
  return React.createElement(SectionTabs, {
    sections: EXECUTION_SECTIONS,
    activeKey: activeExecutionSection,
    setActiveKey: setActiveExecutionSection,
    counts,
  });
}

function SectionTabs({ sections, activeKey, setActiveKey, counts }) {
  return React.createElement("div", { className: "subnav" },
    sections.map((section) =>
      React.createElement("button", {
        className: `subnav-item ${activeKey === section.key ? "active" : ""}`,
        key: section.key,
        onClick: () => setActiveKey(section.key),
      },
        React.createElement("span", null, section.label),
        React.createElement("span", { className: "subnav-count" }, counts[section.key] || 0)
      )
    )
  );
}

function DetailCard({ activeSection, selected, catalog, endpointChecks, onApply, onReject, loading }) {
  const isPendingProposal = activeSection === "proposals" && selected?.value?.status === "pending_review";
  const [activeDetailTab, setActiveDetailTab] = useState("overview");
  useEffect(() => {
    setActiveDetailTab("overview");
  }, [selected?.key]);
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
          onClick: () => onReject(selected.value.id),
        }, loading ? "Working" : "Reject"),
        React.createElement("button", {
          className: "btn primary",
          disabled: loading,
          onClick: () => onApply(selected.value.id),
        }, loading ? "Applying" : "Apply")
      )
    ),
    React.createElement("div", { className: "card-body" },
      selected
        ? React.createElement(Detail, {
            item: selected,
            catalog,
            endpointChecks,
            activeDetailTab,
            setActiveDetailTab,
          })
        : React.createElement("div", { className: "muted" }, "Select an item")
    )
  );
}

function Detail({ item, catalog, endpointChecks, activeDetailTab, setActiveDetailTab }) {
  const data = item.value || {};
  const provenance = data.provenance && typeof data.provenance === "object" ? data.provenance : null;
  const catalogSection = data.__catalog_section || item.section;
  const tabs = detailTabs(item, data, catalogSection, provenance);
  return React.createElement("div", { className: "detail" },
    React.createElement("div", { className: "detail-title" },
      React.createElement("div", { className: "detail-name" }, item.key),
      React.createElement("span", { className: badgeClass(item) }, item.type || item.entity || item.kind || "item")
    ),
    data.description && React.createElement("div", { className: "muted" }, data.description),
    React.createElement("div", { className: "detail-tabs" },
      tabs.map((tab) =>
        React.createElement("button", {
          className: `detail-tab ${activeDetailTab === tab.key ? "active" : ""}`,
          key: tab.key,
          onClick: () => setActiveDetailTab(tab.key),
        }, tab.label)
      )
    ),
    React.createElement("div", { className: "detail-scroll" },
      activeDetailTab === "overview" && React.createElement(DetailOverview, { item, data }),
      activeDetailTab === "execution" && React.createElement(DetailExecution, { item, data, catalog, endpointChecks, catalogSection }),
      activeDetailTab === "evidence" && React.createElement(DetailEvidence, { item, data, provenance }),
      activeDetailTab === "raw" && React.createElement("div", { className: "detail-section" },
        React.createElement("h3", null, "Raw"),
        React.createElement("pre", null, JSON.stringify(data, null, 2))
      )
    )
  );
}

function detailTabs(item, data, catalogSection, provenance) {
  const tabs = [{ key: "overview", label: "Overview" }];
  if (item.section === "capabilities" || catalogSection === "operation_contracts" || catalogSection === "operation_variants") {
    tabs.push({ key: "execution", label: "Execution" });
  }
  if (provenance || data.review || data.counts) {
    tabs.push({ key: "evidence", label: "Evidence" });
  }
  tabs.push({ key: "raw", label: "Raw" });
  return tabs;
}

function DetailOverview({ item, data }) {
  return React.createElement("div", { className: "detail-stack" },
    React.createElement(DefinitionList, { data: compactObject({
      catalog_section: data.__catalog_section,
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
    Array.isArray(data.aliases) && data.aliases.length > 0 && React.createElement(ChipsSection, { title: "Aliases", values: data.aliases }),
    data.provider_mappings && React.createElement(ProviderMappings, { providerMappings: data.provider_mappings }),
    Array.isArray(data.identifiers) && data.identifiers.length > 0 && React.createElement(ChipsSection, { title: "Identifiers", values: data.identifiers }),
    Array.isArray(data.inputs) && data.inputs.length > 0 && React.createElement(ChipsSection, { title: "Inputs", values: data.inputs }),
    Array.isArray(data.outputs) && data.outputs.length > 0 && React.createElement(ChipsSection, { title: "Outputs", values: data.outputs }),
    Array.isArray(data.tools) && data.tools.length > 0 && React.createElement(ChipsSection, { title: "Tools", values: data.tools }),
    Array.isArray(data.join_keys) && data.join_keys.length > 0 && React.createElement(ChipsSection, { title: "Join Keys", values: data.join_keys })
  );
}

function DetailExecution({ item, data, catalog, endpointChecks, catalogSection }) {
  return React.createElement("div", { className: "detail-stack" },
    item.section === "capabilities" && React.createElement(CapabilityExecutionReview, {
      capabilityId: item.key,
      catalog,
      endpointChecks,
    }),
    catalogSection === "operation_contracts" && React.createElement(OperationContractReview, {
      operationId: stripExecutionKey(item.key),
      contract: data,
      catalog,
      endpointChecks,
    }),
    catalogSection === "operation_variants" && React.createElement(OperationVariantReview, {
      variantId: stripExecutionKey(item.key),
      variant: data,
      catalog,
      endpointChecks,
    })
  );
}

function DetailEvidence({ item, data, provenance }) {
  return React.createElement("div", { className: "detail-stack" },
    item.section === "capabilities" && provenance && React.createElement(CapabilityTrace, { provenance }),
    data.review && Object.keys(data.review).length > 0 && React.createElement(DefinitionList, { title: "Review", data: compactObject({
      status: data.review.status,
      action: data.review.action,
      changed_count: Array.isArray(data.review.changed) ? data.review.changed.length : undefined,
    }) }),
    data.counts && React.createElement(DefinitionList, { title: "Counts", data: data.counts }),
    provenance && item.section !== "capabilities" && React.createElement(DefinitionList, { title: "Provenance", data: provenance })
  );
}

function CapabilityExecutionReview({ capabilityId, catalog, endpointChecks }) {
  const implementations = capabilityImplementations(catalog).filter((item) => item.capability_id === capabilityId);
  const variants = implementations.map((item) => catalog?.operation_variants?.[item.variant_id]).filter(Boolean);
  const contracts = uniqueBy(
    implementations.map((item) => catalog?.operation_contracts?.[item.operation_id]).filter(Boolean),
    (contract) => contract.operation_id
  );
  const checks = checksFor(endpointChecks, {
    capabilityId,
    operationIds: implementations.map((item) => item.operation_id),
    variantIds: implementations.map((item) => item.variant_id),
  });
  return React.createElement("div", { className: "review-panel" },
    React.createElement("div", { className: "review-head" },
      React.createElement("div", null,
        React.createElement("h3", null, "Execution Review"),
        React.createElement("div", { className: "subtitle" }, `${implementations.length} implementations · ${variants.length} variants · ${checks.length} checks`)
      )
    ),
    React.createElement(ReviewStats, { stats: {
      implementations: implementations.length,
      contracts: contracts.length,
      variants: variants.length,
      checks: checks.length,
      last_check: checks[0]?.checked_at || checks[0]?.created_at,
    } }),
    implementations.length > 0 && React.createElement(MiniTable, {
      title: "Implementations",
      columns: ["Operation", "Variant", "Tool", "Status"],
      rows: implementations.map((item) => [
        item.operation_id || "-",
        item.variant_id || "-",
        item.tool || "generic_http_contract",
        item.status || "-",
      ]),
    }),
    contracts.length > 0 && React.createElement(MiniTable, {
      title: "Contracts",
      columns: ["Operation", "Method", "Path", "Auth", "Items Path"],
      rows: contracts.map((contract) => [
        contract.operation_id || "-",
        contract.method || "-",
        contract.path || "-",
        authSummary(contract.auth),
        itemsPathSummary(contract.response),
      ]),
    }),
    variants.length > 0 && React.createElement(MiniTable, {
      title: "Variants",
      columns: ["Variant", "Operation", "Fixed Raw", "Fixed Semantic"],
      rows: variants.map((variant) => [
        variant.variant_id || "-",
        variant.operation_id || "-",
        inlineJson(variant.fixed_raw_arguments),
        inlineJson(variant.fixed_semantic_arguments),
      ]),
    }),
    checks.length > 0 && React.createElement(EndpointChecksTable, { checks })
  );
}

function OperationContractReview({ operationId, contract, catalog, endpointChecks }) {
  const resource = catalog?.resources?.[contract.resource_id] || {};
  const variants = Object.values(catalog?.operation_variants || {}).filter((variant) => variant.operation_id === operationId);
  const checks = checksFor(endpointChecks, {
    operationIds: [operationId],
    variantIds: variants.map((item) => item.variant_id),
  });
  return React.createElement("div", { className: "review-panel" },
    React.createElement("div", { className: "review-head" },
      React.createElement("div", null,
        React.createElement("h3", null, "Contract Review"),
        React.createElement("div", { className: "subtitle" }, `${contract.method || "-"} ${contract.path || ""}`)
      )
    ),
    React.createElement(ReviewStats, { stats: {
      resource: contract.resource_id,
      base_url: resource.base_url,
      auth: authSummary(contract.auth),
      variants: variants.length,
      checks: checks.length,
    } }),
    React.createElement(ContractFieldsTable, { title: "Request", fields: contract.request }),
    React.createElement(ResponseFieldsTable, { response: contract.response }),
    variants.length > 0 && React.createElement(MiniTable, {
      title: "Variants",
      columns: ["Variant", "Capability", "Fixed Raw", "Verification"],
      rows: variants.map((variant) => [
        variant.variant_id || "-",
        variant.capability_id || "-",
        inlineJson(variant.fixed_raw_arguments),
        inlineJson(variant.verification?.sample_semantic_arguments),
      ]),
    }),
    checks.length > 0 && React.createElement(EndpointChecksTable, { checks })
  );
}

function OperationVariantReview({ variantId, variant, catalog, endpointChecks }) {
  const contract = catalog?.operation_contracts?.[variant.operation_id] || {};
  const checks = checksFor(endpointChecks, {
    capabilityId: variant.capability_id,
    operationIds: [variant.operation_id],
    variantIds: [variantId],
  });
  return React.createElement("div", { className: "review-panel" },
    React.createElement("div", { className: "review-head" },
      React.createElement("div", null,
        React.createElement("h3", null, "Variant Review"),
        React.createElement("div", { className: "subtitle" }, variant.operation_id || "No operation")
      )
    ),
    React.createElement(ReviewStats, { stats: {
      capability: variant.capability_id,
      operation: variant.operation_id,
      method: contract.method,
      path: contract.path,
      checks: checks.length,
    } }),
    React.createElement(DefinitionList, { title: "Fixed Arguments", data: compactObject({
      fixed_raw_arguments: inlineJson(variant.fixed_raw_arguments),
      fixed_semantic_arguments: inlineJson(variant.fixed_semantic_arguments),
      sample_semantic_arguments: inlineJson(variant.verification?.sample_semantic_arguments),
    }) }),
    checks.length > 0 && React.createElement(EndpointChecksTable, { checks })
  );
}

function ReviewStats({ stats }) {
  const entries = Object.entries(compactObject(stats || {}));
  if (entries.length === 0) return null;
  return React.createElement("div", { className: "review-stats" },
    entries.map(([key, value]) =>
      React.createElement("div", { className: "review-stat", key },
        React.createElement("div", { className: "review-stat-label" }, key),
        React.createElement("div", { className: "review-stat-value" }, String(value))
      )
    )
  );
}

function ContractFieldsTable({ title, fields }) {
  const rows = [];
  for (const [location, values] of Object.entries(fields || {})) {
    if (!values || typeof values !== "object") continue;
    for (const [name, spec] of Object.entries(values)) {
      rows.push([
        location,
        name,
        spec?.semantic_type || spec?.kind || "-",
        fieldRuleSummary(spec),
      ]);
    }
  }
  if (rows.length === 0) return null;
  return React.createElement(MiniTable, {
    title,
    columns: ["In", "Field", "Semantic Type", "Rule"],
    rows,
  });
}

function fieldRuleSummary(spec) {
  if (!spec || typeof spec !== "object") return "-";
  const rules = [];
  if (spec.required) rules.push("required");
  if (spec.default !== undefined) rules.push(`default ${spec.default}`);
  if (spec.enum) rules.push(`enum ${inlineJson(spec.enum)}`);
  if (spec.pattern) rules.push(`pattern ${spec.pattern}`);
  if (spec.transform) rules.push(`transform ${inlineJson(spec.transform)}`);
  if (spec.min_length !== undefined) rules.push(`min ${spec.min_length}`);
  if (spec.max_length !== undefined) rules.push(`max ${spec.max_length}`);
  return rules.join(" · ") || "-";
}

function ResponseFieldsTable({ response }) {
  const fields = response?.fields || {};
  const rows = Object.entries(fields).map(([field, spec]) => [
    field,
    spec?.semantic_type || "-",
  ]);
  return React.createElement("div", { className: "detail-section" },
    React.createElement("h3", null, "Response"),
    React.createElement(DefinitionList, { title: "Paths", data: compactObject({
      items_path: itemsPathSummary(response),
      count_path: response?.count_path,
      success: inlineJson(response?.success),
      error: inlineJson(response?.error),
    }) }),
    rows.length > 0 && React.createElement(MiniTable, {
      title: "Field Mappings",
      columns: ["Raw Field", "Semantic Type"],
      rows,
    })
  );
}

function EndpointChecksTable({ checks }) {
  return React.createElement(MiniTable, {
    title: "Endpoint Checks",
    columns: ["Checked", "Operation", "Variant", "Type", "Status", "Result"],
    rows: checks.slice(0, 12).map((check) => [
      check.checked_at || check.created_at || "-",
      check.operation_id || "-",
      check.variant_id || "-",
      check.check_type || "-",
      check.status || "-",
      check.result_status || check.error_message || "-",
    ]),
  });
}

function MiniTable({ title, columns, rows }) {
  return React.createElement("div", { className: "detail-section" },
    title && React.createElement("h3", null, title),
    React.createElement("div", { className: "mini-table-wrap" },
      React.createElement("table", { className: "mini-table" },
        React.createElement("thead", null,
          React.createElement("tr", null, columns.map((column) => React.createElement("th", { key: column }, column)))
        ),
        React.createElement("tbody", null,
          rows.map((row, rowIndex) =>
            React.createElement("tr", { key: `${title || "table"}:${rowIndex}` },
              row.map((value, colIndex) =>
                React.createElement("td", { className: "wrap", key: `${rowIndex}:${colIndex}` }, String(value || "-"))
              )
            )
          )
        )
      )
    )
  );
}

function CapabilityTrace({ provenance }) {
  const endpoints = Array.isArray(provenance.endpoints) ? provenance.endpoints : [];
  return React.createElement("div", { className: "trace-panel" },
    React.createElement("div", { className: "trace-head" },
      React.createElement("div", null,
        React.createElement("h3", null, "Capability Trace"),
        React.createElement("div", { className: "subtitle" }, provenance.source_document_id || "No source document")
      )
    ),
    React.createElement(DefinitionList, {
      title: "Source",
      data: compactObject({
        source_document_id: provenance.source_document_id,
        source_file_name: provenance.source_file_name,
        source_path: provenance.source_path,
        evidence_snapshot_id: provenance.evidence_snapshot_id,
      }),
    }),
    Array.isArray(provenance.source_section_ids) && provenance.source_section_ids.length > 0 &&
      React.createElement(ChipsSection, { title: "Source Sections", values: provenance.source_section_ids }),
    Array.isArray(provenance.operation_ids) && provenance.operation_ids.length > 0 &&
      React.createElement(ChipsSection, { title: "Operations", values: provenance.operation_ids }),
    Array.isArray(provenance.variant_ids) && provenance.variant_ids.length > 0 &&
      React.createElement(ChipsSection, { title: "Variants", values: provenance.variant_ids }),
    endpoints.length > 0 && React.createElement(EndpointTable, { endpoints })
  );
}

function EndpointTable({ endpoints }) {
  return React.createElement("div", { className: "detail-section" },
    React.createElement("h3", null, "Endpoints"),
    React.createElement("div", { className: "mini-table-wrap" },
      React.createElement("table", { className: "mini-table" },
        React.createElement("thead", null,
          React.createElement("tr", null,
            React.createElement("th", null, "Operation"),
            React.createElement("th", null, "Method"),
            React.createElement("th", null, "Path"),
            React.createElement("th", null, "Provider"),
            React.createElement("th", null, "Resource")
          )
        ),
        React.createElement("tbody", null,
          endpoints.map((endpoint, index) =>
            React.createElement("tr", { key: `${endpoint.operation_id || "endpoint"}:${index}` },
              React.createElement("td", { className: "wrap" }, endpoint.operation_id || "-"),
              React.createElement("td", null, endpoint.method || "-"),
              React.createElement("td", { className: "wrap" }, endpoint.path || "-"),
              React.createElement("td", null, endpoint.provider || "-"),
              React.createElement("td", { className: "wrap" }, endpoint.resource_id || "-")
            )
          )
        )
      )
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

function sectionRows(catalog, sources, proposals, section, activeGraphSection = "entities", activeExecutionSection = "operations") {
  if (section === "proposals") {
    return proposalRows(proposals);
  }
  if (section === "semantic_graph") {
    return graphRows(catalog, activeGraphSection);
  }
  if (section === "execution") {
    return executionRows(catalog, activeExecutionSection);
  }
  if (!catalog) return [];
  const source = catalog[section] || {};
  return Object.entries(source).map(([key, value]) => {
    const data = value && typeof value === "object" ? value : {};
    return {
      key,
      section,
      value: data,
      type: section === "semantic_types" ? "semantic_type" : data.type,
      entity: data.entity,
      kind: section.slice(0, -1),
      summary: summaryFor(section, data),
    };
  });
}

function graphRows(catalog, activeGraphSection) {
  if (!catalog) return [];
  const definition = GRAPH_SECTIONS.find((item) => item.key === activeGraphSection) || GRAPH_SECTIONS[0];
  const source = catalog[definition.key] || {};
  return Object.entries(source).map(([key, value]) => {
    const data = value && typeof value === "object" ? value : {};
    return {
      key: `${definition.key}:${key}`,
      section: "semantic_graph",
      graphSection: definition.key,
      value: { ...data, __catalog_section: definition.key },
      type: definition.label,
      entity: data.entity_id || data.entity,
      kind: definition.label,
      summary: summaryFor(definition.key, data) || key,
    };
  });
}

function executionRows(catalog, activeExecutionSection) {
  if (!catalog) return [];
  const definition = EXECUTION_SECTIONS.find((item) => item.key === activeExecutionSection) || EXECUTION_SECTIONS[0];
  const source = catalog[definition.key] || {};
  return Object.entries(source).map(([key, value]) => {
    const data = value && typeof value === "object" ? value : {};
    return {
      key: `${definition.key}:${key}`,
      section: "execution",
      executionSection: definition.key,
      value: { ...data, __catalog_section: definition.key },
      type: definition.label,
      entity: data.entity,
      kind: definition.label,
      summary: summaryFor(definition.key, data) || key,
    };
  });
}

function pageRows(pageData, activeSection, activeGraphSection, activeExecutionSection) {
  const section = pageData?.section || activeSection;
  const items = Array.isArray(pageData?.items) ? pageData.items : [];
  return items.map((item) => {
    const key = String(item.id || "");
    const data = item.value && typeof item.value === "object" ? item.value : {};
    const graphSection = activeSection === "semantic_graph" ? activeGraphSection : null;
    const executionSection = activeSection === "execution" ? activeExecutionSection : null;
    return {
      key: executionSection || graphSection ? `${section}:${key}` : key,
      section: activeSection,
      graphSection,
      executionSection,
      value: { ...data, __catalog_section: section },
      type: executionSection
        ? (EXECUTION_SECTIONS.find((entry) => entry.key === section)?.label || section)
        : graphSection
          ? (GRAPH_SECTIONS.find((entry) => entry.key === section)?.label || section)
          : data.type,
      entity: data.entity_id || data.entity,
      kind: executionSection || graphSection ? section : activeSection.slice(0, -1),
      summary: summaryFor(section, data) || key,
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
  return rows
    .filter((proposal) => proposal.status === "pending_review")
    .filter((proposal) => Number(proposal.item_count || 0) > 0)
    .map((proposal) => ({
      key: proposal.id,
      section: "proposals",
      value: proposal,
      type: proposal.status,
      entity: proposal.kind,
      kind: "proposal",
      summary: proposalSummary(proposal),
    }));
}

function proposalSummary(proposal) {
  const summary = proposal?.payload?.summary;
  if (summary && typeof summary === "object") {
    if (summary.capability_id) return summary.capability_id;
    const counts = Object.entries(summary)
      .filter(([, value]) => typeof value === "number" && value > 0)
      .map(([key, value]) => `${key}:${value}`)
      .slice(0, 3);
    if (counts.length > 0) return counts.join(" ");
  }
  return proposal.source_document_id || proposal.kind || "";
}

function allRows(catalog, sources, proposals) {
  return [
    ...sectionRows(catalog, sources, proposals, "capabilities"),
    ...GRAPH_SECTIONS.flatMap((section) => graphRows(catalog, section.key)),
    ...EXECUTION_SECTIONS.flatMap((section) => executionRows(catalog, section.key)),
    ...sectionRows(catalog, sources, proposals, "proposals"),
  ];
}

function sectionLabel(sectionKey) {
  return SECTIONS.find((section) => section.key === sectionKey)?.label || sectionKey;
}

function summaryFor(section, data) {
  if (!data || typeof data !== "object") return "";
  if (data.description) return data.description;
  if (section === "proposals") return proposalSummary(data);
  if (section === "capabilities") return data.description_ko || data.description || "";
  if (section === "entities") return data.description_ko || data.name_ko || data.entity_type || "";
  if (section === "entity_identifiers") return `${data.entity_id || "-"} -> ${data.semantic_type_id || "-"}`;
  if (section === "semantic_types") return data.description_ko || data.entity || "";
  if (section === "semantic_join_rules") return `${data.from_semantic_type_id || "-"} -> ${data.to_semantic_type_id || "-"}`;
  if (section === "capability_entity_links") return `${data.capability_id || "-"} ${data.role || ""} ${data.entity_id || "-"}`;
  if (section === "capability_dependencies") return `${data.capability_id || "-"} -> ${data.depends_on_capability_id || "-"}`;
  if (section === "planning_examples") return data.question || "";
  if (section === "resources") return data.base_url || data.name_ko || "";
  if (section === "operations") return `${data.method || "-"} ${data.path || ""}`.trim();
  if (section === "operation_contracts") return `${data.provider || "-"} ${data.method || ""} ${data.path || ""}`.trim();
  if (section === "operation_variants") return `${data.operation_id || "-"} -> ${data.capability_id || "-"}`;
  if (section === "field_mappings") return `${data.raw_name || "-"} -> ${data.semantic_type_id || "-"}`;
  return "";
}

function stripExecutionKey(key) {
  const text = String(key || "");
  const index = text.indexOf(":");
  return index >= 0 ? text.slice(index + 1) : text;
}

function capabilityImplementations(catalog) {
  const value = catalog?.capability_implementations;
  if (Array.isArray(value)) return value.filter((item) => item && typeof item === "object");
  if (value && typeof value === "object") {
    return Object.values(value).flatMap((item) => Array.isArray(item) ? item : [item]).filter((item) => item && typeof item === "object");
  }
  return [];
}

function checksFor(endpointChecks, { capabilityId, operationIds = [], variantIds = [] }) {
  const checks = Array.isArray(endpointChecks?.endpoint_checks) ? endpointChecks.endpoint_checks : [];
  const operations = new Set(operationIds.filter(Boolean));
  const variants = new Set(variantIds.filter(Boolean));
  return checks
    .filter((check) =>
      (capabilityId && check.capability_id === capabilityId) ||
      (check.operation_id && operations.has(check.operation_id)) ||
      (check.variant_id && variants.has(check.variant_id))
    )
    .sort((left, right) => String(right.checked_at || right.created_at || "").localeCompare(String(left.checked_at || left.created_at || "")));
}

function uniqueBy(values, keyFn) {
  const seen = new Set();
  const out = [];
  values.forEach((value) => {
    const key = keyFn(value);
    if (!key || seen.has(key)) return;
    seen.add(key);
    out.push(value);
  });
  return out;
}

function authSummary(auth) {
  if (!auth || typeof auth !== "object") return "-";
  const envNames = Array.isArray(auth.env_names) ? auth.env_names.join(",") : "";
  return [auth.parameter, auth.in, envNames].filter(Boolean).join(" · ") || "-";
}

function itemsPathSummary(response) {
  const value = response?.items_path;
  if (Array.isArray(value)) return value.join(", ");
  return value || "-";
}

function inlineJson(value) {
  if (value === undefined || value === null) return "";
  if (typeof value === "string") return value;
  if (typeof value !== "object") return String(value);
  if (Array.isArray(value) && value.length === 0) return "";
  if (!Array.isArray(value) && Object.keys(value).length === 0) return "";
  return JSON.stringify(value);
}

function filterRows(rows, query) {
  const text = query.trim().toLowerCase();
  if (!text) return rows;
  return rows.filter((row) => JSON.stringify({ key: row.key, value: row.value }).toLowerCase().includes(text));
}

function sectionCounts(catalog, sources, proposals) {
  return {
    capabilities: countObject(catalog?.capabilities),
    execution: countObject(catalog?.operations),
    semantic_graph: countObject(catalog?.entities),
    proposals: proposalRows(proposals).length,
    semantic_types: countObject(catalog?.semantic_types),
    entities: countObject(catalog?.entities),
    entity_identifiers: countObject(catalog?.entity_identifiers),
    semantic_join_rules: countObject(catalog?.semantic_join_rules),
    capability_entity_links: countObject(catalog?.capability_entity_links),
    capability_dependencies: countObject(catalog?.capability_dependencies),
    planning_examples: countObject(catalog?.planning_examples),
    resources: countObject(catalog?.resources),
    operations: countObject(catalog?.operations),
    operation_contracts: countObject(catalog?.operation_contracts),
    operation_variants: countObject(catalog?.operation_variants),
    field_mappings: countObject(catalog?.field_mappings),
    capability_implementations: Array.isArray(catalog?.capability_implementations)
      ? catalog.capability_implementations.length
      : countObject(catalog?.capability_implementations),
  };
}

function countObject(value) {
  return value && typeof value === "object" ? Object.keys(value).length : 0;
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
