if (!window.React || !window.ReactDOM) {
  document.getElementById("root").innerHTML =
    '<div class="app"><main class="main"><div class="error">React runtime could not be loaded.</div></main></div>';
  throw new Error("React runtime could not be loaded");
}

const { useEffect, useMemo, useRef, useState } = React;

const SECTIONS = [
  { key: "sources", label: "Sources", icon: "S" },
  { key: "ingestion_runs", label: "Ingestion Runs", icon: "I" },
  { key: "secrets", label: "Secrets", icon: "K" },
  { key: "capabilities", label: "Capabilities", icon: "C" },
  { key: "semantic_graph", label: "Semantic Graph", icon: "S" },
  { key: "execution", label: "Execution", icon: "E" },
  { key: "proposals", label: "Governance", icon: "G" },
  { key: "catalog_versions", label: "Versions", icon: "V" },
];

const SECTION_ROUTES = {
  sources: "sources",
  ingestion_runs: "ingestion-runs",
  secrets: "secrets",
  capabilities: "capabilities",
  semantic_graph: "semantic-graph",
  execution: "execution",
  proposals: "governance",
  catalog_versions: "catalog-versions",
};

const ROUTE_SECTIONS = Object.fromEntries(Object.entries(SECTION_ROUTES).map(([key, value]) => [value, key]));
const DEFAULT_SECTION = "capabilities";
const MIN_MANUAL_LOADING_MS = 450;

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
  { key: "endpoint_checks", label: "Endpoint Checks" },
];

const EDITABLE_CATALOG_SECTIONS = new Set([
  "planning_examples",
  "capabilities",
  "semantic_types",
  "entities",
  "semantic_join_rules",
  "capability_entity_links",
  "capability_dependencies",
]);

async function fetchJson(url) {
  const response = await fetch(resolveDashboardUrl(url));
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json();
}

function resolveDashboardUrl(url) {
  if (/^https?:\/\//.test(url) || url.startsWith("/")) return url;
  if (url.startsWith("api/")) {
    const base = dashboardBasePath();
    return `${base}/api/${url.slice("api/".length)}`;
  }
  return url;
}

function dashboardBasePath() {
  const segments = window.location.pathname.split("/").filter(Boolean);
  const routeIndex = segments.findIndex((segment) => ROUTE_SECTIONS[segment]);
  if (routeIndex >= 0) {
    return routeIndex === 0 ? "" : `/${segments.slice(0, routeIndex).join("/")}`;
  }
  const normalized = window.location.pathname.replace(/\/+$/, "");
  return normalized === "" || normalized === "/" ? "" : normalized;
}

function sectionFromLocation() {
  const segments = window.location.pathname.split("/").filter(Boolean);
  const route = [...segments].reverse().find((segment) => ROUTE_SECTIONS[segment]);
  return route ? ROUTE_SECTIONS[route] : DEFAULT_SECTION;
}

function hasSectionRoute() {
  return window.location.pathname.split("/").filter(Boolean).some((segment) => ROUTE_SECTIONS[segment]);
}

function sectionUrl(section) {
  const base = dashboardBasePath();
  const route = SECTION_ROUTES[section] || SECTION_ROUTES[DEFAULT_SECTION];
  return `${base}/${route}`;
}

function App() {
  const [catalog, setCatalog] = useState(null);
  const [meta, setMeta] = useState(null);
  const [sources, setSources] = useState(null);
  const [ingestionRuns, setIngestionRuns] = useState(null);
  const [secrets, setSecrets] = useState(null);
  const [sourceSummary, setSourceSummary] = useState(null);
  const [proposals, setProposals] = useState(null);
  const [catalogVersions, setCatalogVersions] = useState(null);
  const [proposalDetails, setProposalDetails] = useState({});
  const [catalogVersionDetails, setCatalogVersionDetails] = useState({});
  const [catalogView, setCatalogView] = useState({ mode: "current", version: null, snapshot: null, diff: null });
  const [endpointChecks, setEndpointChecks] = useState(null);
  const [activeSection, setActiveSection] = useState(sectionFromLocation);
  const [activeGraphSection, setActiveGraphSection] = useState("entities");
  const [activeExecutionSection, setActiveExecutionSection] = useState("operations");
  const [selectedKey, setSelectedKey] = useState(null);
  const [pageData, setPageData] = useState(null);
  const [pageOffset, setPageOffset] = useState(0);
  const [pageLoading, setPageLoading] = useState(false);
  const [query, setQuery] = useState("");
  const [sourceModal, setSourceModal] = useState(null);
  const [sourceDeleteModal, setSourceDeleteModal] = useState(null);
  const [sourceIngestModal, setSourceIngestModal] = useState(null);
  const [secretModal, setSecretModal] = useState(null);
  const [catalogEditModal, setCatalogEditModal] = useState(null);
  const [catalogDeleteModal, setCatalogDeleteModal] = useState(null);
  const [proposalItemEditModal, setProposalItemEditModal] = useState(null);
  const [loading, setLoading] = useState(false);
  const [reloadingRuns, setReloadingRuns] = useState(false);
  const [error, setError] = useState(null);
  const pageSize = 25;

  async function load() {
    setLoading(true);
    setError(null);
    try {
      const [catalogRes, metaRes, sourcesRes, runsRes, secretsRes, sourceSummaryRes, proposalsRes, versionsRes, checksRes] = await Promise.all([
        fetchJson("api/catalog"),
        fetchJson("api/catalog/meta"),
        fetchJson("api/sources"),
        fetchJson("api/ingestion/runs?limit=100").catch(() => ({ ingestion_runs: [] })),
        fetchJson("api/secrets").catch(() => ({ secrets: [] })),
        fetchJson("api/sources/summary"),
        fetchJson("api/proposals?status=pending_review&limit=100"),
        fetchJson("api/catalog/versions?limit=100").catch(() => ({ catalog_versions: [] })),
        fetchJson("api/semantic/execution/checks?limit=500").catch(() => ({ endpoint_checks: [] })),
      ]);
      setCatalog(catalogRes);
      setMeta(metaRes);
      setSources(sourcesRes);
      setIngestionRuns(runsRes);
      setSecrets(secretsRes);
      setSourceSummary(sourceSummaryRes);
      setProposals(proposalsRes);
      setCatalogVersions(versionsRes);
      setProposalDetails({});
      setCatalogVersionDetails({});
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

  useEffect(() => {
    if (!hasSectionRoute()) {
      window.history.replaceState({}, "", sectionUrl(activeSection));
    }
  }, []);

  useEffect(() => {
    function onPopState() {
      setActiveSection(sectionFromLocation());
    }
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  function navigateSection(section, options = {}) {
    const nextSection = SECTION_ROUTES[section] ? section : DEFAULT_SECTION;
    const nextUrl = sectionUrl(nextSection);
    if (window.location.pathname !== nextUrl) {
      if (options.replace) {
        window.history.replaceState({}, "", nextUrl);
      } else {
        window.history.pushState({}, "", nextUrl);
      }
    }
    setActiveSection(nextSection);
  }

  async function refreshSecrets() {
    const data = await fetchJson("api/secrets").catch(() => ({ secrets: [] }));
    setSecrets(data);
    return data;
  }

  async function refreshSources() {
    const [sourcesRes, sourceSummaryRes, metaRes] = await Promise.all([
      fetchJson("api/sources"),
      fetchJson("api/sources/summary"),
      fetchJson("api/catalog/meta"),
    ]);
    setSources(sourcesRes);
    setSourceSummary(sourceSummaryRes);
    setMeta(metaRes);
    return sourcesRes;
  }

  async function refreshIngestionRuns(options = {}) {
    const startedAt = Date.now();
    if (!options.silent) setReloadingRuns(true);
    try {
      const data = await fetchJson("api/ingestion/runs?limit=100").catch(() => ({ ingestion_runs: [] }));
      setIngestionRuns(data);
      return data;
    } finally {
      if (!options.silent) {
        const remaining = MIN_MANUAL_LOADING_MS - (Date.now() - startedAt);
        if (remaining > 0) {
          window.setTimeout(() => setReloadingRuns(false), remaining);
        } else {
          setReloadingRuns(false);
        }
      }
    }
  }

  async function loadPage(section, offset) {
    if (catalogView.mode === "version") {
      setPageData(null);
      return;
    }
    if (section === "proposals" || section === "sources" || section === "ingestion_runs" || section === "secrets" || section === "catalog_versions") {
      setPageData(null);
      return;
    }
    if (section === "execution" && activeExecutionSection === "endpoint_checks") {
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
  }, [activeSection, activeGraphSection, activeExecutionSection, pageOffset, catalogView.mode]);

  useEffect(() => {
    if (activeSection !== "ingestion_runs") return undefined;
    refreshIngestionRuns({ silent: true });
    const timer = window.setInterval(() => refreshIngestionRuns({ silent: true }), 5000);
    return () => window.clearInterval(timer);
  }, [activeSection]);

  const isVersionMode = catalogView.mode === "version";
  const visibleCatalog = isVersionMode ? catalogView.snapshot : catalog;
  const visiblePageData = isVersionMode ? null : pageData;
  const visibleSources = isVersionMode ? null : sources;
  const visibleIngestionRuns = isVersionMode ? null : ingestionRuns;
  const visibleSecrets = isVersionMode ? null : secrets;
  const visibleProposals = isVersionMode ? null : proposals;
  const visibleEndpointChecks = isVersionMode ? null : endpointChecks;
  const rows = useMemo(
    () => visiblePageData ? pageRows(visiblePageData, activeSection, activeGraphSection, activeExecutionSection) : sectionRows(visibleCatalog, visibleSources, visibleIngestionRuns, visibleSecrets, visibleProposals, catalogVersions, activeSection, activeGraphSection, activeExecutionSection, visibleEndpointChecks),
    [visibleCatalog, visibleSources, visibleIngestionRuns, visibleSecrets, visibleProposals, catalogVersions, visiblePageData, activeSection, activeGraphSection, activeExecutionSection, visibleEndpointChecks]
  );
  const globalRows = useMemo(() => allRows(visibleCatalog, visibleSources, visibleIngestionRuns, visibleSecrets, visibleProposals, catalogVersions, visibleEndpointChecks), [visibleCatalog, visibleSources, visibleIngestionRuns, visibleSecrets, visibleProposals, catalogVersions, visibleEndpointChecks]);
  const searchResults = useMemo(() => filterRows(globalRows, query), [globalRows, query]);
  const selected = useMemo(() => {
    let row = null;
    if (selectedKey) {
      row = rows.find((row) => row.key === selectedKey) || rows[0] || null;
    } else {
      row = rows[0] || null;
    }
    if (row?.section === "proposals" && proposalDetails[row.key]) {
      return {
        ...row,
        value: {
          ...row.value,
          __proposal_detail: proposalDetails[row.key],
        },
      };
    }
    if (row?.section === "catalog_versions" && catalogVersionDetails[row.key]) {
      return {
        ...row,
        value: {
          ...row.value,
          __catalog_version_detail: catalogVersionDetails[row.key].detail,
          __catalog_version_diff: catalogVersionDetails[row.key].diff,
        },
      };
    }
    return row;
  }, [rows, selectedKey, proposalDetails, catalogVersionDetails]);

  useEffect(() => {
    if (activeSection !== "proposals" || !selected?.key || proposalDetails[selected.key]) return undefined;
    let cancelled = false;
    fetchJson(`api/proposals/${encodeURIComponent(selected.key)}`)
      .then((detail) => {
        if (!cancelled) {
          setProposalDetails((prev) => ({ ...prev, [selected.key]: detail }));
        }
      })
      .catch((err) => {
        if (!cancelled) setError(err.message || String(err));
      });
    return () => {
      cancelled = true;
    };
  }, [activeSection, selected?.key, proposalDetails]);

  useEffect(() => {
    if (activeSection !== "catalog_versions" || !selected?.key || catalogVersionDetails[selected.key]) return undefined;
    let cancelled = false;
    fetchCatalogVersionDetail(selected.key)
      .then((detail) => {
        if (!cancelled) {
          setCatalogVersionDetails((prev) => ({ ...prev, [selected.key]: detail }));
        }
      })
      .catch((err) => {
        if (!cancelled) setError(err.message || String(err));
      });
    return () => {
      cancelled = true;
    };
  }, [activeSection, selected?.key, catalogVersionDetails]);

  const counts = useMemo(() => sectionCounts(visibleCatalog, visibleSources, visibleIngestionRuns, visibleSecrets, visibleProposals, catalogVersions, visibleEndpointChecks), [visibleCatalog, visibleSources, visibleIngestionRuns, visibleSecrets, visibleProposals, catalogVersions, visibleEndpointChecks]);

  async function fetchCatalogVersionDetail(versionId) {
    const [detail, diff] = await Promise.all([
      fetchJson(`api/catalog/versions/${encodeURIComponent(versionId)}`),
      fetchJson(`api/catalog/versions/${encodeURIComponent(versionId)}/diff`).catch(() => null),
    ]);
    return { detail, diff };
  }

  async function viewCatalogVersionSnapshot(versionId) {
    if (!versionId) return;
    setLoading(true);
    setError(null);
    try {
      const cached = catalogVersionDetails[versionId] || await fetchCatalogVersionDetail(versionId);
      setCatalogVersionDetails((prev) => ({ ...prev, [versionId]: cached }));
      const version = cached.detail?.catalog_version || {};
      setCatalogView({
        mode: "version",
        version,
        snapshot: version.snapshot || {},
        diff: cached.diff || null,
      });
      navigateSection("capabilities");
      setSelectedKey(null);
      setPageData(null);
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
    }
  }

  function backToCurrentCatalog() {
    setCatalogView({ mode: "current", version: null, snapshot: null, diff: null });
    setSelectedKey(null);
    setPageData(null);
  }

  function downloadCatalogVersionSnapshot(versionId) {
    if (!versionId) return;
    const link = document.createElement("a");
    link.href = resolveDashboardUrl(`api/catalog/versions/${encodeURIComponent(versionId)}/export`);
    link.download = `${versionId}.catalog-snapshot.json`;
    document.body.appendChild(link);
    link.click();
    link.remove();
  }

  async function restoreCatalogVersion(versionId) {
    if (!versionId) return;
    const confirmed = window.confirm(`Restore catalog from ${versionId}? This creates a new active version.`);
    if (!confirmed) return;
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(resolveDashboardUrl(`api/catalog/versions/${encodeURIComponent(versionId)}/restore`), { method: "POST" });
      if (!response.ok) {
        throw new Error(`${response.status} ${response.statusText}`);
      }
      backToCurrentCatalog();
      setCatalogVersionDetails({});
      await load();
      navigateSection("catalog_versions");
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
    }
  }

  async function createCatalogVersionSnapshot() {
    const note = window.prompt("Snapshot note", "manual catalog snapshot");
    if (note === null) return;
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(resolveDashboardUrl("api/catalog/versions"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ reason: "manual_snapshot", note }),
      });
      if (!response.ok) {
        throw new Error(`${response.status} ${response.statusText}`);
      }
      setCatalogVersionDetails({});
      await load();
      navigateSection("catalog_versions");
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
    }
  }

  async function applyProposal(proposalId) {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(resolveDashboardUrl(`api/proposals/${encodeURIComponent(proposalId)}/apply`), { method: "POST" });
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
      const response = await fetch(resolveDashboardUrl(`api/proposals/${encodeURIComponent(proposalId)}/reject`), { method: "POST" });
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

  async function saveProposalItem(editState, document) {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(resolveDashboardUrl(`api/proposals/${encodeURIComponent(editState.proposalId)}/items/${encodeURIComponent(editState.item.id)}`), {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(document),
      });
      if (!response.ok) {
        let detail = "";
        try {
          detail = (await response.json())?.detail || "";
        } catch {
          detail = await response.text();
        }
        throw new Error(`${response.status} ${response.statusText}: ${detail}`);
      }
      const detail = await fetchJson(`api/proposals/${encodeURIComponent(editState.proposalId)}`);
      setProposalDetails((prev) => ({ ...prev, [editState.proposalId]: detail }));
      setProposalItemEditModal(null);
    } catch (err) {
      setError(err.message || String(err));
      throw err;
    } finally {
      setLoading(false);
    }
  }

  async function uploadSource(form) {
    setLoading(true);
    setError(null);
    try {
      const body = new FormData();
      body.append("file", form.file);
      if (form.sourceId) body.append("source_id", form.sourceId);
      if (form.provider) body.append("provider", form.provider);
      if (form.providerNameKo) body.append("provider_name_ko", form.providerNameKo);
      if (form.title) body.append("title", form.title);
      if (form.authSecretRefs) body.append("auth_secret_refs", form.authSecretRefs);
      if (form.authParameterNames) body.append("auth_parameter_names", form.authParameterNames);
      const response = await fetch(resolveDashboardUrl("api/sources/upload"), { method: "POST", body });
      if (!response.ok) {
        let detail = "";
        try {
          detail = (await response.json())?.detail || "";
        } catch {
          detail = await response.text();
        }
        if (response.status === 409) {
          throw new Error(detail || "Source key already exists");
        }
        throw new Error(`${response.status} ${response.statusText}: ${detail}`);
      }
      const result = await response.json();
      navigateSection("sources", { replace: true });
      await refreshSources();
      setSourceModal(null);
      return result;
    } catch (err) {
      setError(err.message || String(err));
      throw err;
    } finally {
      setLoading(false);
    }
  }

  async function saveSource(form) {
    if (form.file) {
      return uploadSource(form);
    }
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(resolveDashboardUrl(`api/sources/${encodeURIComponent(form.id)}`), {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          provider: form.provider,
          provider_name_ko: form.providerNameKo,
          title: form.title,
          auth_secret_refs: form.authSecretRefs,
          auth_parameter_names: form.authParameterNames,
          status: form.status,
        }),
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`${response.status} ${response.statusText}: ${text}`);
      }
      const result = await response.json();
      await refreshSources();
      setSourceModal(null);
      return result;
    } catch (err) {
      setError(err.message || String(err));
      throw err;
    } finally {
      setLoading(false);
    }
  }

  async function openSourceDelete(source) {
    if (!source?.id) return;
    setLoading(true);
    setError(null);
    try {
      const plan = await fetchJson(`api/sources/${encodeURIComponent(source.id)}/delete-plan`);
      setSourceDeleteModal({ source, plan, mode: "archive", confirmText: "" });
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
    }
  }

  async function deleteSource(sourceId, mode) {
    if (!sourceId) return;
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(resolveDashboardUrl(`api/sources/${encodeURIComponent(sourceId)}/delete`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode }),
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`${response.status} ${response.statusText}: ${text}`);
      }
      setSelectedKey(null);
      setSourceDeleteModal(null);
      await refreshSources();
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
    }
  }

  async function startSourceIngestion(source, options = {}) {
    if (!source?.id) return;
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(resolveDashboardUrl(`api/sources/${encodeURIComponent(source.id)}/ingest`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          revision_id: source.revision_id,
          commit_mode: options.commitMode || "proposal",
          force: Boolean(options.force),
          requested_by: "dashboard",
          llm_mode: options.llmMode || undefined,
          llm_secret_ref: options.llmSecretRef || undefined,
          manual_llm_response: options.manualLlmResponse || undefined,
        }),
      });
      if (!response.ok) {
        let detail = "";
        try {
          detail = (await response.json())?.detail || "";
        } catch {
          detail = await response.text();
        }
        throw new Error(`${response.status} ${response.statusText}: ${detail}`);
      }
      const result = await response.json();
      await refreshIngestionRuns();
      setSourceIngestModal(null);
      navigateSection("ingestion_runs");
      setSelectedKey(result?.ingestion_run?.id || null);
      return result;
    } catch (err) {
      setError(err.message || String(err));
      throw err;
    } finally {
      setLoading(false);
    }
  }

  async function saveSecret(form) {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(resolveDashboardUrl("api/secrets"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(form),
      });
      if (!response.ok) {
        let detail = "";
        try {
          detail = (await response.json())?.detail || "";
        } catch {
          detail = await response.text();
        }
        if (response.status === 409) {
          throw new Error(detail || "Secret key already exists");
        }
        throw new Error(`${response.status} ${response.statusText}: ${detail}`);
      }
      const result = await response.json();
      await refreshSecrets();
      setSecretModal(null);
      return result;
    } catch (err) {
      setError(err.message || String(err));
      throw err;
    } finally {
      setLoading(false);
    }
  }

  async function deleteSecret(secretId) {
    if (!secretId) return;
    if (!window.confirm(`Delete ${secretId}?`)) return;
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(resolveDashboardUrl(`api/secrets/${encodeURIComponent(secretId)}`), { method: "DELETE" });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`${response.status} ${response.statusText}: ${text}`);
      }
      setSelectedKey(null);
      await refreshSecrets();
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
    }
  }

  async function refreshCatalogView() {
    const [catalogRes, metaRes] = await Promise.all([
      fetchJson("api/catalog"),
      fetchJson("api/catalog/meta"),
    ]);
    setCatalog(catalogRes);
    setMeta(metaRes);
    await loadPage(activeSection, pageOffset);
  }

  function editableCatalogRef(row) {
    const section = row?.value?.__catalog_section || row?.section;
    if (!section || !EDITABLE_CATALOG_SECTIONS.has(section)) return null;
    const id = row?.value?.id || row?.value?.operation_id || row?.value?.variant_id || row?.key;
    if (!id) return null;
    return { section, id: String(id), item: row.value };
  }

  async function saveCatalogItem(ref, payload) {
    if (!ref?.section || !ref?.id) return;
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(resolveDashboardUrl(`api/catalog/sections/${encodeURIComponent(ref.section)}/${encodeURIComponent(ref.id)}`), {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`${response.status} ${response.statusText}: ${text}`);
      }
      const result = await response.json();
      setCatalogEditModal(null);
      await refreshCatalogView();
      setSelectedKey(ref.id);
      return result;
    } catch (err) {
      setError(err.message || String(err));
      throw err;
    } finally {
      setLoading(false);
    }
  }

  async function openCatalogDelete(ref) {
    if (!ref?.section || !ref?.id) return;
    setLoading(true);
    setError(null);
    try {
      const plan = await fetchJson(`api/catalog/sections/${encodeURIComponent(ref.section)}/${encodeURIComponent(ref.id)}/delete-plan`);
      setCatalogDeleteModal({ ref, plan, mode: plan.default_mode || "deprecate", confirmText: "" });
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
    }
  }

  async function deleteCatalogItem(state) {
    const ref = state?.ref;
    const mode = state?.mode || "deprecate";
    if (!ref?.section || !ref?.id) return;
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(resolveDashboardUrl(`api/catalog/sections/${encodeURIComponent(ref.section)}/${encodeURIComponent(ref.id)}/delete`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode }),
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`${response.status} ${response.statusText}: ${text}`);
      }
      setSelectedKey(null);
      setCatalogDeleteModal(null);
      await refreshCatalogView();
    } catch (err) {
      setError(err.message || String(err));
      throw err;
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
          navigateSection(row.section);
          if (row.executionSection) setActiveExecutionSection(row.executionSection);
          if (row.graphSection) setActiveGraphSection(row.graphSection);
          setSelectedKey(row.key);
        },
      }),
      React.createElement("div", { className: "toolbar" })
    ),
    React.createElement("div", { className: "shell" },
      React.createElement(Sidebar, { activeSection, setActiveSection: navigateSection, counts }),
      React.createElement("main", { className: "main" },
        error && React.createElement("div", { className: "error" }, error),
        isVersionMode && React.createElement(CatalogVersionBanner, {
          catalogView,
          onBack: backToCurrentCatalog,
        }),
        React.createElement(Kpis, { counts, catalog: visibleCatalog, meta, sourceSummary }),
        !isVersionMode && activeSection === "sources" && React.createElement(SectionToolbar, {
          title: "Sources",
          detail: "Upload source documents and manage source metadata",
          actionLabel: "Upload Source",
          onAction: () => setSourceModal({}),
        }),
        !isVersionMode && activeSection === "secrets" && React.createElement(SectionToolbar, {
          title: "Secrets",
          detail: "Register API keys once, then reference secret IDs from sources",
          actionLabel: "New Secret",
          onAction: () => setSecretModal({}),
        }),
        !isVersionMode && activeSection === "ingestion_runs" && React.createElement(SectionToolbar, {
          title: "Ingestion Runs",
          detail: "Track source ingestion graph execution and results",
          actionLabel: "Reload",
          onAction: refreshIngestionRuns,
          loading: reloadingRuns,
          loadingLabel: "Reloading",
        }),
        !isVersionMode && activeSection === "catalog_versions" && React.createElement(SectionToolbar, {
          title: "Versions",
          detail: "Create explicit catalog snapshots for backup, audit, and restore",
          actionLabel: "Create Snapshot",
          onAction: createCatalogVersionSnapshot,
          loading,
          loadingLabel: "Creating",
        }),
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
            catalog: visibleCatalog,
            endpointChecks: visibleEndpointChecks,
            isVersionMode,
            catalogView,
            onViewCatalogVersion: viewCatalogVersionSnapshot,
            onDownloadCatalogVersion: downloadCatalogVersionSnapshot,
            onRestoreCatalogVersion: restoreCatalogVersion,
            onApply: applyProposal,
            onReject: rejectProposal,
            onEditSource: (source) => setSourceModal(source || {}),
            onDeleteSource: openSourceDelete,
            onIngestSource: (source) => setSourceIngestModal(source || {}),
            onEditSecret: (secret) => setSecretModal(secret || {}),
            onDeleteSecret: deleteSecret,
            editableCatalogRef: isVersionMode ? null : editableCatalogRef,
            onEditCatalogItem: (ref) => setCatalogEditModal(ref),
            onDeleteCatalogItem: openCatalogDelete,
            onEditProposalItem: isVersionMode ? null : (editState) => setProposalItemEditModal(editState),
            loading,
          })
        )
      ),
      sourceModal !== null && React.createElement(SourceModal, {
        source: sourceModal,
        secrets,
        loading,
        onSave: saveSource,
        onClose: () => setSourceModal(null),
      }),
      sourceDeleteModal !== null && React.createElement(SourceDeleteModal, {
        state: sourceDeleteModal,
        loading,
        onChange: setSourceDeleteModal,
        onDelete: deleteSource,
        onClose: () => setSourceDeleteModal(null),
      }),
      sourceIngestModal !== null && React.createElement(SourceIngestModal, {
        source: sourceIngestModal,
        meta,
        secrets,
        loading,
        onStart: startSourceIngestion,
        onClose: () => setSourceIngestModal(null),
      }),
      secretModal !== null && React.createElement(SecretModal, {
        secret: secretModal,
        loading,
        onSave: saveSecret,
        onClose: () => setSecretModal(null),
      }),
      catalogEditModal !== null && React.createElement(CatalogEditModal, {
        refData: catalogEditModal,
        loading,
        onSave: saveCatalogItem,
        onClose: () => setCatalogEditModal(null),
      }),
      catalogDeleteModal !== null && React.createElement(CatalogDeleteModal, {
        state: catalogDeleteModal,
        loading,
        onChange: setCatalogDeleteModal,
        onDelete: deleteCatalogItem,
        onClose: () => setCatalogDeleteModal(null),
      }),
      proposalItemEditModal !== null && React.createElement(ProposalItemEditModal, {
        state: proposalItemEditModal,
        loading,
        onSave: saveProposalItem,
        onClose: () => setProposalItemEditModal(null),
      })
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
    React.createElement(Kpi, { label: "Sources", value: counts.sources || 0, detail: "Registered documents" }),
    React.createElement(Kpi, { label: "Capabilities", value: counts.capabilities || 0, detail: `${countObject(catalog?.capability_documents)} documents` }),
    React.createElement(Kpi, { label: "Entities", value: counts.entities || 0, detail: `${counts.semantic_join_rules || 0} join rules` }),
    React.createElement(Kpi, { label: "Operations", value: counts.operations || 0, detail: `${countObject(catalog?.operation_contracts)} contracts` }),
    React.createElement(Kpi, { label: "Proposals", value: counts.proposals || 0, detail: "Pending review" })
  );
}

function RequiredLabel({ children, required = false }) {
  return React.createElement("span", {
    className: required ? "field-label required" : "field-label",
  }, children);
}

function SourceModal({ source, secrets, loading, onSave, onClose }) {
  const isEdit = Boolean(source?.id);
  const canEdit = !isEdit || source?.source_kind === "registry";
  const fileInputRef = useRef(null);
  const [file, setFile] = useState(null);
  const [sourceId, setSourceId] = useState(source?.id || "");
  const [provider, setProvider] = useState(source?.provider || "");
  const [providerNameKo, setProviderNameKo] = useState(source?.provider_name_ko || "");
  const [title, setTitle] = useState(source?.title || "");
  const [status, setStatus] = useState(source?.status || "active");
  const [authSecretRefs, setAuthSecretRefs] = useState(displaySecretRefs(source?.auth_secret_refs));
  const [authParameterNames, setAuthParameterNames] = useState((source?.auth_parameter_names || []).join(","));
  const [message, setMessage] = useState("");
  const secretOptions = Array.isArray(secrets?.secrets) ? secrets.secrets : [];
  useEffect(() => {
    setFile(null);
    setSourceId(source?.id || "");
    setProvider(source?.provider || "");
    setProviderNameKo(source?.provider_name_ko || "");
    setTitle(source?.title || "");
    setStatus(source?.status || "active");
    setAuthSecretRefs(displaySecretRefs(source?.auth_secret_refs));
    setAuthParameterNames((source?.auth_parameter_names || []).join(","));
    setMessage("");
    if (fileInputRef.current) fileInputRef.current.value = "";
  }, [source?.id]);
  useEffect(() => {
    if (isEdit) return;
    const baseTitle = title || file?.name?.replace(/\.[^.]+$/, "") || "";
    setSourceId(suggestSourceId(provider, baseTitle));
  }, [file, provider, title, isEdit]);
  async function submit(event) {
    event.preventDefault();
    if (!isEdit && !file) {
      setMessage("Select a file");
      return;
    }
    setMessage("");
    try {
      const result = await onSave({
        id: source?.id,
        sourceId,
        file,
        provider,
        providerNameKo,
        title: title || file?.name || "",
        authSecretRefs,
        authParameterNames,
        status,
      });
      setMessage(`Saved ${result?.source?.id || "source"}`);
    } catch (err) {
      setMessage(err.message || String(err));
    }
  }
  return React.createElement("div", { className: "modal-backdrop", onMouseDown: onClose },
    React.createElement("section", {
      className: "modal",
      role: "dialog",
      "aria-modal": "true",
      onMouseDown: (event) => event.stopPropagation(),
    },
      React.createElement("div", { className: "modal-head" },
        React.createElement("div", null,
          React.createElement("div", { className: "card-title" }, isEdit ? "Edit Source" : "Upload Source"),
          React.createElement("div", { className: "subtitle" }, isEdit ? "Update source metadata" : "Files are stored in MinIO/S3 and registered as source revisions")
        ),
        React.createElement("button", {
          className: "icon-btn",
          onClick: onClose,
          type: "button",
          "aria-label": "Close",
        }, "x")
      ),
      message && React.createElement("div", { className: "upload-message" }, message),
      !canEdit && React.createElement("div", { className: "error" }, "Legacy source documents are read-only. Re-upload them to manage revisions."),
      React.createElement("form", { className: "modal-form", onSubmit: submit },
        !isEdit && React.createElement("label", null,
          React.createElement(RequiredLabel, { required: true }, "File"),
          React.createElement("input", {
            type: "file",
            ref: fileInputRef,
            required: true,
            onChange: (event) => {
              const selected = event.target.files?.[0] || null;
              setFile(selected);
              if (selected && !title) setTitle(selected.name.replace(/\.[^.]+$/, ""));
            },
          })
        ),
        React.createElement("label", null,
          React.createElement(RequiredLabel, { required: true }, "Title"),
          React.createElement("input", {
            value: title,
            disabled: !canEdit,
            required: true,
            onChange: (event) => setTitle(event.target.value),
            placeholder: "문서 제목",
          })
        ),
        React.createElement("label", null,
          React.createElement(RequiredLabel, null, "Provider"),
          React.createElement("input", {
            value: provider,
            disabled: !canEdit,
            onChange: (event) => setProvider(event.target.value),
            placeholder: "nts, pps, koreaexim",
          })
        ),
        React.createElement("label", null,
          React.createElement(RequiredLabel, null, "Provider Name"),
          React.createElement("input", {
            value: providerNameKo,
            disabled: !canEdit,
            onChange: (event) => setProviderNameKo(event.target.value),
            placeholder: "국세청",
          })
        ),
        isEdit && React.createElement("label", null,
          React.createElement(RequiredLabel, null, "Status"),
          React.createElement("input", {
            value: status,
            disabled: !canEdit,
            onChange: (event) => setStatus(event.target.value),
            placeholder: "active",
          })
        ),
        React.createElement("label", null,
          React.createElement(RequiredLabel, null, "Secret"),
          React.createElement("input", {
            value: authSecretRefs,
            disabled: !canEdit,
            onChange: (event) => setAuthSecretRefs(event.target.value),
            list: "source-secret-ids",
            placeholder: "data_go_kr.service_key",
          })
        ),
        React.createElement("datalist", { id: "source-secret-ids" },
          secretOptions.map((secret) =>
            React.createElement("option", {
              key: secret.id,
              value: displaySecretId(secret.id),
              label: [secret.provider, secret.name].filter(Boolean).join(" · "),
            })
          )
        ),
        React.createElement("label", null,
          React.createElement(RequiredLabel, null, "Auth Params"),
          React.createElement("input", {
            value: authParameterNames,
            disabled: !canEdit,
            onChange: (event) => setAuthParameterNames(event.target.value),
            placeholder: "serviceKey, authkey",
          })
        ),
        React.createElement("label", null,
          React.createElement(RequiredLabel, null, "Source Key"),
          React.createElement("input", {
            value: sourceId,
            disabled: true,
            placeholder: "source.pps.g2b_contract_info",
          })
        ),
        React.createElement("div", { className: "modal-actions" },
          React.createElement("button", {
            className: "btn",
            onClick: onClose,
            type: "button",
          }, "Cancel"),
          React.createElement("button", {
            className: "btn primary",
            disabled: loading || !canEdit || !title.trim() || (!isEdit && !file),
            type: "submit",
          }, loading ? "Saving" : "Save")
        )
      ),
    )
  );
}

function SectionToolbar({ title, detail, actionLabel, onAction, loading = false, loadingLabel = "Loading" }) {
  return React.createElement("section", { className: "section-toolbar" },
    React.createElement("div", null,
      React.createElement("div", { className: "card-title" }, title),
      detail && React.createElement("div", { className: "subtitle" }, detail)
    ),
    React.createElement("button", {
      className: "btn primary",
      disabled: loading,
      onClick: onAction,
      type: "button",
    },
      loading && React.createElement("span", { className: "btn-spinner", "aria-hidden": "true" }),
      React.createElement("span", null, loading ? loadingLabel : actionLabel)
    )
  );
}

function SecretModal({ secret, loading, onSave, onClose }) {
  const isEdit = Boolean(secret?.id);
  const [id, setId] = useState(displaySecretId(secret?.id) || "");
  const [provider, setProvider] = useState(secret?.provider || "");
  const [name, setName] = useState(secret?.name || "");
  const [description, setDescription] = useState(secret?.description || "");
  const [value, setValue] = useState("");
  const [message, setMessage] = useState("");
  useEffect(() => {
    setId(displaySecretId(secret?.id) || "");
    setProvider(secret?.provider || "");
    setName(secret?.name || "");
    setDescription(secret?.description || "");
    setValue("");
    setMessage("");
  }, [secret?.id]);
  async function submit(event) {
    event.preventDefault();
    if (!id.trim()) {
      setMessage("Key is required");
      return;
    }
    if (!name.trim()) {
      setMessage("Name is required");
      return;
    }
    try {
      const result = await onSave({ id, provider, name, description, value, allow_update: isEdit });
      setMessage(`Saved ${result?.secret?.id || "secret"}`);
    } catch (err) {
      setMessage(err.message || String(err));
    }
  }
  return React.createElement("div", { className: "modal-backdrop", onMouseDown: onClose },
    React.createElement("section", {
      className: "modal",
      role: "dialog",
      "aria-modal": "true",
      onMouseDown: (event) => event.stopPropagation(),
    },
      React.createElement("div", { className: "modal-head" },
        React.createElement("div", null,
          React.createElement("div", { className: "card-title" }, isEdit ? "Edit Secret" : "Register Secret"),
          React.createElement("div", { className: "subtitle" }, "Secret values are write-only; saved values are never returned by the API")
        ),
        React.createElement("button", {
          className: "icon-btn",
          onClick: onClose,
          type: "button",
          "aria-label": "Close",
        }, "x")
      ),
      message && React.createElement("div", { className: "upload-message" }, message),
      React.createElement("form", { className: "modal-form", onSubmit: submit },
        React.createElement("label", null,
          React.createElement(RequiredLabel, { required: true }, "Key"),
          React.createElement("input", {
            value: id,
            disabled: isEdit,
            required: true,
            onChange: (event) => setId(event.target.value),
            placeholder: "data_go_kr.service_key",
          })
        ),
        React.createElement("label", null,
          React.createElement(RequiredLabel, null, "Provider"),
          React.createElement("input", {
            value: provider,
            onChange: (event) => setProvider(event.target.value),
            placeholder: "data_go_kr, nts, koreaexim",
          })
        ),
        React.createElement("label", null,
          React.createElement(RequiredLabel, { required: true }, "Name"),
          React.createElement("input", {
            value: name,
            required: true,
            onChange: (event) => setName(event.target.value),
            placeholder: "service_key",
          })
        ),
        React.createElement("label", null,
          React.createElement(RequiredLabel, null, "Description"),
          React.createElement("input", {
            value: description,
            onChange: (event) => setDescription(event.target.value),
            placeholder: "공공데이터포털 서비스키",
          })
        ),
        React.createElement("label", null,
          React.createElement(RequiredLabel, { required: !isEdit }, isEdit ? "New Secret Value" : "Secret Value"),
          React.createElement("input", {
            value,
            onChange: (event) => setValue(event.target.value),
            type: "password",
            required: !isEdit,
            placeholder: isEdit ? "변경할 때만 입력" : "저장 시에만 입력",
          })
        ),
        React.createElement("div", { className: "modal-actions" },
          React.createElement("button", {
            className: "btn",
            onClick: onClose,
            type: "button",
          }, "Cancel"),
          React.createElement("button", {
            className: "btn primary",
            disabled: loading || !id.trim() || !name.trim(),
            type: "submit",
          }, loading ? "Saving" : "Save")
        )
      )
    )
  );
}

function SourceIngestModal({ source, meta, secrets, loading, onStart, onClose }) {
  const [commitMode, setCommitMode] = useState("proposal");
  const [force, setForce] = useState(false);
  const [llmMode, setLlmMode] = useState("openai");
  const [llmSecretRef, setLlmSecretRef] = useState(defaultOpenAiSecretRef(secrets));
  const [message, setMessage] = useState("");
  const serviceLlmMode = meta?.llm?.mode || "disabled";
  const llmSecrets = llmSecretOptions(secrets);
  const openaiReady = llmMode === "openai" && (Boolean(llmSecretRef) || Boolean(meta?.llm?.openai_api_key_configured));
  const canStart = openaiReady && !loading;
  async function submit(event) {
    event.preventDefault();
    if (!openaiReady) {
      setMessage("Select an OpenAI secret or configure OPENAI_API_KEY.");
      return;
    }
    setMessage("");
    try {
      await onStart(source, { commitMode, force, llmMode, llmSecretRef });
    } catch (err) {
      setMessage(err.message || String(err));
    }
  }
  return React.createElement("div", { className: "modal-backdrop", onMouseDown: onClose },
    React.createElement("section", {
      className: "modal",
      role: "dialog",
      "aria-modal": "true",
      onMouseDown: (event) => event.stopPropagation(),
    },
      React.createElement("div", { className: "modal-head" },
        React.createElement("div", null,
          React.createElement("div", { className: "card-title" }, "Start Ingestion"),
          React.createElement("div", { className: "subtitle" }, source?.title || source?.id || "Source")
        ),
        React.createElement("button", {
          className: "icon-btn",
          onClick: onClose,
          type: "button",
          "aria-label": "Close",
        }, "x")
      ),
      message && React.createElement("div", { className: "upload-message" }, message),
      React.createElement("form", { className: "modal-form", onSubmit: submit },
        React.createElement("label", null,
          React.createElement(RequiredLabel, { required: true }, "Commit Mode"),
          React.createElement("select", {
            value: commitMode,
            onChange: (event) => setCommitMode(event.target.value),
          },
            React.createElement("option", { value: "proposal" }, "Proposal"),
            React.createElement("option", { value: "direct_apply" }, "Direct Apply")
          )
        ),
        React.createElement("label", null,
          React.createElement(RequiredLabel, { required: true }, "LLM Mode"),
          React.createElement("select", {
            value: llmMode,
            onChange: (event) => setLlmMode(event.target.value),
          },
            React.createElement("option", { value: "openai" }, "OpenAI")
          )
        ),
        React.createElement("label", null,
          React.createElement(RequiredLabel, { required: !meta?.llm?.openai_api_key_configured }, "LLM Secret"),
          React.createElement("select", {
            value: llmSecretRef,
            onChange: (event) => setLlmSecretRef(event.target.value),
          },
            React.createElement("option", { value: "" }, meta?.llm?.openai_api_key_configured ? "Use service OPENAI_API_KEY" : "Select a secret"),
            llmSecrets.map((secret) =>
              React.createElement("option", { key: secret.id, value: secret.id },
                `${displaySecretId(secret.id)}${secret.value_preview ? ` (${secret.value_preview})` : ""}`
              )
            )
          )
        ),
        commitMode === "direct_apply" && React.createElement("div", { className: "warning" },
          "Direct Apply writes approved catalog data immediately."
        ),
        React.createElement("div", { className: openaiReady ? "notice" : "warning" },
          openaiReady
            ? "OpenAI generates the semantic proposal. The selected secret is resolved only during this ingestion run."
            : `OpenAI is not ready. Current service LLM mode: ${serviceLlmMode}.`
        ),
        React.createElement("label", { className: "check-line" },
          React.createElement("input", {
            type: "checkbox",
            checked: force,
            onChange: (event) => setForce(event.target.checked),
          }),
          React.createElement("span", null, "Force re-run even if this revision was already ingested")
        ),
        React.createElement("label", null,
          React.createElement(RequiredLabel, null, "Source Key"),
          React.createElement("input", {
            value: source?.id || "",
            disabled: true,
          })
        ),
        React.createElement("label", null,
          React.createElement(RequiredLabel, null, "Revision"),
          React.createElement("input", {
            value: source?.revision_id || "",
            disabled: true,
          })
        ),
        React.createElement("div", { className: "modal-actions" },
          React.createElement("button", {
            className: "btn",
            onClick: onClose,
            type: "button",
          }, "Cancel"),
          React.createElement("button", {
            className: "btn primary",
            disabled: !canStart,
            type: "submit",
          }, loading ? "Starting" : "Start")
        )
      )
    )
  );
}

function SourceDeleteModal({ state, loading, onChange, onDelete, onClose }) {
  const plan = state?.plan || {};
  const source = state?.source || plan.source || {};
  const mode = state?.mode || "archive";
  const confirmText = state?.confirmText || "";
  const needsConfirm = mode !== "archive";
  const canDelete = !needsConfirm || confirmText === source.id;
  const counts = plan.counts || {};
  const modes = Array.isArray(plan.modes) ? plan.modes : [];
  return React.createElement("div", { className: "modal-backdrop", onMouseDown: onClose },
    React.createElement("section", {
      className: "modal",
      role: "dialog",
      "aria-modal": "true",
      onMouseDown: (event) => event.stopPropagation(),
    },
      React.createElement("div", { className: "modal-head" },
        React.createElement("div", null,
          React.createElement("div", { className: "card-title" }, "Delete Source"),
          React.createElement("div", { className: "subtitle" }, source.id || "No source")
        ),
        React.createElement("button", {
          className: "icon-btn",
          onClick: onClose,
          type: "button",
          "aria-label": "Close",
        }, "x")
      ),
      React.createElement(DefinitionList, { title: "Impact Preview", data: compactObject(counts) }),
      React.createElement("div", { className: "delete-options" },
        modes.map((item) =>
          React.createElement("label", { className: "delete-option", key: item.mode },
            React.createElement("input", {
              type: "radio",
              checked: mode === item.mode,
              onChange: () => onChange({ ...state, mode: item.mode, confirmText: "" }),
            }),
            React.createElement("span", null,
              React.createElement("strong", null, item.label || item.mode),
              item.destructive && React.createElement("em", null, " destructive")
            )
          )
        )
      ),
      needsConfirm && React.createElement("label", { className: "confirm-field" },
        React.createElement("span", null, `Type ${source.id} to confirm`),
        React.createElement("input", {
          value: confirmText,
          onChange: (event) => onChange({ ...state, confirmText: event.target.value }),
          placeholder: source.id,
        })
      ),
      React.createElement("div", { className: "modal-actions" },
        React.createElement("button", {
          className: "btn",
          onClick: onClose,
          type: "button",
        }, "Cancel"),
        React.createElement("button", {
          className: "btn danger",
          disabled: loading || !canDelete,
          onClick: () => onDelete(source.id, mode),
          type: "button",
        }, loading ? "Deleting" : "Delete")
      )
    )
  );
}

function CatalogEditModal({ refData, loading, onSave, onClose }) {
  const [text, setText] = useState(JSON.stringify(cleanCatalogPayload(refData?.item || {}), null, 2));
  const [message, setMessage] = useState("");
  async function submit(event) {
    event.preventDefault();
    let payload;
    try {
      payload = JSON.parse(text);
    } catch (err) {
      setMessage(`Invalid JSON: ${err.message || String(err)}`);
      return;
    }
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
      setMessage("Catalog item must be a JSON object");
      return;
    }
    setMessage("");
    try {
      await onSave(refData, cleanCatalogPayload(payload));
    } catch (err) {
      setMessage(err.message || String(err));
    }
  }
  return React.createElement("div", { className: "modal-backdrop", onMouseDown: onClose },
    React.createElement("section", {
      className: "modal",
      role: "dialog",
      "aria-modal": "true",
      onMouseDown: (event) => event.stopPropagation(),
    },
      React.createElement("div", { className: "modal-head" },
        React.createElement("div", null,
          React.createElement("div", { className: "card-title" }, "Edit Catalog Item"),
          React.createElement("div", { className: "subtitle" }, `${refData?.section || "-"} · ${refData?.id || "-"}`)
        ),
        React.createElement("button", {
          className: "icon-btn",
          onClick: onClose,
          type: "button",
          "aria-label": "Close",
        }, "x")
      ),
      message && React.createElement("div", { className: "error" }, message),
      React.createElement("form", { className: "modal-form", onSubmit: submit },
        React.createElement("label", null,
          React.createElement(RequiredLabel, { required: true }, "JSON"),
          React.createElement("textarea", {
            value: text,
            onChange: (event) => setText(event.target.value),
            rows: 18,
          })
        ),
        React.createElement("div", { className: "modal-actions" },
          React.createElement("button", {
            className: "btn",
            onClick: onClose,
            type: "button",
          }, "Cancel"),
          React.createElement("button", {
            className: "btn primary",
            disabled: loading,
            type: "submit",
          }, loading ? "Saving" : "Save")
        )
      )
    )
  );
}

function ProposalItemEditModal({ state, loading, onSave, onClose }) {
  const item = state?.item || {};
  const fields = editableProposalFields(item.item_type);
  const payload = item.payload || {};
  const [values, setValues] = useState(() => Object.fromEntries(
    fields.map((field) => [field.key, field.kind === "json"
      ? JSON.stringify(getPath(payload, field.path) ?? field.emptyValue ?? null, null, 2)
      : String(getPath(payload, field.path) ?? "")
    ])
  ));
  const [message, setMessage] = useState("");
  async function submit(event) {
    event.preventDefault();
    const nextPayload = deepClone(payload);
    for (const field of fields) {
      let value = values[field.key];
      if (field.kind === "json") {
        try {
          value = JSON.parse(value);
        } catch (err) {
          setMessage(`Invalid JSON in ${field.label}: ${err.message || String(err)}`);
          return;
        }
      }
      setPath(nextPayload, field.path, value);
    }
    setMessage("");
    try {
      await onSave(state, { payload: nextPayload });
    } catch (err) {
      setMessage(err.message || String(err));
    }
  }
  return React.createElement("div", { className: "modal-backdrop", onMouseDown: onClose },
    React.createElement("section", {
      className: "modal wide-modal",
      role: "dialog",
      "aria-modal": "true",
      onMouseDown: (event) => event.stopPropagation(),
    },
      React.createElement("div", { className: "modal-head" },
        React.createElement("div", null,
          React.createElement("div", { className: "card-title" }, "Edit Proposal Item"),
          React.createElement("div", { className: "subtitle" }, `${item.item_type || "-"} · ${item.target_id || item.id || "-"}`)
        ),
        React.createElement("button", {
          className: "icon-btn",
          onClick: onClose,
          type: "button",
          "aria-label": "Close",
        }, "x")
      ),
      message && React.createElement("div", { className: "error" }, message),
      React.createElement("form", { className: "modal-form", onSubmit: submit },
        React.createElement(DefinitionList, { title: "Read-only Review Metadata", data: compactObject({
          item_type: item.item_type,
          target_id: item.target_id,
          action: item.action,
          status: item.status,
          evidence: inlineJson(item.evidence),
        }) }),
        fields.length === 0
          ? React.createElement("div", { className: "warning" }, "This proposal item is read-only. Shared catalog objects must be changed through a separate governance proposal.")
          : fields.map((field) =>
              React.createElement("label", { key: field.key },
                React.createElement(RequiredLabel, { required: false }, field.label),
                field.kind === "json"
                  ? React.createElement("textarea", {
                      value: values[field.key],
                      onChange: (event) => setValues((prev) => ({ ...prev, [field.key]: event.target.value })),
                      rows: field.rows || 6,
                    })
                  : React.createElement("textarea", {
                      value: values[field.key],
                      onChange: (event) => setValues((prev) => ({ ...prev, [field.key]: event.target.value })),
                      rows: field.rows || 3,
                    })
              )
            ),
        React.createElement("div", { className: "modal-actions" },
          React.createElement("button", {
            className: "btn",
            onClick: onClose,
            type: "button",
          }, "Cancel"),
          React.createElement("button", {
            className: "btn primary",
            disabled: loading || fields.length === 0,
            type: "submit",
          }, loading ? "Saving" : "Save")
        )
      )
    )
  );
}

function editableProposalFields(itemType) {
  const fields = {
    capability: [
      { key: "description_ko", label: "Description", path: ["description_ko"], rows: 4 },
      { key: "use_when", label: "Use When", path: ["use_when"], rows: 4 },
      { key: "examples", label: "Examples JSON", path: ["examples"], kind: "json", emptyValue: [], rows: 5 },
      { key: "aliases", label: "Aliases JSON", path: ["provenance", "aliases"], kind: "json", emptyValue: [], rows: 5 },
      { key: "intent_patterns", label: "Intent Patterns JSON", path: ["provenance", "intent_patterns"], kind: "json", emptyValue: [], rows: 5 },
    ],
    semantic_type: [
      { key: "description_ko", label: "Description", path: ["description_ko"], rows: 4 },
      { key: "aliases", label: "Aliases JSON", path: ["aliases"], kind: "json", emptyValue: [], rows: 5 },
    ],
    entity: [
      { key: "name_ko", label: "Name", path: ["name_ko"], rows: 2 },
      { key: "description_ko", label: "Description", path: ["description_ko"], rows: 4 },
    ],
    planning_example: [
      { key: "question", label: "Question", path: ["question"], rows: 3 },
      { key: "tags", label: "Tags JSON", path: ["tags"], kind: "json", emptyValue: [], rows: 4 },
    ],
  };
  return fields[itemType] || [];
}

function getPath(value, path) {
  return path.reduce((current, key) => current && typeof current === "object" ? current[key] : undefined, value);
}

function setPath(target, path, value) {
  let current = target;
  path.slice(0, -1).forEach((key) => {
    if (!current[key] || typeof current[key] !== "object" || Array.isArray(current[key])) {
      current[key] = {};
    }
    current = current[key];
  });
  current[path[path.length - 1]] = value;
}

function deepClone(value) {
  return JSON.parse(JSON.stringify(value || {}));
}

function CatalogDeleteModal({ state, loading, onChange, onDelete, onClose }) {
  const plan = state?.plan || {};
  const refData = state?.ref || {};
  const modes = Array.isArray(plan.modes) ? plan.modes : [];
  const mode = state?.mode || plan.default_mode || modes[0]?.mode || "deprecate";
  const confirmText = state?.confirmText || "";
  const selectedMode = modes.find((item) => item.mode === mode) || {};
  const needsConfirm = Boolean(selectedMode.destructive);
  const canDelete = modes.some((item) => item.mode === mode) && (!needsConfirm || confirmText === refData.id);
  const blockers = Array.isArray(plan.blockers) ? plan.blockers : [];
  return React.createElement("div", { className: "modal-backdrop", onMouseDown: onClose },
    React.createElement("section", {
      className: "modal",
      role: "dialog",
      "aria-modal": "true",
      onMouseDown: (event) => event.stopPropagation(),
    },
      React.createElement("div", { className: "modal-head" },
        React.createElement("div", null,
          React.createElement("div", { className: "card-title" }, "Delete Catalog Item"),
          React.createElement("div", { className: "subtitle" }, `${refData.section || "-"} · ${refData.id || "-"}`)
        ),
        React.createElement("button", {
          className: "icon-btn",
          onClick: onClose,
          type: "button",
          "aria-label": "Close",
        }, "x")
      ),
      blockers.length > 0 && React.createElement("div", { className: "warning" },
        `Dependent objects exist: ${blockers.map((item) => `${item.section} ${item.count}`).join(", ")}`
      ),
      React.createElement("div", { className: "delete-options" },
        modes.map((item) =>
          React.createElement("label", { className: "delete-option", key: item.mode },
            React.createElement("input", {
              type: "radio",
              checked: mode === item.mode,
              onChange: () => onChange({ ...state, mode: item.mode, confirmText: "" }),
            }),
            React.createElement("span", null,
              React.createElement("strong", null, item.label || item.mode),
              item.destructive && React.createElement("em", null, " destructive")
            )
          )
        )
      ),
      needsConfirm && React.createElement("label", { className: "confirm-field" },
        React.createElement("span", null, `Type ${refData.id} to confirm`),
        React.createElement("input", {
          value: confirmText,
          onChange: (event) => onChange({ ...state, confirmText: event.target.value }),
          placeholder: refData.id,
        })
      ),
      React.createElement("div", { className: "modal-actions" },
        React.createElement("button", {
          className: "btn",
          onClick: onClose,
          type: "button",
        }, "Cancel"),
        React.createElement("button", {
          className: "btn danger",
          disabled: loading || !canDelete,
          onClick: () => onDelete(state),
          type: "button",
        }, loading ? "Working" : (mode === "deprecate" ? "Deprecate" : "Delete"))
      )
    )
  );
}

function cleanCatalogPayload(value) {
  const copy = { ...(value || {}) };
  delete copy.__catalog_section;
  return copy;
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
                React.createElement("th", null, activeSection === "ingestion_runs" ? "Progress" : "Summary")
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
                  React.createElement("td", { className: "wrap muted" },
                    activeSection === "ingestion_runs"
                      ? React.createElement(IngestionProgress, { run: row.value, compact: true })
                      : (row.summary || "-")
                  )
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

function CatalogVersionBanner({ catalogView, onBack }) {
  const version = catalogView?.version || {};
  return React.createElement("div", { className: "version-banner" },
    React.createElement("div", { className: "version-banner-main" },
      React.createElement("strong", null, `Viewing ${version.id || "catalog snapshot"}`),
      React.createElement("span", null, [
        version.version_number ? `v${version.version_number}` : null,
        version.reason,
        "Read-only snapshot",
      ].filter(Boolean).join(" · "))
    ),
    React.createElement("button", {
      className: "btn",
      onClick: onBack,
      type: "button",
    }, "Back to Current")
  );
}

function DetailCard({
  activeSection,
  selected,
  catalog,
  endpointChecks,
  isVersionMode = false,
  catalogView = null,
  onViewCatalogVersion,
  onDownloadCatalogVersion,
  onRestoreCatalogVersion,
  onApply,
  onReject,
  onEditSource,
  onDeleteSource,
  onIngestSource,
  onEditSecret,
  onDeleteSecret,
  editableCatalogRef,
  onEditCatalogItem,
  onDeleteCatalogItem,
  onEditProposalItem,
  loading,
}) {
  const isPendingProposal = activeSection === "proposals" && selected?.value?.status === "pending_review";
  const isEditableSource = activeSection === "sources" && selected?.value?.source_kind === "registry";
  const isSecret = activeSection === "secrets" && selected?.value?.id;
  const catalogRef = editableCatalogRef ? editableCatalogRef(selected) : null;
  const isCatalogVersion = activeSection === "catalog_versions" && selected?.value?.id;
  const proposalBlockers = isPendingProposal ? proposalApplyBlockers(selected?.value) : [];
  const [activeDetailTab, setActiveDetailTab] = useState("overview");
  useEffect(() => {
    setActiveDetailTab("overview");
  }, [selected?.key]);
  return React.createElement("section", { className: "card" },
    React.createElement("div", { className: "card-head" },
      React.createElement("div", null,
        React.createElement("div", { className: "card-title" }, "Detail"),
        React.createElement("div", { className: "subtitle" }, selected ? (isVersionMode ? `${activeSection} · read-only` : activeSection) : "No selection")
      ),
      isCatalogVersion && React.createElement("div", { className: "actions" },
        React.createElement("button", {
          className: "btn",
          disabled: loading,
          onClick: () => onDownloadCatalogVersion && onDownloadCatalogVersion(selected.value.id),
        }, "Download JSON"),
        React.createElement("button", {
          className: "btn danger",
          disabled: loading,
          onClick: () => onRestoreCatalogVersion && onRestoreCatalogVersion(selected.value.id),
        }, loading ? "Working" : "Restore"),
        React.createElement("button", {
          className: "btn primary",
          disabled: loading,
          onClick: () => onViewCatalogVersion && onViewCatalogVersion(selected.value.id),
        }, loading ? "Loading" : "View Snapshot")
      ),
      !isVersionMode && isPendingProposal && React.createElement("div", { className: "actions" },
        React.createElement("button", {
          className: "btn danger",
          disabled: loading,
          onClick: () => onReject(selected.value.id),
        }, loading ? "Working" : "Reject"),
        React.createElement("button", {
          className: "btn primary",
          disabled: loading || proposalBlockers.length > 0,
          title: proposalBlockers.length > 0 ? "Apply blocked until all operation variants pass verification." : undefined,
          onClick: () => onApply(selected.value.id),
        }, loading ? "Applying" : proposalBlockers.length > 0 ? "Apply Blocked" : "Apply")
      ),
      !isVersionMode && isEditableSource && React.createElement("div", { className: "actions" },
        React.createElement("button", {
          className: "btn primary",
          disabled: loading || !selected.value.revision_id,
          onClick: () => onIngestSource(selected.value),
        }, loading ? "Starting" : "Ingest"),
        React.createElement("button", {
          className: "btn",
          disabled: loading,
          onClick: () => onEditSource(selected.value),
        }, "Edit"),
        React.createElement("button", {
          className: "btn danger",
          disabled: loading,
          onClick: () => onDeleteSource(selected.value),
        }, loading ? "Working" : "Delete")
      ),
      !isVersionMode && isSecret && React.createElement("div", { className: "actions" },
        React.createElement("button", {
          className: "btn",
          disabled: loading,
          onClick: () => onEditSecret(selected.value),
        }, "Edit"),
        React.createElement("button", {
          className: "btn danger",
          disabled: loading,
          onClick: () => onDeleteSecret(selected.value.id),
        }, loading ? "Working" : "Delete")
      ),
      !isVersionMode && catalogRef && React.createElement("div", { className: "actions" },
        React.createElement("button", {
          className: "btn",
          disabled: loading,
          onClick: () => onEditCatalogItem(catalogRef),
        }, "Edit"),
        React.createElement("button", {
          className: "btn danger",
          disabled: loading,
          onClick: () => onDeleteCatalogItem(catalogRef),
        }, loading ? "Working" : "Delete")
      )
    ),
    React.createElement("div", { className: "card-body" },
      selected
        ? React.createElement(Detail, {
            item: selected,
            catalog,
            endpointChecks,
            onEditProposalItem,
            catalogView,
            activeDetailTab,
            setActiveDetailTab,
          })
        : React.createElement("div", { className: "muted" }, "Select an item")
    )
  );
}

function Detail({ item, catalog, endpointChecks, onEditProposalItem, catalogView, activeDetailTab, setActiveDetailTab }) {
  const data = item.value || {};
  const provenance = data.provenance && typeof data.provenance === "object" ? data.provenance : null;
  const catalogSection = data.__catalog_section || item.section;
  const tabs = detailTabs(item, data, catalogSection, provenance);
  return React.createElement("div", { className: `detail ${activeDetailTab === "raw" ? "raw-detail" : ""}` },
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
    React.createElement("div", { className: `detail-scroll ${activeDetailTab === "raw" ? "raw-scroll" : ""}` },
      activeDetailTab === "overview" && (
        item.section === "proposals"
          ? React.createElement(ProposalOverview, { data })
          : item.section === "catalog_versions"
            ? React.createElement(CatalogVersionOverview, { data })
          : React.createElement(DetailOverview, { item, data })
      ),
      activeDetailTab === "review" && React.createElement(ProposalReview, { data, onEditProposalItem }),
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
  if (item.section === "proposals") {
    tabs.push({ key: "review", label: "Review" });
  }
  if (item.section === "capabilities" || catalogSection === "operation_contracts" || catalogSection === "operation_variants") {
    tabs.push({ key: "execution", label: "Execution" });
  }
  if (provenance || data.review || data.counts) {
    tabs.push({ key: "evidence", label: "Evidence" });
  }
  tabs.push({ key: "raw", label: "Raw" });
  return tabs;
}

function CatalogVersionOverview({ data }) {
  const version = data.__catalog_version_detail?.catalog_version || data;
  const diff = data.__catalog_version_diff?.diff || null;
  const snapshot = version.snapshot && typeof version.snapshot === "object" ? version.snapshot : {};
  const counts = version.counts && typeof version.counts === "object" ? version.counts : {};
  const diffSections = diff?.sections && typeof diff.sections === "object" ? diff.sections : {};
  return React.createElement("div", { className: "detail-stack" },
    React.createElement(DefinitionList, { title: "Version", data: compactObject({
      id: version.id,
      version_number: version.version_number,
      status: version.status,
      reason: version.reason,
      proposal_id: version.proposal_id,
      snapshot_sha256: version.snapshot_sha256,
      created_by: version.created_by,
      created_at: version.created_at,
      snapshot_scope: version.metadata?.snapshot_scope,
    }) }),
    React.createElement(MiniTable, {
      title: "Snapshot Sections",
      columns: ["Section", "Items"],
      rows: Object.keys(snapshot).sort().map((section) => [
        section,
        String(counts[section] ?? sectionItemCount(snapshot[section])),
      ]),
    }),
    diff && React.createElement(MiniTable, {
      title: "Diff From Previous",
      columns: ["Section", "Added", "Removed", "Changed"],
      rows: Object.entries(diffSections).map(([section, item]) => [
        section,
        String(item?.counts?.added || 0),
        String(item?.counts?.removed || 0),
        String(item?.counts?.changed || 0),
      ]),
    }),
    diff && Object.keys(diffSections).length === 0 && React.createElement("div", { className: "muted" }, "No changes from the comparison version")
  );
}

function sectionItemCount(value) {
  if (Array.isArray(value)) return value.length;
  if (value && typeof value === "object") return Object.keys(value).length;
  return 0;
}

function ProposalOverview({ data }) {
  const detail = data.__proposal_detail || {};
  const items = Array.isArray(detail.items) ? detail.items : [];
  const proposal = detail.proposal || data;
  if (!items.length) {
    return React.createElement("div", { className: "detail-stack" },
      React.createElement(DefinitionList, { data: compactObject({
        id: data.id,
        status: data.status,
        kind: data.kind,
        source_document_id: data.source_document_id,
        proposal_builder: data.proposal_builder,
      }) }),
      React.createElement("div", { className: "muted" }, "Loading proposal detail")
    );
  }
  const capabilities = proposalPayloads(items, "capability");
  const entities = proposalPayloads(items, "entity");
  const entityIdentifiers = proposalPayloads(items, "entity_identifier");
  const entityLinks = proposalPayloads(items, "capability_entity_link");
  const contracts = proposalPayloads(items, "operation_contract");
  const fields = proposalPayloads(items, "operation_field");
  const semanticTypes = proposalPayloads(items, "semantic_type");
  const examples = proposalPayloads(items, "planning_example");
  const resources = proposalPayloads(items, "resource");
  const blockers = proposalApplyBlockers(data);
  return React.createElement("div", { className: "detail-stack" },
    blockers.length > 0 && React.createElement(ProposalApplyBlockers, { blockers }),
    React.createElement(DefinitionList, { title: "Proposal", data: compactObject({
      id: proposal.id || data.id,
      status: proposal.status || data.status,
      kind: proposal.kind || data.kind,
      source_document_id: proposal.source_document_id || data.source_document_id,
      created_by: proposal.created_by || data.created_by,
      created_at: proposal.created_at || data.created_at,
    }) }),
    React.createElement(ProposalItemCounts, { items }),
    React.createElement(ProposalCapabilitySummary, { capabilities }),
    React.createElement(ProposalEntitySummary, { entities, entityIdentifiers, entityLinks }),
    React.createElement(ProposalEndpointSummary, { capabilities, contracts, resources }),
    React.createElement(ProposalFieldSummary, { fields }),
    React.createElement(ProposalSemanticTypeSummary, { semanticTypes }),
    examples.length > 0 && React.createElement(ProposalExamplesSummary, { examples })
  );
}

function ProposalReview({ data, onEditProposalItem }) {
  const detail = data.__proposal_detail || {};
  const items = Array.isArray(detail.items) ? detail.items : [];
  const proposal = detail.proposal || data;
  if (!items.length) {
    return React.createElement("div", { className: "detail-stack" },
      React.createElement("div", { className: "muted" }, "Loading proposal detail")
    );
  }
  const editableItems = items.filter((item) => editableProposalFields(item.item_type).length > 0);
  const blockers = proposalApplyBlockers(data);
  return React.createElement("div", { className: "detail-stack" },
    blockers.length > 0 && React.createElement(ProposalApplyBlockers, { blockers }),
    React.createElement(DefinitionList, { title: "Editable Review", data: compactObject({
      proposal_id: proposal.id || data.id,
      editable_items: editableItems.length,
      total_items: items.length,
      read_only_items: Math.max(0, items.length - editableItems.length),
    }) }),
    React.createElement(ProposalItemsSummary, {
      proposalId: proposal.id || data.id,
      items: editableItems,
      onEditProposalItem,
    }),
    editableItems.length === 0 && React.createElement("div", { className: "muted" }, "No editable proposal items")
  );
}

function proposalPayloads(items, itemType) {
  return items
    .filter((item) => item.item_type === itemType)
    .map((item) => item.payload || {})
    .filter((payload) => payload && typeof payload === "object");
}

function proposalApplyBlockers(data) {
  const detail = data?.__proposal_detail || {};
  const items = Array.isArray(detail.items) ? detail.items : [];
  const proposal = detail.proposal || data || {};
  if (!items.length) return [];
  const contractsByOperation = {};
  items.forEach((item) => {
    if (item.item_type !== "operation_contract") return;
    const payload = item.payload || {};
    if (payload.operation_id) contractsByOperation[payload.operation_id] = payload;
  });
  return items
    .filter((item) => item.item_type === "operation_variant")
    .map((item) => {
      const payload = item.payload || {};
      const verification = item.evidence?.verification || payload.verification || {};
      const status = verification.status || verification.verification_status || verification.result_status;
      if (!["failed", "error", "inconclusive", "skipped"].includes(String(status || "").toLowerCase())) return null;
      const contract = contractsByOperation[payload.operation_id] || {};
      return proposalVerificationBlocker(item, payload, verification, contract, proposal);
    })
    .filter(Boolean);
}

function proposalVerificationBlocker(item, variant, verification, contract, proposal) {
  const requestArgs = verification?.request?.arguments && typeof verification.request.arguments === "object"
    ? verification.request.arguments
    : {};
  const contractFields = contractRequestFields(contract);
  const missingFields = contractFields
    .filter((field) => field.semanticType !== "api_service_key")
    .filter((field) => field.defaultValue === undefined)
    .filter((field) => !Object.prototype.hasOwnProperty.call(requestArgs, field.name))
    .map((field) => field.name);
  const expectedSemanticTypes = uniqueBy(
    contractFields
      .map((field) => field.semanticType)
      .filter((value) => value && value !== "api_service_key" && value !== "page_number" && value !== "page_size" && value !== "result_format"),
    (value) => value
  );
  const providedSemanticTypes = new Set([
    ...Object.keys(variant.fixed_semantic_arguments || {}),
    ...Object.keys(verification.sample_semantic_arguments || {}),
    ...Object.keys(variant.verification?.sample_semantic_arguments || {}),
  ]);
  const missingSemanticTypes = expectedSemanticTypes.filter((type) => !providedSemanticTypes.has(type));
  const unexpectedSemanticTypes = Array.from(providedSemanticTypes).filter((type) => !expectedSemanticTypes.includes(type));
  return {
    item_id: item.id,
    proposal_id: proposal.id || item.proposal_id,
    source_document_id: proposal.source_document_id || item.source_document_id,
    capability_id: variant.capability_id || verification.capability_id,
    operation_id: variant.operation_id || verification.operation_id,
    variant_id: variant.variant_id || verification.variant_id,
    status: verification.status || "failed",
    message: verification.message || verification.error_message || "variant verification failed",
    provider_status: verification.provider_status,
    response_sample: verification.response_sample,
    normalized_sample: verification.normalized_sample,
    request: verification.request || {},
    request_arguments: requestArgs,
    missing_fields: missingFields,
    missing_semantic_types: missingSemanticTypes,
    unexpected_semantic_types: unexpectedSemanticTypes,
    sample_semantic_arguments: verification.sample_semantic_arguments || variant.verification?.sample_semantic_arguments || {},
  };
}

function contractRequestFields(contract) {
  const request = contract?.request || {};
  const fields = [];
  Object.entries(request).forEach(([location, values]) => {
    if (!values || typeof values !== "object") return;
    Object.entries(values).forEach(([name, spec]) => {
      fields.push({
        location,
        name,
        semanticType: spec?.semantic_type || spec?.kind,
        defaultValue: spec?.default,
      });
    });
  });
  return fields;
}

function ProposalApplyBlockers({ blockers }) {
  if (!blockers.length) return null;
  return React.createElement("div", { className: "apply-blockers" },
    React.createElement("div", { className: "apply-blockers-head" },
      React.createElement("strong", null, "Apply Blocked"),
      React.createElement("span", null, `${blockers.length} operation variant verification ${blockers.length === 1 ? "issue" : "issues"}`)
    ),
    blockers.map((blocker) =>
      React.createElement("div", { className: "apply-blocker", key: blocker.item_id || blocker.variant_id },
        React.createElement(DefinitionList, { title: blocker.variant_id || "Variant verification", data: compactObject({
          capability: blocker.capability_id,
          operation: blocker.operation_id,
          status: blocker.status,
          provider_status: blocker.provider_status,
          provider_error: providerErrorSummary(blocker),
          message: blocker.message,
        }) }),
        blocker.missing_fields.length > 0 && React.createElement(ChipsSection, { title: "Missing Request Fields", values: blocker.missing_fields }),
        (blocker.missing_semantic_types.length > 0 || blocker.unexpected_semantic_types.length > 0) && React.createElement(DefinitionList, {
          title: "Likely Mapping Mismatch",
          data: compactObject({
            contract_expects: blocker.missing_semantic_types.join(", "),
            variant_provides_extra: blocker.unexpected_semantic_types.join(", "),
            sample_semantic_arguments: inlineJson(blocker.sample_semantic_arguments),
          }),
        }),
        React.createElement(DefinitionList, { title: "Request Preview", data: compactObject({
          method: blocker.request.method,
          url: blocker.request.url,
          arguments: inlineJson(blocker.request_arguments),
        }) }),
        React.createElement("div", { className: "actions" },
          React.createElement("button", {
            className: "btn",
            type: "button",
            onClick: () => copyFixPrompt(blocker),
          }, "Copy Fix Prompt")
        ),
        React.createElement("div", { className: "muted" }, "Execution proposal items are read-only. Regenerate this proposal from a corrected codex_manual payload, then apply the new passed proposal.")
      )
    )
  );
}

function copyFixPrompt(blocker) {
  const prompt = [
    "현재 작업 디렉터리는 /workspace 저장소라고 가정해줘. 먼저 AGENTS.md를 읽고 그 규칙을 따라.",
    "",
    "목표:",
    "아래 failed proposal을 직접 수정하지 말고, 같은 source를 기준으로 codex_manual payload를 고쳐 재-ingestion해서 새 pending_review proposal을 생성해줘.",
    "",
    "중요 규칙:",
    "- 질문하지 마.",
    "- 코드는 변경하지 마.",
    "- tracked file은 수정하지 마.",
    "- tmp/semantic_ingestion/ 아래 비밀이 아닌 임시 파일만 사용해.",
    "- secret 값은 출력하거나 tmp 파일에 저장하지 마.",
    "- proposal 생성까지만 진행하고 apply/restore/delete는 하지 마.",
    "- ingestion은 semantic platform API/graph 경계로 실행해서 Ingestion Runs에 남겨.",
    "- shell redirection, pipe, heredoc, command substitution은 쓰지 마.",
    "",
    "수정해야 할 실패 원인:",
    `- source_document_id: ${blocker.source_document_id || "-"}`,
    `- failed_proposal_id: ${blocker.proposal_id || "-"}`,
    `- capability_id: ${blocker.capability_id || "-"}`,
    `- operation_id: ${blocker.operation_id || "-"}`,
    `- variant_id: ${blocker.variant_id || "-"}`,
    `- provider_error: ${providerErrorSummary(blocker) || blocker.message || "-"}`,
    `- missing_request_fields: ${blocker.missing_fields.join(", ") || "-"}`,
    `- contract_expects_semantic_types: ${blocker.missing_semantic_types.join(", ") || "-"}`,
    `- variant_provided_extra_semantic_types: ${blocker.unexpected_semantic_types.join(", ") || "-"}`,
    `- sample_semantic_arguments: ${inlineJson(blocker.sample_semantic_arguments) || "-"}`,
    "",
    "해야 할 일:",
    "1. source/revision/secret 상태를 직접 확인해.",
    "2. 기존 failed proposal의 execution item을 대시보드/DB에서 손수 고치지 마.",
    "3. manual LLM response payload에서 semantic argument/mapping 불일치를 바로잡아.",
    "4. source별 ingestion run을 새로 만들고 새 proposal을 pending_review로 남겨.",
    "5. endpoint check 결과와 새 proposal id를 보고해.",
  ].join("\n");
  if (navigator.clipboard?.writeText) {
    navigator.clipboard.writeText(prompt).catch(() => window.prompt("Copy fix prompt", prompt));
  } else {
    window.prompt("Copy fix prompt", prompt);
  }
}

function providerErrorSummary(blocker) {
  const normalized = blocker.normalized_sample;
  const header = normalized?.response?.header || normalized?.["nkoneps.com.response.ResponseError"]?.header;
  if (header?.resultCode || header?.resultMsg) {
    return [header.resultCode, header.resultMsg].filter(Boolean).join(" / ");
  }
  if (blocker.response_sample && typeof blocker.response_sample === "string") {
    const code = blocker.response_sample.match(/"resultCode"\s*:\s*"([^"]+)"/)?.[1];
    const message = blocker.response_sample.match(/"resultMsg"\s*:\s*"([^"]+)"/)?.[1];
    return [code, message].filter(Boolean).join(" / ");
  }
  return undefined;
}

function ProposalItemsSummary({ proposalId, items, onEditProposalItem }) {
  return React.createElement("div", { className: "detail-section" },
    React.createElement("h3", null, "Editable Proposal Items"),
    React.createElement("div", { className: "mini-table-wrap" },
      React.createElement("table", { className: "mini-table" },
        React.createElement("thead", null,
          React.createElement("tr", null,
            React.createElement("th", null, "Type"),
            React.createElement("th", null, "Target"),
            React.createElement("th", null, "Action"),
            React.createElement("th", null, "Status"),
            React.createElement("th", null, "")
          )
        ),
        React.createElement("tbody", null,
          items.map((item) => {
            const editable = editableProposalFields(item.item_type).length > 0;
            return React.createElement("tr", { key: item.id },
              React.createElement("td", { className: "wrap" }, item.item_type || "-"),
              React.createElement("td", { className: "wrap" }, item.target_id || "-"),
              React.createElement("td", null, item.action || "-"),
              React.createElement("td", null, item.status || "-"),
              React.createElement("td", null,
                editable
                  ? React.createElement("button", {
                      className: "btn small",
                      type: "button",
                      onClick: () => onEditProposalItem && onEditProposalItem({ proposalId, item }),
                    }, "Edit")
                  : React.createElement("span", { className: "muted" }, "Read-only")
              )
            );
          })
        )
      )
    )
  );
}

function ProposalItemCounts({ items }) {
  const counts = items.reduce((acc, item) => {
    const type = item.item_type || "unknown";
    acc[type] = (acc[type] || 0) + 1;
    return acc;
  }, {});
  return React.createElement(DefinitionList, { title: "Item Counts", data: counts });
}

function ProposalCapabilitySummary({ capabilities }) {
  if (!capabilities.length) return null;
  return React.createElement("div", { className: "detail-section" },
    React.createElement("h3", null, "Capabilities"),
    capabilities.map((capability) =>
      React.createElement("div", { className: "review-block", key: capability.id || capability.description_ko },
        React.createElement(DefinitionList, { title: capability.id || "Capability", data: compactObject({
          description_ko: capability.description_ko,
          use_when: capability.use_when,
          status: capability.status,
        }) }),
        Array.isArray(capability.inputs) && capability.inputs.length > 0 &&
          React.createElement(ChipsSection, { title: "Inputs", values: capability.inputs }),
        Array.isArray(capability.outputs) && capability.outputs.length > 0 &&
          React.createElement(ChipsSection, { title: "Outputs", values: capability.outputs }),
        capability.provenance && React.createElement(CapabilityTrace, { provenance: capability.provenance })
      )
    )
  );
}

function ProposalEntitySummary({ entities, entityIdentifiers, entityLinks }) {
  if (!entities.length && !entityIdentifiers.length && !entityLinks.length) return null;
  return React.createElement("div", { className: "detail-stack" },
    entities.length > 0 && React.createElement(MiniTable, {
      title: "Entities",
      columns: ["ID", "Name", "Type", "Summary"],
      rows: entities.map((entity) => [
        entity.id || "-",
        entity.name_ko || "-",
        entity.type || entity.entity_type || "-",
        entity.summary || entity.description_ko || "-",
      ]),
    }),
    entityIdentifiers.length > 0 && React.createElement(MiniTable, {
      title: "Entity Identifiers",
      columns: ["Entity", "Semantic Type", "Required", "Description"],
      rows: entityIdentifiers.map((identifier) => [
        identifier.entity_id || "-",
        identifier.semantic_type_id || "-",
        identifier.required === true ? "required" : "optional",
        identifier.description_ko || identifier.summary || "-",
      ]),
    }),
    entityLinks.length > 0 && React.createElement(MiniTable, {
      title: "Capability Entity Links",
      columns: ["Capability", "Entity", "Role", "Semantic Type"],
      rows: entityLinks.map((link) => [
        link.capability_id || "-",
        link.entity_id || "-",
        link.role || "-",
        link.semantic_type_id || "-",
      ]),
    })
  );
}

function ProposalEndpointSummary({ capabilities, contracts, resources }) {
  const endpoints = capabilities.flatMap((capability) => {
    const provenance = capability.provenance || {};
    return Array.isArray(provenance.endpoints) ? provenance.endpoints : [];
  });
  const resourceById = Object.fromEntries(resources.map((resource) => [resource.id, resource]));
  return React.createElement("div", { className: "detail-stack" },
    endpoints.length > 0 && React.createElement(EndpointTable, { endpoints }),
    contracts.map((contract) =>
      React.createElement("div", { className: "review-block", key: contract.operation_id },
        React.createElement(DefinitionList, { title: `Contract ${contract.operation_id || ""}`, data: compactObject({
          method: contract.method,
          base_url: contract.base_url || resourceById[contract.resource_id]?.base_url,
          path: contract.path,
          resource_id: contract.resource_id,
          capability_id: contract.capability_id,
          auth: inlineJson(contract.auth),
          items_path: itemsPathSummary(contract.response?.items_path),
          count_path: contract.response?.count_path,
          success: inlineJson(contract.response?.success),
        }) })
      )
    )
  );
}

function ProposalFieldSummary({ fields }) {
  if (!fields.length) return null;
  const requestFields = fields.filter((field) => field.direction === "request");
  const responseFields = fields.filter((field) => field.direction === "response");
  return React.createElement("div", { className: "detail-stack" },
    React.createElement(MiniTable, {
      title: "Request Fields",
      columns: ["Operation", "Location", "Raw", "Required", "Label", "Description", "Example", "Type"],
      rows: requestFields.map((field) => [
        field.operation_id || "-",
        field.location || "-",
        field.raw_name || "-",
        field.required === true ? "required" : "optional",
        field.label_ko || "-",
        field.description_ko || "-",
        field.example || "-",
        field.type_hint || "-",
      ]),
    }),
    React.createElement(MiniTable, {
      title: "Response Fields",
      columns: ["Operation", "Raw", "Semantic Type", "Label", "Description", "Example", "Type"],
      rows: responseFields.map((field) => [
        field.operation_id || "-",
        field.raw_name || "-",
        field.evidence?.semantic_type || field.semantic_type_id || "-",
        field.label_ko || "-",
        field.description_ko || "-",
        field.example || "-",
        field.type_hint || "-",
      ]),
    })
  );
}

function ProposalSemanticTypeSummary({ semanticTypes }) {
  if (!semanticTypes.length) return null;
  return React.createElement(MiniTable, {
    title: "Semantic Types",
    columns: ["ID", "Name", "Type", "Summary"],
    rows: semanticTypes.map((type) => [
      type.id || "-",
      type.name_ko || "-",
      type.type || "-",
      type.summary || type.description_ko || "-",
    ]),
  });
}

function ProposalExamplesSummary({ examples }) {
  return React.createElement(MiniTable, {
    title: "Planning Examples",
    columns: ["Question", "Expected Capabilities"],
    rows: examples.map((example) => [
      example.question || "-",
      Array.isArray(example.expected_capability_ids) ? example.expected_capability_ids.join(", ") : "-",
    ]),
  });
}

function DetailOverview({ item, data }) {
  return React.createElement("div", { className: "detail-stack" },
    item.section === "ingestion_runs" && React.createElement("div", { className: "detail-section" },
      React.createElement("h3", null, "Progress"),
      React.createElement(IngestionProgress, { run: data })
    ),
    React.createElement(DefinitionList, { data: compactObject({
      catalog_section: data.__catalog_section,
      entity: data.entity,
      type: data.type,
      from: data.from,
      to: data.to,
      primary_entity: data.primary_entity,
      field: data.field,
      status: data.status,
      catalog_status: data.catalog_status,
      source_kind: data.source_kind,
      action: data.action,
      direction: data.direction,
      provider: data.provider,
      provider_name_ko: data.provider_name_ko,
      title: data.title,
      file_name: data.file_name,
      revision_id: data.revision_id,
      revision_number: data.revision_number,
      sha256: data.sha256,
      object_uri: data.object_uri,
      tool_name: data.tool_name,
      path: data.path,
      current_sha256: data.current_sha256,
      proposal_builder: data.proposal_builder,
      source_path: data.source_path,
      display_id: data.display_id,
      has_value: data.has_value,
      value_preview: data.value_preview,
      value_sha256: data.value_sha256,
      value_length: data.value_length,
      current_step: data.current_step,
      error_message: data.error_message,
      version_number: data.version_number,
      reason: data.reason,
      proposal_id: data.proposal_id,
      snapshot_sha256: data.snapshot_sha256,
      created_by: data.created_by,
      created_at: data.created_at,
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

function sectionRows(catalog, sources, ingestionRuns, secrets, proposals, catalogVersions, section, activeGraphSection = "entities", activeExecutionSection = "operations", endpointChecks = null) {
  if (section === "sources") {
    return sourceRows(sources);
  }
  if (section === "ingestion_runs") {
    return ingestionRunRows(ingestionRuns);
  }
  if (section === "secrets") {
    return secretRows(secrets);
  }
  if (section === "proposals") {
    return proposalRows(proposals);
  }
  if (section === "catalog_versions") {
    return catalogVersionRows(catalogVersions);
  }
  if (section === "semantic_graph") {
    return graphRows(catalog, activeGraphSection);
  }
  if (section === "execution") {
    return executionRows(catalog, activeExecutionSection, endpointChecks);
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

function executionRows(catalog, activeExecutionSection, endpointChecks = null) {
  if (activeExecutionSection === "endpoint_checks") {
    const checks = Array.isArray(endpointChecks?.endpoint_checks) ? endpointChecks.endpoint_checks : [];
    return checks.map((check) => ({
      key: `endpoint_checks:${check.id || `${check.operation_id}:${check.variant_id}:${check.checked_at}`}`,
      section: "execution",
      executionSection: "endpoint_checks",
      value: { ...check, __catalog_section: "endpoint_checks" },
      type: "Endpoint Checks",
      entity: check.operation_id,
      kind: "Endpoint Checks",
      summary: summaryFor("endpoint_checks", check) || check.id || check.operation_id,
    }));
  }
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
  const documents = Array.isArray(sources?.sources) ? sources.sources : [];
  return documents.map((document) => ({
    key: document.id || document.document_id || document.path,
    section: "sources",
    value: document,
    type: document.catalog_status || document.status,
    entity: document.provider,
    kind: "source",
    summary: sourceSummary(document),
  }));
}

function ingestionRunRows(ingestionRuns) {
  const runs = Array.isArray(ingestionRuns?.ingestion_runs) ? ingestionRuns.ingestion_runs : [];
  return runs.map((run) => ({
    key: run.id,
    section: "ingestion_runs",
    value: run,
    type: run.status,
    entity: run.source_id,
    kind: "ingestion_run",
    summary: ingestionRunSummary(run),
  }));
}

function ingestionRunSummary(run) {
  const progress = ingestionRunProgress(run);
  return [
    run.status,
    run.current_step,
    progress?.label,
    run.title || run.source_id,
    run.revision_number ? `rev ${run.revision_number}` : null,
  ].filter(Boolean).join(" · ");
}

function ingestionRunProgress(run) {
  const progress = run?.result && typeof run.result === "object" ? run.result.progress : null;
  const status = String(run?.status || "");
  if (status === "succeeded") {
    return { percent: 100, label: "Completed", detail: "Ingestion completed", state: "success" };
  }
  if (status === "failed") {
    return { percent: 100, label: "Failed", detail: run?.error_message || "Ingestion failed", state: "failed" };
  }
  if (!progress || typeof progress !== "object") {
    return status === "running"
      ? { percent: 8, label: run?.current_step || "Running", detail: "Preparing graph execution", state: "running" }
      : null;
  }
  const batchIndex = Number(progress.batch_index || 0);
  const batchCount = Number(progress.batch_count || 0);
  const hasBatch = Number.isFinite(batchIndex) && Number.isFinite(batchCount) && batchIndex > 0 && batchCount > 0;
  const completed = progress.status === "completed";
  const percent = hasBatch
    ? Math.max(3, Math.min(100, Math.round(((batchIndex - (completed ? 0 : 1)) / batchCount) * 100)))
    : status === "running" ? 12 : 0;
  const operationNames = Array.isArray(progress.operation_names) ? progress.operation_names.filter(Boolean) : [];
  const operationLabel = operationNames.length > 1 ? `${operationNames[0]} +${operationNames.length - 1}` : operationNames[0];
  const label = hasBatch
    ? `Batch ${batchIndex}/${batchCount}${completed ? " done" : ""}`
    : progress.phase || run?.current_step || "Running";
  return {
    percent,
    label,
    detail: [progress.phase, operationLabel].filter(Boolean).join(" · "),
    state: status === "running" ? "running" : status || "queued",
  };
}

function IngestionProgress({ run, compact = false }) {
  const progress = ingestionRunProgress(run);
  if (!progress) return React.createElement("span", { className: "muted" }, "-");
  return React.createElement("div", { className: `run-progress ${compact ? "compact" : ""}` },
    React.createElement("div", { className: "run-progress-head" },
      React.createElement("span", null, progress.label),
      React.createElement("span", { className: "muted" }, `${progress.percent}%`)
    ),
    React.createElement("div", { className: `run-progress-track ${progress.state}` },
      React.createElement("div", { className: "run-progress-fill", style: { width: `${progress.percent}%` } })
    ),
    !compact && progress.detail && React.createElement("div", { className: "run-progress-detail" }, progress.detail)
  );
}

function sourceSummary(document) {
  return [
    document.provider_name_ko || document.provider,
    document.title || document.file_name,
    document.catalog_status,
  ].filter(Boolean).join(" · ");
}

function secretRows(secrets) {
  const rows = Array.isArray(secrets?.secrets) ? secrets.secrets : [];
  return rows.map((secret) => ({
    key: displaySecretId(secret.id),
    section: "secrets",
    value: { ...secret, display_id: displaySecretId(secret.id) },
    type: secret.has_value ? "configured" : "empty",
    entity: secret.provider,
    kind: "secret",
    summary: [secret.provider, secret.name, secret.value_preview || (secret.has_value ? "value configured" : "no value")].filter(Boolean).join(" · "),
  }));
}

function llmSecretOptions(secrets) {
  const rows = Array.isArray(secrets?.secrets) ? secrets.secrets : [];
  return rows.filter((secret) => {
    const haystack = [secret.id, secret.name, secret.provider, secret.description].join(" ").toLowerCase();
    return secret.has_value && (haystack.includes("openai") || haystack.includes("llm"));
  });
}

function defaultOpenAiSecretRef(secrets) {
  return llmSecretOptions(secrets)[0]?.id || "";
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

function catalogVersionRows(catalogVersions) {
  const rows = Array.isArray(catalogVersions?.catalog_versions) ? catalogVersions.catalog_versions : [];
  return rows.map((version) => ({
    key: version.id,
    section: "catalog_versions",
    value: version,
    type: version.status,
    entity: version.reason,
    kind: "catalog_version",
    summary: catalogVersionSummary(version),
  }));
}

function catalogVersionSummary(version) {
  const counts = version?.counts && typeof version.counts === "object" ? version.counts : {};
  return [
    `v${version.version_number || "-"}`,
    version.reason,
    `capabilities:${counts.capabilities || 0}`,
    `operations:${counts.operations || 0}`,
    version.snapshot_sha256 ? String(version.snapshot_sha256).slice(0, 12) : null,
  ].filter(Boolean).join(" · ");
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

function allRows(catalog, sources, ingestionRuns, secrets, proposals, catalogVersions, endpointChecks = null) {
  return [
    ...sourceRows(sources),
    ...ingestionRunRows(ingestionRuns),
    ...secretRows(secrets),
    ...sectionRows(catalog, sources, ingestionRuns, secrets, proposals, catalogVersions, "capabilities"),
    ...GRAPH_SECTIONS.flatMap((section) => graphRows(catalog, section.key)),
    ...EXECUTION_SECTIONS.flatMap((section) => executionRows(catalog, section.key, endpointChecks)),
    ...sectionRows(catalog, sources, ingestionRuns, secrets, proposals, catalogVersions, "proposals"),
    ...catalogVersionRows(catalogVersions),
  ];
}

function sectionLabel(sectionKey) {
  return SECTIONS.find((section) => section.key === sectionKey)?.label || sectionKey;
}

function summaryFor(section, data) {
  if (!data || typeof data !== "object") return "";
  if (data.description) return data.description;
  if (section === "proposals") return proposalSummary(data);
  if (section === "catalog_versions") return catalogVersionSummary(data);
  if (section === "capabilities") return data.description_ko || data.description || "";
  if (section === "entities") return data.description_ko || data.name_ko || data.entity_type || "";
  if (section === "entity_identifiers") return `${data.entity_id || "-"} -> ${data.semantic_type_id || "-"}`;
  if (section === "semantic_types") return data.description_ko || data.entity || "";
  if (section === "semantic_join_rules") return `${data.from_semantic_type_id || "-"} -> ${data.to_semantic_type_id || "-"}`;
  if (section === "capability_entity_links") return `${data.capability_id || "-"} ${data.role || ""} ${data.entity_id || "-"}`;
  if (section === "capability_dependencies") return `${data.capability_id || "-"} -> ${data.depends_on_capability_id || "-"}`;
  if (section === "planning_examples") return data.question || "";
  if (section === "sources") return sourceSummary(data);
  if (section === "ingestion_runs") return ingestionRunSummary(data);
  if (section === "secrets") return [data.provider, data.name, data.value_preview || (data.has_value ? "value configured" : "no value")].filter(Boolean).join(" · ");
  if (section === "resources") return data.base_url || data.name_ko || "";
  if (section === "operations") return `${data.method || "-"} ${data.path || ""}`.trim();
  if (section === "operation_contracts") return `${data.provider || "-"} ${data.method || ""} ${data.path || ""}`.trim();
  if (section === "operation_variants") return `${data.operation_id || "-"} -> ${data.capability_id || "-"}`;
  if (section === "field_mappings") return `${data.raw_name || "-"} -> ${data.semantic_type_id || "-"}`;
  if (section === "endpoint_checks") return `${data.status || "-"} ${data.operation_id || ""} ${data.variant_id || ""}`.trim();
  return "";
}

function displaySecretId(id) {
  const value = String(id || "");
  return value.startsWith("secret.") ? value.slice("secret.".length) : value;
}

function displaySecretRefs(refs) {
  return (Array.isArray(refs) ? refs : [])
    .map((item) => displaySecretId(item))
    .filter(Boolean)
    .join(",");
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

function suggestSourceId(provider, title) {
  const providerSlug = slugValue(provider || "unknown");
  let titleSlug = slugValue(title || "");
  if (titleSlug === "unknown") titleSlug = "document";
  return `source.${providerSlug}.${titleSlug}`;
}

function slugValue(value) {
  const slug = String(value || "")
    .normalize("NFKC")
    .trim()
    .toLowerCase()
    .replace(/[^\p{L}\p{N}._-]+/gu, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");
  return slug || "unknown";
}

function filterRows(rows, query) {
  const text = query.trim().toLowerCase();
  if (!text) return rows;
  return rows.filter((row) => JSON.stringify({ key: row.key, value: row.value }).toLowerCase().includes(text));
}

function sectionCounts(catalog, sources, ingestionRuns, secrets, proposals, catalogVersions, endpointChecks = null) {
  const endpointCheckCount = Array.isArray(endpointChecks?.endpoint_checks) ? endpointChecks.endpoint_checks.length : 0;
  return {
    sources: Array.isArray(sources?.sources) ? sources.sources.length : 0,
    ingestion_runs: Array.isArray(ingestionRuns?.ingestion_runs) ? ingestionRuns.ingestion_runs.length : 0,
    secrets: Array.isArray(secrets?.secrets) ? secrets.secrets.length : 0,
    capabilities: countObject(catalog?.capabilities),
    execution: countObject(catalog?.operations),
    semantic_graph: countObject(catalog?.entities),
    proposals: proposalRows(proposals).length,
    catalog_versions: Array.isArray(catalogVersions?.catalog_versions) ? catalogVersions.catalog_versions.length : 0,
    semantic_types: countObject(catalog?.semantic_types),
    entities: countObject(catalog?.entities),
    entity_identifiers: countObject(catalog?.entity_identifiers),
    semantic_join_rules: countObject(catalog?.semantic_join_rules),
    capability_entity_links: countObject(catalog?.capability_entity_links),
    capability_dependencies: countObject(catalog?.capability_dependencies),
    planning_examples: countObject(catalog?.planning_examples),
    endpoint_checks: endpointCheckCount,
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
