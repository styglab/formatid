"use client";

import "@xyflow/react/dist/style.css";

import type { FormEvent } from "react";
import { useEffect, useMemo, useState } from "react";
import { useTheme } from "next-themes";
import {
  Background,
  BaseEdge,
  type Connection,
  type HandleType,
  Controls,
  EdgeLabelRenderer,
  Handle,
  MarkerType,
  Position,
  ReactFlow,
  applyNodeChanges,
  getSmoothStepPath,
  type Edge,
  type EdgeProps,
  type Node,
  type NodeChange,
  type NodeProps
} from "@xyflow/react";
import {
  AlertTriangle,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Database,
  Eye,
  FileText,
  Fingerprint,
  GitBranch,
  LoaderCircle,
  PencilLine,
  Plus,
  Search,
  Sparkles,
  Trash2
} from "lucide-react";
import { Sidebar } from "@/components/layout/sidebar";
import { Topbar } from "@/components/layout/topbar";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";

type Language = "ko" | "en";
type ModuleView = "semantic-model" | "execution-contracts" | "governance";
type SemanticTab = "registry" | "graph";
type DrawerKind =
  | "create"
  | "semantic"
  | "relationship"
  | "create-relationship"
  | "create-source"
  | "source"
  | null;

type SemanticType = {
  id: string;
  name: string;
  description?: string;
  datatype?: string;
  entity_kind?: string;
  parent_entity_id?: string;
  parent_entity_name?: string;
  semantic_role?: string;
  aliases?: string[];
  owners?: string[];
  tags?: string[];
  status?: string;
  created_at?: string | null;
  updated_at?: string | null;
  draft_change_type?: string;
  draft_snapshot?: SemanticType | null;
  approved_snapshot?: SemanticType | null;
  pending_proposal_id?: string;
};

type SemanticRelationship = {
  id: string;
  source_id: string;
  source_name: string;
  target_id: string;
  target_name: string;
  relation_type: string;
  status?: string;
  created_at?: string | null;
  updated_at?: string | null;
  draft_change_type?: string;
  draft_snapshot?: SemanticRelationship | null;
  approved_snapshot?: SemanticRelationship | null;
  pending_proposal_id?: string;
};

type ExecutionSource = {
  id: string;
  name: string;
  provider?: string;
  source_type: string;
  description?: string;
  status?: string;
  config?: Record<string, unknown>;
  created_at?: string | null;
  updated_at?: string | null;
  draft_change_type?: string;
  draft_snapshot?: ExecutionSource | null;
  approved_snapshot?: ExecutionSource | null;
  pending_proposal_id?: string;
};

type Proposal = {
  id: string;
  title: string;
  entity_type: string;
  entity_id?: string;
  change_type: string;
  status: string;
  payload?: Record<string, unknown>;
  created_at?: string | null;
  reviewed_at?: string | null;
  reviewed_by?: string | null;
};

type Overview = {
  counts: {
    semantic_types: number;
    approved_semantic_types: number;
    draft_semantic_types: number;
    execution_sources?: number;
    pending_proposals: number;
    relationships: number;
  };
  recent_proposals: Proposal[];
};

type SemanticTypeForm = {
  name: string;
  description: string;
  datatype: string;
  entityKind: string;
  parentEntityId: string;
  aliases: string;
  owners: string;
};

type RelationshipForm = {
  sourceId: string;
  targetId: string;
  relationType: string;
};

type ExecutionSourceForm = {
  name: string;
  provider: string;
  sourceType: string;
  description: string;
  inputMode: string;
  referenceUri: string;
  manualNotes: string;
};

type ToastState = {
  tone: "success" | "error";
  text: string;
} | null;

const API_BASE = process.env.NEXT_PUBLIC_SEMANTIC_LAYER_API_URL || "/semantic-layer";
const TYPES_PAGE_SIZE = 10;
const ATTRIBUTES_PAGE_SIZE = 10;
const SOURCES_PAGE_SIZE = 10;
const PROPOSALS_PAGE_SIZE = 10;

const text = {
  ko: {
    title: "Semantic Model",
    search: "entity 검색...",
    language: { ko: "한글", en: "EN" },
    mode: { light: "밝게", dark: "어둡게" },
    filters: { all: "Current", approved: "Approved", draft: "Pending" },
    actions: {
      create: "새 semantic type",
      createEntity: "새 entity",
      createAttribute: "새 attribute",
      createSource: "새 source",
      save: "수정 저장",
      delete: "삭제 요청",
      openEditor: "편집 열기",
      createSubmit: "생성하고 review 등록",
      createRelation: "관계 추가",
      updateRelation: "관계 수정",
      deleteRelation: "관계 삭제",
      approve: "승인",
      reject: "거절",
      close: "닫기"
    },
    sections: {
      table: "Table",
      inspector: "Inspector",
      graph: "Graph",
      sources: "Sources",
      relationships: "Relationships",
      review: "Reviews"
    },
    fields: {
      name: "이름",
      description: "설명",
      datatype: "데이터 타입",
      kind: "종류",
      parentEntity: "상위 엔터티",
      aliases: "별칭",
      owners: "오너",
      provider: "Provider",
      sourceType: "Source type",
      inputMode: "Input mode",
      referenceUri: "Reference URI",
      manualNotes: "Manual notes",
      source: "Source",
      target: "Target",
      relation: "Relation"
    },
    status: {
      approved: "승인됨",
      draft: "초안",
      pending: "검토 대기",
      loading: "불러오는 중",
      emptyTypes: "등록된 semantic type이 없습니다.",
      emptyRelations: "등록된 relationship이 없습니다.",
      emptyProposals: "대기 중인 proposal이 없습니다.",
      emptySelection: "왼쪽 목록에서 semantic type를 선택하세요.",
      emptyReviewSelection: "왼쪽 큐에서 proposal을 선택하세요."
    },
    messages: {
      created: "semantic type created",
      updated: "semantic type updated",
      deleted: "semantic type delete proposal created",
      sourceCreated: "execution source created",
      sourceUpdated: "execution source updated",
      sourceDeleted: "execution source delete proposal created",
      relationCreated: "relationship draft created",
      relationUpdated: "relationship update proposal created",
      relationDeleted: "relationship delete proposal created"
    },
    confirm: {
      delete: "선택한 semantic type의 삭제 proposal을 생성합니다. 계속할까요?",
      deleteSource: "선택한 execution source의 삭제 proposal을 생성합니다. 계속할까요?",
      deleteRelation: "선택한 relationship의 삭제 proposal을 생성합니다. 계속할까요?"
    },
    tabs: { registry: "Table", graph: "Graph" },
    drawer: {
      create: "Create Semantic Type",
      semantic: "Edit Semantic Type",
      relationship: "Edit Relationship",
      createRelationship: "Create Relationship",
      createSource: "Create Source",
      source: "Edit Source"
    }
  },
  en: {
    title: "Semantic Model",
    search: "Search entities...",
    language: { ko: "KO", en: "EN" },
    mode: { light: "Light", dark: "Dark" },
    filters: { all: "Current", approved: "Approved", draft: "Pending" },
    actions: {
      create: "New semantic type",
      createEntity: "New entity",
      createAttribute: "New attribute",
      createSource: "New source",
      save: "Save changes",
      delete: "Request delete",
      openEditor: "Open editor",
      createSubmit: "Create and open review",
      createRelation: "Add relationship",
      updateRelation: "Update relationship",
      deleteRelation: "Delete relationship",
      approve: "Approve",
      reject: "Reject",
      close: "Close"
    },
    sections: {
      table: "Table",
      inspector: "Inspector",
      graph: "Graph",
      sources: "Sources",
      relationships: "Relationships",
      review: "Reviews"
    },
    fields: {
      name: "Name",
      description: "Description",
      datatype: "Datatype",
      kind: "Kind",
      parentEntity: "Parent entity",
      aliases: "Aliases",
      owners: "Owners",
      provider: "Provider",
      sourceType: "Source type",
      inputMode: "Input mode",
      referenceUri: "Reference URI",
      manualNotes: "Manual notes",
      source: "Source",
      target: "Target",
      relation: "Relation"
    },
    status: {
      approved: "Approved",
      draft: "Draft",
      pending: "Pending review",
      loading: "Loading",
      emptyTypes: "No semantic types yet.",
      emptyRelations: "No semantic relationships yet.",
      emptyProposals: "No pending proposals.",
      emptySelection: "Select a semantic type from the table.",
      emptyReviewSelection: "Select a proposal from the queue."
    },
    messages: {
      created: "semantic type created",
      updated: "semantic type updated",
      deleted: "semantic type delete proposal created",
      sourceCreated: "execution source created",
      sourceUpdated: "execution source updated",
      sourceDeleted: "execution source delete proposal created",
      relationCreated: "relationship draft created",
      relationUpdated: "relationship update proposal created",
      relationDeleted: "relationship delete proposal created"
    },
    confirm: {
      delete: "This will create a delete proposal for the selected semantic type. Continue?",
      deleteSource: "This will create a delete proposal for the selected execution source. Continue?",
      deleteRelation: "This will create a delete proposal for the selected relationship. Continue?"
    },
    tabs: { registry: "Table", graph: "Graph" },
    drawer: {
      create: "Create Semantic Type",
      semantic: "Edit Semantic Type",
      relationship: "Edit Relationship",
      createRelationship: "Create Relationship",
      createSource: "Create Source",
      source: "Edit Source"
    }
  }
};

const semanticTypeDefaults: SemanticTypeForm = {
  name: "",
  description: "",
  datatype: "string",
  entityKind: "entity",
  parentEntityId: "",
  aliases: "",
  owners: "platform"
};

const relationshipDefaults: RelationshipForm = {
  sourceId: "",
  targetId: "",
  relationType: "related_to"
};

const executionSourceDefaults: ExecutionSourceForm = {
  name: "",
  provider: "",
  sourceType: "api",
  description: "",
  inputMode: "document",
  referenceUri: "",
  manualNotes: ""
};

function hasPendingOverlay(item: {
  status?: string;
  draft_snapshot?: unknown;
  draft_change_type?: string;
  pending_proposal_id?: string;
}) {
  return Boolean(
    item.status === "draft" || item.draft_snapshot || item.draft_change_type || item.pending_proposal_id
  );
}

function isDraftCreate(item: {
  status?: string;
  draft_change_type?: string;
  approved_snapshot?: unknown;
}) {
  return item.status === "draft" || item.draft_change_type === "create" || !item.approved_snapshot;
}

export default function Page() {
  const [language, setLanguage] = useState<Language>("ko");
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [overview, setOverview] = useState<Overview | null>(null);
  const [semanticTypes, setSemanticTypes] = useState<SemanticType[]>([]);
  const [executionSources, setExecutionSources] = useState<ExecutionSource[]>([]);
  const [relationships, setRelationships] = useState<SemanticRelationship[]>([]);
  const [proposals, setProposals] = useState<Proposal[]>([]);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState("");
  const [toast, setToast] = useState<ToastState>(null);
  const [activeModule, setActiveModule] = useState<ModuleView>("semantic-model");
  const [activeTab, setActiveTab] = useState<SemanticTab>("registry");
  const [currentTypesPage, setCurrentTypesPage] = useState(1);
  const [currentAttributesPage, setCurrentAttributesPage] = useState(1);
  const [currentSourcesPage, setCurrentSourcesPage] = useState(1);
  const [currentProposalsPage, setCurrentProposalsPage] = useState(1);
  const [openDrawer, setOpenDrawer] = useState<DrawerKind>(null);
  const [selectedSemanticTypeId, setSelectedSemanticTypeId] = useState("");
  const [selectedExecutionSourceId, setSelectedExecutionSourceId] = useState("");
  const [selectedRelationshipId, setSelectedRelationshipId] = useState("");
  const [selectedProposalId, setSelectedProposalId] = useState("");
  const [selectedProposalIds, setSelectedProposalIds] = useState<string[]>([]);
  const [semanticTypeForm, setSemanticTypeForm] = useState<SemanticTypeForm>(semanticTypeDefaults);
  const [createSemanticTypeForm, setCreateSemanticTypeForm] = useState<SemanticTypeForm>(semanticTypeDefaults);
  const [executionSourceForm, setExecutionSourceForm] = useState<ExecutionSourceForm>(executionSourceDefaults);
  const [createExecutionSourceForm, setCreateExecutionSourceForm] = useState<ExecutionSourceForm>(executionSourceDefaults);
  const [relationshipForm, setRelationshipForm] = useState<RelationshipForm>(relationshipDefaults);
  const [relationshipEditForm, setRelationshipEditForm] = useState<RelationshipForm>(relationshipDefaults);
  const [searchQuery, setSearchQuery] = useState("");
  const [sourceQuery, setSourceQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState<"all" | "approved" | "draft">("all");
  const { resolvedTheme, setTheme } = useTheme();
  const labels = text[language];
  const screenMode = resolvedTheme === "dark" ? "dark" : "light";
  const canEditCurrentView = statusFilter === "all";

  useEffect(() => {
    void loadData();
  }, []);

  useEffect(() => {
    if (!toast) {
      return;
    }
    const timeoutId = window.setTimeout(() => setToast(null), 3200);
    return () => window.clearTimeout(timeoutId);
  }, [toast]);

  useEffect(() => {
    setCurrentTypesPage(1);
  }, [searchQuery, statusFilter]);

  useEffect(() => {
    if (!openDrawer) {
      return;
    }
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setOpenDrawer(null);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [openDrawer]);

  const selectedSemanticType = useMemo(
    () => semanticTypes.find((item) => item.id === selectedSemanticTypeId) || null,
    [selectedSemanticTypeId, semanticTypes]
  );
  const selectedRelationship = useMemo(
    () => relationships.find((item) => item.id === selectedRelationshipId) || null,
    [relationships, selectedRelationshipId]
  );
  const selectedExecutionSource = useMemo(
    () => executionSources.find((item) => item.id === selectedExecutionSourceId) || null,
    [executionSources, selectedExecutionSourceId]
  );
  const selectedProposal = useMemo(
    () => proposals.find((item) => item.id === selectedProposalId) || null,
    [proposals, selectedProposalId]
  );
  const selectedSemanticDisplay = selectedSemanticType ? selectedSemanticType.draft_snapshot || selectedSemanticType : null;
  const selectedEntityContextId = selectedSemanticDisplay
    ? selectedSemanticDisplay.entity_kind === "entity"
      ? selectedSemanticDisplay.id
      : selectedSemanticDisplay.parent_entity_id || ""
    : "";

  useEffect(() => {
    setCurrentAttributesPage(1);
  }, [selectedEntityContextId]);

  useEffect(() => {
    setCurrentSourcesPage(1);
  }, [sourceQuery, statusFilter]);

  useEffect(() => {
    setCurrentProposalsPage(1);
  }, [proposals.length]);

  const entitySemanticTypes = useMemo(
    () => semanticTypes.filter((item) => (item.draft_snapshot || item).entity_kind === "entity"),
    [semanticTypes]
  );
  const attributeSemanticTypes = useMemo(
    () => semanticTypes.filter((item) => (item.draft_snapshot || item).entity_kind === "attribute"),
    [semanticTypes]
  );

  const filteredSemanticTypes = useMemo(() => {
    const lowered = searchQuery.trim().toLowerCase();
    return entitySemanticTypes.filter((item) => {
      const hasPending = hasPendingOverlay(item);
      const statusMatches =
        statusFilter === "all"
          ? true
          : statusFilter === "approved"
            ? !isDraftCreate(item)
            : hasPending;
      const searchMatches =
        !lowered ||
        item.name.toLowerCase().includes(lowered) ||
        (item.description || "").toLowerCase().includes(lowered) ||
        (item.aliases || []).some((alias) => alias.toLowerCase().includes(lowered));
      return statusMatches && searchMatches;
    });
  }, [entitySemanticTypes, searchQuery, statusFilter]);

  const totalTypesPages = Math.max(1, Math.ceil(filteredSemanticTypes.length / TYPES_PAGE_SIZE));
  const paginatedSemanticTypes = useMemo(() => {
    const start = (currentTypesPage - 1) * TYPES_PAGE_SIZE;
    return filteredSemanticTypes.slice(start, start + TYPES_PAGE_SIZE);
  }, [currentTypesPage, filteredSemanticTypes]);

  const filterRelationshipByView = (item: SemanticRelationship) => {
    const hasPending = hasPendingOverlay(item);
    if (statusFilter === "approved") {
      return !isDraftCreate(item);
    }
    if (statusFilter === "draft") {
      return hasPending;
    }
    return true;
  };

  const filteredRelationshipsByView = useMemo(
    () => relationships.filter(filterRelationshipByView),
    [relationships, statusFilter]
  );

  const relationshipCountMap = useMemo(() => {
    const counts = new Map<string, number>();
    for (const relationship of filteredRelationshipsByView) {
      counts.set(relationship.source_id, (counts.get(relationship.source_id) || 0) + 1);
      counts.set(relationship.target_id, (counts.get(relationship.target_id) || 0) + 1);
    }
    return counts;
  }, [filteredRelationshipsByView]);

  const visibleRelationships = useMemo(() => {
    if (!selectedSemanticTypeId) {
      return filteredRelationshipsByView;
    }
    return filteredRelationshipsByView.filter(
      (item) =>
        (item.source_id === selectedSemanticTypeId || item.target_id === selectedSemanticTypeId)
    );
  }, [filteredRelationshipsByView, selectedSemanticTypeId]);

  const selectedSemanticAttributes = useMemo(() => {
    if (!selectedEntityContextId) {
      return [];
    }
    return semanticTypes.filter((item) => {
      const display = item.draft_snapshot || item;
      return display.entity_kind === "attribute" && display.parent_entity_id === selectedEntityContextId;
    });
  }, [selectedEntityContextId, semanticTypes]);

  const totalAttributePages = Math.max(1, Math.ceil(selectedSemanticAttributes.length / ATTRIBUTES_PAGE_SIZE));
  const paginatedAttributes = useMemo(() => {
    const start = (currentAttributesPage - 1) * ATTRIBUTES_PAGE_SIZE;
    return selectedSemanticAttributes.slice(start, start + ATTRIBUTES_PAGE_SIZE);
  }, [currentAttributesPage, selectedSemanticAttributes]);

  const semanticModelGraphTypes = useMemo(() => {
    const visibleEntityIds = new Set(filteredSemanticTypes.map((item) => item.id));
    if (statusFilter === "draft") {
      filteredRelationshipsByView.forEach((item) => {
        if (hasPendingOverlay(item)) {
          visibleEntityIds.add(item.source_id);
          visibleEntityIds.add(item.target_id);
        }
      });
    }
    return semanticTypes.filter((item) => {
      const display = item.draft_snapshot || item;
      if (display.entity_kind === "entity") {
        return visibleEntityIds.has(item.id);
      }
      if (display.entity_kind === "attribute") {
        return Boolean(display.parent_entity_id && visibleEntityIds.has(display.parent_entity_id));
      }
      return false;
    });
  }, [filteredRelationshipsByView, filteredSemanticTypes, semanticTypes, statusFilter]);

  const semanticModelGraphRelationships = useMemo(() => {
    const visibleEntityIds = new Set(semanticModelGraphTypes.map((item) => item.id));
    return filteredRelationshipsByView.filter(
      (item) => visibleEntityIds.has(item.source_id) && visibleEntityIds.has(item.target_id)
    );
  }, [filteredRelationshipsByView, semanticModelGraphTypes]);

  const filteredExecutionSources = useMemo(() => {
    const lowered = sourceQuery.trim().toLowerCase();
    return executionSources.filter((item) => {
      const display = item.draft_snapshot || item;
      const hasPending = hasPendingOverlay(item);
      const statusMatches =
        statusFilter === "all"
          ? true
          : statusFilter === "approved"
            ? !isDraftCreate(item)
            : hasPending;
      const searchMatches =
        !lowered ||
        display.name.toLowerCase().includes(lowered) ||
        (display.provider || "").toLowerCase().includes(lowered) ||
        (display.description || "").toLowerCase().includes(lowered);
      return statusMatches && searchMatches;
    });
  }, [executionSources, sourceQuery, statusFilter]);

  const totalSourcesPages = Math.max(1, Math.ceil(filteredExecutionSources.length / SOURCES_PAGE_SIZE));
  const paginatedExecutionSources = useMemo(() => {
    const start = (currentSourcesPage - 1) * SOURCES_PAGE_SIZE;
    return filteredExecutionSources.slice(start, start + SOURCES_PAGE_SIZE);
  }, [currentSourcesPage, filteredExecutionSources]);

  const graphViewMode = statusFilter === "approved" ? "approved" : "draft";
  const sortedProposals = useMemo(
    () =>
      [...proposals].sort(
        (a, b) => new Date(b.created_at || 0).getTime() - new Date(a.created_at || 0).getTime()
      ),
    [proposals]
  );
  const totalProposalPages = Math.max(1, Math.ceil(sortedProposals.length / PROPOSALS_PAGE_SIZE));
  const paginatedProposals = useMemo(() => {
    const start = (currentProposalsPage - 1) * PROPOSALS_PAGE_SIZE;
    return sortedProposals.slice(start, start + PROPOSALS_PAGE_SIZE);
  }, [currentProposalsPage, sortedProposals]);
  const allPageProposalsSelected =
    paginatedProposals.length > 0 && paginatedProposals.every((proposal) => selectedProposalIds.includes(proposal.id));

  async function loadData() {
    setLoading(true);
    try {
      const [overviewResponse, typesResponse, sourcesResponse, relationshipsResponse, proposalsResponse] = await Promise.all([
        fetchJson<Overview>("/api/overview"),
        fetchJson<SemanticType[]>("/api/semantic-types"),
        fetchJson<ExecutionSource[]>("/api/execution-sources"),
        fetchJson<SemanticRelationship[]>("/api/semantic-relationships"),
        fetchJson<Proposal[]>("/api/proposals?status=pending_review")
      ]);
      setOverview(overviewResponse);
      setSemanticTypes(typesResponse);
      setExecutionSources(sourcesResponse);
      setRelationships(relationshipsResponse);
      setProposals(proposalsResponse);

      const entityCandidates = typesResponse.filter((item) => (item.draft_snapshot || item).entity_kind === "entity");
      const refreshedType =
        typesResponse.find((item) => item.id === selectedSemanticTypeId) || entityCandidates[0] || null;
      if (refreshedType) {
        selectSemanticType(refreshedType);
      } else {
        setSelectedSemanticTypeId("");
      }

      const refreshedRelationship =
        relationshipsResponse.find((item) => item.id === selectedRelationshipId) || relationshipsResponse[0] || null;
      if (refreshedRelationship) {
        setSelectedRelationshipId(refreshedRelationship.id);
        fillRelationshipEditForm(refreshedRelationship);
      } else {
        setSelectedRelationshipId("");
      }

      const refreshedSource =
        sourcesResponse.find((item) => item.id === selectedExecutionSourceId) || sourcesResponse[0] || null;
      if (refreshedSource) {
        selectExecutionSource(refreshedSource);
      } else {
        setSelectedExecutionSourceId("");
      }

      if (proposalsResponse.length) {
        setSelectedProposalId((current) =>
          proposalsResponse.some((proposal) => proposal.id === current) ? current : proposalsResponse[0].id
        );
      } else {
        setSelectedProposalId("");
      }
      setSelectedProposalIds((current) => current.filter((id) => proposalsResponse.some((proposal) => proposal.id === id)));

      return {
        semanticTypes: typesResponse,
        executionSources: sourcesResponse,
        relationships: relationshipsResponse,
        proposals: proposalsResponse
      };
    } catch (requestError) {
      showToast("error", requestError instanceof Error ? requestError.message : "Failed to load semantic model.");
      return null;
    } finally {
      setLoading(false);
    }
  }

  function showToast(tone: "success" | "error", text: string) {
    setToast({ tone, text });
  }

  function fillSemanticForm(semanticType: SemanticType) {
    const source = semanticType.draft_snapshot || semanticType;
    setSemanticTypeForm({
      name: source.name,
      description: source.description || "",
      datatype: source.datatype || "string",
      entityKind: source.entity_kind || "attribute",
      parentEntityId: source.parent_entity_id || "",
      aliases: (source.aliases || []).join(", "),
      owners: (source.owners || []).join(", ")
    });
  }

  function fillRelationshipEditForm(relationship: SemanticRelationship) {
    setRelationshipEditForm({
      sourceId: relationship.source_id,
      targetId: relationship.target_id,
      relationType: relationship.relation_type
    });
  }

  function fillExecutionSourceForm(source: ExecutionSource) {
    const display = source.draft_snapshot || source;
    const config = (display.config || {}) as Record<string, unknown>;
    setExecutionSourceForm({
      name: display.name,
      provider: display.provider || "",
      sourceType: display.source_type,
      description: display.description || "",
      inputMode: String(config.input_mode || "document"),
      referenceUri: String(config.reference_uri || ""),
      manualNotes: String(config.manual_notes || "")
    });
  }

  function selectSemanticType(semanticType: SemanticType, options?: { openDrawer?: boolean }) {
    setSelectedSemanticTypeId(semanticType.id);
    fillSemanticForm(semanticType);
    setRelationshipForm((current) => ({ ...current, sourceId: semanticType.id }));
    if (options?.openDrawer && canEditCurrentView) {
      setOpenDrawer("semantic");
    }
  }

  function selectRelationship(relationship: SemanticRelationship, options?: { openDrawer?: boolean }) {
    setSelectedRelationshipId(relationship.id);
    fillRelationshipEditForm(relationship);
    if (options?.openDrawer && canEditCurrentView) {
      setOpenDrawer("relationship");
    }
  }

  function selectExecutionSource(source: ExecutionSource, options?: { openDrawer?: boolean }) {
    setSelectedExecutionSourceId(source.id);
    fillExecutionSourceForm(source);
    if (options?.openDrawer && canEditCurrentView) {
      setOpenDrawer("source");
    }
  }

  function clearGraphSelection() {
    setSelectedSemanticTypeId("");
    setSelectedRelationshipId("");
  }

  function openCreateEntityDrawer() {
    if (!canEditCurrentView) return;
    setCreateSemanticTypeForm({ ...semanticTypeDefaults, entityKind: "entity", parentEntityId: "" });
    setOpenDrawer("create");
  }

  function openCreateAttributeDrawer(parentEntityId: string) {
    if (!canEditCurrentView) return;
    setCreateSemanticTypeForm({
      ...semanticTypeDefaults,
      entityKind: "attribute",
      parentEntityId,
      datatype: "string"
    });
    setOpenDrawer("create");
  }

  function openCreateSourceDrawer() {
    if (!canEditCurrentView) return;
    setCreateExecutionSourceForm(executionSourceDefaults);
    setOpenDrawer("create-source");
  }

  async function submitCreateSemanticType(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting("create-semantic-type");
    try {
      const payload = {
        name: createSemanticTypeForm.name.trim(),
        description: createSemanticTypeForm.description.trim(),
        datatype: createSemanticTypeForm.datatype,
        entity_kind: createSemanticTypeForm.entityKind,
        parent_entity_id: createSemanticTypeForm.parentEntityId,
        semantic_role: "",
        aliases: commaList(createSemanticTypeForm.aliases),
        owners: commaList(createSemanticTypeForm.owners)
      };
      await fetchJson("/api/semantic-types", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      const refreshed = await loadData();
      const created = refreshed?.semanticTypes.find((item) => item.name === payload.name);
      if (created) {
        selectSemanticType(created);
      }
      setCreateSemanticTypeForm(semanticTypeDefaults);
      setOpenDrawer(null);
      showToast("success", labels.messages.created);
    } catch (requestError) {
      showToast("error", requestError instanceof Error ? requestError.message : "Failed to create semantic type.");
    } finally {
      setSubmitting("");
    }
  }

  async function submitSemanticType(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selectedSemanticTypeId) {
      return;
    }
    setSubmitting("semantic-type");
    try {
      await fetchJson(`/api/semantic-types/${selectedSemanticTypeId}`, {
        method: "PATCH",
        body: JSON.stringify({
          name: semanticTypeForm.name.trim(),
          description: semanticTypeForm.description.trim(),
          datatype: semanticTypeForm.datatype,
          entity_kind: semanticTypeForm.entityKind,
          parent_entity_id: semanticTypeForm.parentEntityId,
          semantic_role: "",
          aliases: commaList(semanticTypeForm.aliases),
          owners: commaList(semanticTypeForm.owners)
        })
      });
      await loadData();
      setOpenDrawer(null);
      showToast("success", labels.messages.updated);
    } catch (requestError) {
      showToast("error", requestError instanceof Error ? requestError.message : "Failed to save semantic type.");
    } finally {
      setSubmitting("");
    }
  }

  async function submitRelationship(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!relationshipForm.sourceId || !relationshipForm.targetId) {
      showToast("error", "source and target semantic types are required");
      return;
    }
    setSubmitting("relationship");
    try {
      await fetchJson(`/api/semantic-types/${relationshipForm.sourceId}/relationships`, {
        method: "POST",
        body: JSON.stringify({
          target_id: relationshipForm.targetId,
          relation_type: relationshipForm.relationType.trim()
        })
      });
      await loadData();
      setOpenDrawer(null);
      setRelationshipForm(relationshipDefaults);
      showToast("success", labels.messages.relationCreated);
    } catch (requestError) {
      showToast("error", requestError instanceof Error ? requestError.message : "Failed to create relationship.");
    } finally {
      setSubmitting("");
    }
  }

  async function submitCreateExecutionSource(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting("create-execution-source");
    try {
      const payload = executionSourcePayloadFromForm(createExecutionSourceForm);
      await fetchJson("/api/execution-sources", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      const refreshed = await loadData();
      const created = refreshed?.executionSources.find((item) => item.name === payload.name);
      if (created) {
        selectExecutionSource(created);
      }
      setCreateExecutionSourceForm(executionSourceDefaults);
      setOpenDrawer(null);
      showToast("success", labels.messages.sourceCreated);
    } catch (requestError) {
      showToast("error", requestError instanceof Error ? requestError.message : "Failed to create execution source.");
    } finally {
      setSubmitting("");
    }
  }

  async function submitExecutionSource(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selectedExecutionSourceId) {
      return;
    }
    setSubmitting("execution-source");
    try {
      await fetchJson(`/api/execution-sources/${selectedExecutionSourceId}`, {
        method: "PATCH",
        body: JSON.stringify(executionSourcePayloadFromForm(executionSourceForm))
      });
      await loadData();
      setOpenDrawer(null);
      showToast("success", labels.messages.sourceUpdated);
    } catch (requestError) {
      showToast("error", requestError instanceof Error ? requestError.message : "Failed to save execution source.");
    } finally {
      setSubmitting("");
    }
  }

  async function submitRelationshipEdit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selectedRelationshipId) {
      return;
    }
    setSubmitting("relationship-edit");
    try {
      await fetchJson(`/api/semantic-relationships/${selectedRelationshipId}`, {
        method: "PATCH",
        body: JSON.stringify({
          source_id: relationshipEditForm.sourceId,
          target_id: relationshipEditForm.targetId,
          relation_type: relationshipEditForm.relationType.trim()
        })
      });
      await loadData();
      setOpenDrawer(null);
      showToast("success", labels.messages.relationUpdated);
    } catch (requestError) {
      showToast("error", requestError instanceof Error ? requestError.message : "Failed to update relationship.");
    } finally {
      setSubmitting("");
    }
  }

  async function reviewProposal(proposalId: string, decision: "approve" | "reject") {
    setSubmitting(proposalId);
    try {
      await fetchJson(`/api/proposals/${proposalId}/${decision}`, {
        method: "POST",
        body: JSON.stringify({ reviewer: "dashboard" })
      });
      await loadData();
      setSelectedProposalIds((current) => current.filter((item) => item !== proposalId));
      showToast("success", `proposal ${decision}d`);
    } catch (requestError) {
      showToast("error", requestError instanceof Error ? requestError.message : "Failed to review proposal.");
    } finally {
      setSubmitting("");
    }
  }

  async function reviewSelectedProposals(decision: "approve" | "reject") {
    if (!selectedProposalIds.length) {
      return;
    }
    setSubmitting(`bulk-${decision}`);
    try {
      for (const proposalId of selectedProposalIds) {
        await fetchJson(`/api/proposals/${proposalId}/${decision}`, {
          method: "POST",
          body: JSON.stringify({ reviewer: "dashboard" })
        });
      }
      await loadData();
      setSelectedProposalIds([]);
      showToast("success", `${selectedProposalIds.length} proposals ${decision}d`);
    } catch (requestError) {
      showToast("error", requestError instanceof Error ? requestError.message : "Failed to review proposals.");
    } finally {
      setSubmitting("");
    }
  }

  async function deleteSemanticType() {
    if (!selectedSemanticTypeId || !window.confirm(labels.confirm.delete)) {
      return;
    }
    setSubmitting("delete-semantic-type");
    try {
      await fetchJson(`/api/semantic-types/${selectedSemanticTypeId}`, { method: "DELETE" });
      setSelectedSemanticTypeId("");
      setOpenDrawer(null);
      await loadData();
      showToast("success", labels.messages.deleted);
    } catch (requestError) {
      showToast("error", requestError instanceof Error ? requestError.message : "Failed to delete semantic type.");
    } finally {
      setSubmitting("");
    }
  }

  async function deleteRelationship() {
    if (!selectedRelationshipId || !window.confirm(labels.confirm.deleteRelation)) {
      return;
    }
    setSubmitting("delete-relationship");
    try {
      await fetchJson(`/api/semantic-relationships/${selectedRelationshipId}`, { method: "DELETE" });
      setSelectedRelationshipId("");
      setOpenDrawer(null);
      await loadData();
      showToast("success", labels.messages.relationDeleted);
    } catch (requestError) {
      showToast("error", requestError instanceof Error ? requestError.message : "Failed to delete relationship.");
    } finally {
      setSubmitting("");
    }
  }

  async function deleteExecutionSource() {
    if (!selectedExecutionSourceId || !window.confirm(labels.confirm.deleteSource)) {
      return;
    }
    setSubmitting("delete-execution-source");
    try {
      await fetchJson(`/api/execution-sources/${selectedExecutionSourceId}`, { method: "DELETE" });
      setSelectedExecutionSourceId("");
      setOpenDrawer(null);
      await loadData();
      showToast("success", labels.messages.sourceDeleted);
    } catch (requestError) {
      showToast("error", requestError instanceof Error ? requestError.message : "Failed to delete execution source.");
    } finally {
      setSubmitting("");
    }
  }

  return (
    <div>
      <div className="flex min-h-screen bg-background text-foreground">
        <Sidebar
          activeItem={activeModule}
          collapsed={sidebarCollapsed}
          onToggle={() => setSidebarCollapsed((current) => !current)}
          onSelect={(itemId) => setActiveModule(itemId as ModuleView)}
        />
        <div className="flex min-w-0 flex-1 flex-col">
          <Topbar
            language={language}
            screenMode={screenMode}
            onLanguageChange={setLanguage}
            onScreenModeChange={(mode) => setTheme(mode)}
            pendingCount={proposals.length}
            notifications={proposals.slice(0, 5).map((proposal) => ({
              id: proposal.id,
              title: proposal.title,
              meta: `${proposal.entity_type} · ${proposal.change_type}`
            }))}
            labels={{
              search: labels.search,
              language: labels.language,
              mode: labels.mode
            }}
          />
          <main className="mx-auto flex w-full max-w-[1680px] flex-1 flex-col p-4 lg:p-5">
            <section className="mb-3 flex items-center justify-between gap-3">
              <div className="flex items-center gap-2 text-sm">
                <h1 className="text-lg font-semibold tracking-tight">
                  {activeModule === "semantic-model"
                    ? labels.title
                    : activeModule === "execution-contracts"
                      ? "Execution Contracts"
                      : "Reviews"}
                </h1>
                {activeModule === "semantic-model" ? (
                  <>
                    <InlineStat label="Entities" value={entitySemanticTypes.length} />
                    <InlineStat label="Attributes" value={attributeSemanticTypes.length} />
                    <InlineStat label="Relations" value={overview?.counts.relationships ?? 0} />
                    <InlineStat label="Reviews" value={overview?.counts.pending_proposals ?? 0} />
                  </>
                ) : null}
                {activeModule === "execution-contracts" ? (
                  <>
                    <InlineStat label="Sources" value={executionSources.length} />
                    <InlineStat label="Pending" value={overview?.counts.pending_proposals ?? 0} />
                  </>
                ) : null}
              </div>
            </section>

            {activeModule === "semantic-model" ? (
              <section className="mb-4">
                <div className="flex flex-col gap-3 rounded-xl border border-border/80 bg-card/40 px-4 py-3 lg:flex-row lg:items-center lg:justify-between">
                  <div className="flex min-w-0 flex-1 flex-col gap-3 lg:flex-row lg:items-center">
                    <div className="relative min-w-0 lg:w-[280px]">
                      <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                      <Input
                        className="border-border/80 bg-background pl-9"
                        placeholder={labels.search}
                        value={searchQuery}
                        onChange={(event) => setSearchQuery(event.target.value)}
                      />
                    </div>
                    <div className="flex gap-2">
                      {(["all", "approved", "draft"] as const).map((status) => (
                        <button
                          key={status}
                          type="button"
                          onClick={() => setStatusFilter(status)}
                          className={`rounded-full border px-2.5 py-1 text-xs transition ${
                            statusFilter === status
                              ? "border-primary/30 bg-primary text-primary-foreground"
                              : "border-border bg-background text-muted-foreground hover:bg-accent hover:text-foreground"
                          }`}
                        >
                          {labels.filters[status]}
                        </button>
                      ))}
                    </div>
                  </div>
                  <div className="inline-flex rounded-lg border border-border bg-muted/20 p-1">
                    {(["registry", "graph"] as const).map((tab) => (
                      <button
                        key={tab}
                        type="button"
                        onClick={() => setActiveTab(tab)}
                        className={`rounded-md px-3 py-1.5 text-sm transition ${
                          activeTab === tab
                            ? "bg-primary text-primary-foreground shadow-sm"
                            : "text-muted-foreground hover:bg-accent hover:text-foreground"
                        }`}
                      >
                        {labels.tabs[tab]}
                      </button>
                    ))}
                  </div>
                </div>
              </section>
            ) : null}

            {activeModule === "semantic-model" && activeTab === "registry" ? (
              <section className="space-y-4">
                <div className="grid gap-4 xl:grid-cols-[280px_340px_minmax(0,1fr)]">
                <Card className="workspace-panel flex h-[720px] flex-col rounded-2xl shadow-none">
                  <CardHeader className="relative border-b border-border p-4 pr-28">
                    <CardTitle>Entities</CardTitle>
                    <Button
                      size="sm"
                      variant="outline"
                      className="absolute right-4 top-1/2 h-7 -translate-y-1/2 px-2.5 text-[11px]"
                      onClick={openCreateEntityDrawer}
                      disabled={!canEditCurrentView}
                    >
                      <Plus className="h-4 w-4" />
                      {labels.actions.createEntity}
                    </Button>
                  </CardHeader>
                  <CardContent className="flex flex-1 flex-col p-0">
                    {loading ? (
                      <div className="p-4">
                        <RegistryPlaceholder label={labels.status.loading} />
                      </div>
                    ) : paginatedSemanticTypes.length ? (
                      <>
                        <div className="flex-1 divide-y divide-border/70 overflow-auto">
                          {paginatedSemanticTypes.map((semanticType) => {
                            const displaySemanticType = semanticType.draft_snapshot || semanticType;
                            const isSelected = semanticType.id === selectedEntityContextId;
                            return (
                              <button
                                key={semanticType.id}
                                type="button"
                                onClick={() => selectSemanticType(semanticType)}
                                className={`block w-full px-4 py-2.5 text-left transition hover:bg-muted/20 ${
                                  isSelected ? "bg-primary/5" : ""
                                }`}
                              >
                                <div className="flex items-start justify-between gap-3">
                                  <div className="min-w-0">
                                    <div className="truncate text-[13px] font-medium">{displaySemanticType.name}</div>
                                    <div className="mt-1 text-[11px] text-muted-foreground">
                                      {relationshipCountMap.get(semanticType.id) || 0} relations
                                    </div>
                                  </div>
                                  <div className="flex shrink-0 flex-col items-end gap-1">
                                    {semanticType.draft_snapshot ? <Badge variant="warning">Draft</Badge> : null}
                                  </div>
                                </div>
                              </button>
                            );
                          })}
                        </div>
                        <PaginationBar
                          currentPage={currentTypesPage}
                          totalPages={totalTypesPages}
                          onPageChange={setCurrentTypesPage}
                        />
                      </>
                    ) : (
                      <div className="p-4">
                        <EmptyState icon={Database} label={labels.status.emptyTypes} />
                      </div>
                    )}
                  </CardContent>
                </Card>

                <Card className="workspace-panel flex h-[720px] flex-col rounded-2xl shadow-none">
                  <CardHeader className="relative border-b border-border p-4 pr-32">
                    <CardTitle>Attributes</CardTitle>
                    {selectedEntityContextId ? (
                      <Button
                        size="sm"
                        variant="outline"
                        className="absolute right-4 top-1/2 h-7 -translate-y-1/2 px-2.5 text-[11px]"
                        onClick={() => openCreateAttributeDrawer(selectedEntityContextId)}
                        disabled={!canEditCurrentView}
                      >
                        <Plus className="h-4 w-4" />
                        {labels.actions.createAttribute}
                      </Button>
                    ) : null}
                  </CardHeader>
                  <CardContent className="flex flex-1 flex-col p-0">
                    {!selectedEntityContextId ? (
                      <div className="p-4">
                        <EmptyState icon={Eye} label="Select an entity to show its attributes." />
                      </div>
                    ) : selectedSemanticAttributes.length ? (
                      <>
                      <div className="flex-1 divide-y divide-border/70 overflow-auto">
                        {paginatedAttributes.map((attribute) => {
                          const displayAttribute = attribute.draft_snapshot || attribute;
                          const isSelected = attribute.id === selectedSemanticTypeId;
                          return (
                            <button
                              key={attribute.id}
                              type="button"
                              onClick={() => selectSemanticType(attribute)}
                              className={`block w-full px-4 py-2 text-left transition hover:bg-muted/20 ${
                                isSelected ? "bg-primary/5" : ""
                              }`}
                            >
                              <div className="flex items-start justify-between gap-3">
                                <div className="min-w-0">
                                  <div className="truncate text-[13px] font-medium">{displayAttribute.name}</div>
                                  <div className="mt-0.5 text-[11px] text-muted-foreground">
                                    {displayAttribute.aliases?.slice(0, 2).join(", ") || displayAttribute.id}
                                  </div>
                                </div>
                                <div className="flex shrink-0 flex-col items-end gap-1">
                                  {attribute.draft_snapshot ? <Badge className="px-1.5 py-0 text-[10px]" variant="warning">Draft</Badge> : null}
                                </div>
                              </div>
                            </button>
                          );
                        })}
                      </div>
                      <PaginationBar
                        currentPage={currentAttributesPage}
                        totalPages={totalAttributePages}
                        onPageChange={setCurrentAttributesPage}
                      />
                      </>
                    ) : (
                      <div className="p-4">
                        <EmptyState icon={Database} label="No attributes for this entity." />
                      </div>
                    )}
                  </CardContent>
                </Card>

                <Card className="workspace-panel flex h-[720px] flex-col rounded-2xl shadow-none">
                  <CardHeader className="border-b border-border p-4">
                    <CardTitle>{labels.sections.inspector}</CardTitle>
                  </CardHeader>
                  <CardContent className="flex-1 space-y-4 overflow-auto p-4">
                    {!selectedSemanticType ? (
                      <EmptyState icon={Eye} label={labels.status.emptySelection} />
                    ) : (
                      (() => {
                        const displaySemanticType = selectedSemanticType.draft_snapshot || selectedSemanticType;
                        return (
                          <>
                            <div className="space-y-3">
                              <div className="flex items-start justify-between gap-3">
                                <div>
                                  <div className="text-lg font-semibold">{displaySemanticType.name}</div>
                                  <div className="mt-1 text-xs text-muted-foreground">
                                    {displaySemanticType.entity_kind || "semantic"} · {displaySemanticType.datatype || "string"}
                                  </div>
                                </div>
                                <div className="flex flex-wrap gap-2">
                                  <Badge variant={selectedSemanticType.status === "approved" ? "success" : "warning"}>
                                    {selectedSemanticType.status === "approved" ? labels.status.approved : labels.status.draft}
                                  </Badge>
                                  {selectedSemanticType.draft_snapshot ? <Badge variant="warning">Draft changes</Badge> : null}
                                </div>
                              </div>
                              <div className="grid gap-2 sm:grid-cols-2">
                                <MetaPill label="Semantic ID" value={selectedSemanticType.id} icon={Fingerprint} />
                                <MetaPill label="Updated" value={formatDate(selectedSemanticType.updated_at, language)} />
                                <MetaPill label="Owners" value={(displaySemanticType.owners || []).join(", ") || "platform"} />
                                <MetaPill label="Relations" value={String(visibleRelationships.length)} icon={GitBranch} />
                                <MetaPill label="Kind" value={displaySemanticType.entity_kind || "attribute"} />
                                {displaySemanticType.parent_entity_name ? <MetaPill label="Parent" value={displaySemanticType.parent_entity_name} /> : null}
                              </div>
                            </div>

                            {displaySemanticType.description ? (
                              <div className="rounded-xl border border-border/70 bg-muted/15 p-4 text-sm leading-6">
                                {displaySemanticType.description}
                              </div>
                            ) : null}

                            <InspectorList label="Aliases" values={displaySemanticType.aliases || []} emptyLabel="No aliases" />
                            {displaySemanticType.entity_kind === "entity" ? (
                              <RelationshipList
                                relationships={visibleRelationships}
                                selectedSemanticTypeId={displaySemanticType.id}
                                onSelectRelationship={(relationship) => selectRelationship(relationship, { openDrawer: true })}
                              />
                            ) : null}
                            {selectedSemanticType.draft_snapshot ? (
                              <DraftCompareCard approved={selectedSemanticType.approved_snapshot} draft={selectedSemanticType.draft_snapshot} />
                            ) : null}

                            <div className="flex flex-wrap gap-2 border-t border-border pt-4">
                              <Button onClick={() => setOpenDrawer("semantic")} disabled={!canEditCurrentView}>
                                <PencilLine className="h-4 w-4" />
                                {labels.actions.openEditor}
                              </Button>
                              <Button
                                variant="destructive"
                                onClick={() => void deleteSemanticType()}
                                disabled={!canEditCurrentView || submitting === "delete-semantic-type"}
                              >
                                {submitting === "delete-semantic-type" ? (
                                  <LoaderCircle className="h-4 w-4 animate-spin" />
                                ) : (
                                  <Trash2 className="h-4 w-4" />
                                )}
                                {labels.actions.delete}
                              </Button>
                            </div>
                          </>
                        );
                      })()
                    )}
                  </CardContent>
                </Card>
                </div>
              </section>
            ) : null}

            {activeModule === "semantic-model" && activeTab === "graph" ? (
              <section className="space-y-4">
                <Card className="workspace-panel rounded-2xl shadow-none">
                  <CardContent className="h-[720px] p-4">
                    {loading ? (
                      <RegistryPlaceholder label={labels.status.loading} />
                    ) : semanticModelGraphTypes.length ? (
                      <GraphCanvas
                        semanticTypes={semanticModelGraphTypes}
                        relationships={semanticModelGraphRelationships}
                        selectedSemanticTypeId={selectedSemanticTypeId}
                        selectedRelationshipId={selectedRelationshipId}
                        graphViewMode={graphViewMode}
                        onSelectSemanticType={(semanticType) => selectSemanticType(semanticType, { openDrawer: true })}
                        onSelectRelationship={(relationship) => selectRelationship(relationship, { openDrawer: true })}
                        onCreateRelationship={(sourceId, targetId) => {
                          if (!canEditCurrentView) return;
                          const existingRelationship = relationships.find(
                            (item) =>
                              item.source_id === sourceId &&
                              item.target_id === targetId &&
                              item.draft_change_type !== "delete"
                          );
                          if (existingRelationship) {
                            selectRelationship(existingRelationship, { openDrawer: true });
                            return;
                          }
                          setRelationshipForm({ sourceId, targetId, relationType: "related_to" });
                          setSelectedSemanticTypeId(sourceId);
                          setSelectedRelationshipId("");
                          setOpenDrawer("create-relationship");
                        }}
                        onClearSelection={clearGraphSelection}
                      />
                    ) : (
                      <EmptyState icon={Database} label={labels.status.emptyTypes} />
                    )}
                  </CardContent>
                </Card>
              </section>
            ) : null}

            {activeModule === "execution-contracts" ? (
              <section className="flex flex-col gap-4">
                <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                  <div className="relative min-w-0 lg:w-[320px]">
                    <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                    <Input
                      className="border-border/80 bg-background pl-9"
                      placeholder="source 검색..."
                      value={sourceQuery}
                      onChange={(event) => setSourceQuery(event.target.value)}
                    />
                  </div>
                  <div className="flex gap-2">
                    {(["all", "approved", "draft"] as const).map((status) => (
                      <button
                        key={status}
                        type="button"
                        onClick={() => setStatusFilter(status)}
                        className={`rounded-full border px-2.5 py-1 text-xs transition ${
                          statusFilter === status
                            ? "border-primary/30 bg-primary text-primary-foreground"
                            : "border-border bg-background text-muted-foreground hover:bg-accent hover:text-foreground"
                        }`}
                      >
                        {labels.filters[status]}
                      </button>
                    ))}
                  </div>
                </div>

                <div className="grid gap-4 xl:grid-cols-[340px_minmax(0,1fr)]">
                  <Card className="workspace-panel flex h-[720px] flex-col rounded-2xl shadow-none">
                    <CardHeader className="relative border-b border-border p-4 pr-28">
                      <CardTitle>{labels.sections.sources}</CardTitle>
                      <Button
                        size="sm"
                        variant="outline"
                        className="absolute right-4 top-1/2 h-7 -translate-y-1/2 px-2.5 text-[11px]"
                        onClick={openCreateSourceDrawer}
                        disabled={!canEditCurrentView}
                      >
                        <Plus className="h-4 w-4" />
                        {labels.actions.createSource}
                      </Button>
                    </CardHeader>
                    <CardContent className="flex flex-1 flex-col p-0">
                      {loading ? (
                        <div className="p-4">
                          <RegistryPlaceholder label={labels.status.loading} />
                        </div>
                      ) : paginatedExecutionSources.length ? (
                        <>
                          <div className="flex-1 divide-y divide-border/70 overflow-auto">
                            {paginatedExecutionSources.map((source) => {
                              const display = source.draft_snapshot || source;
                              const config = (display.config || {}) as Record<string, unknown>;
                              const inputMode = String(config.input_mode || "document");
                              const isSelected = source.id === selectedExecutionSourceId;
                              return (
                                <button
                                  key={source.id}
                                  type="button"
                                  onClick={() => selectExecutionSource(source)}
                                  className={`block w-full px-4 py-3 text-left transition hover:bg-muted/20 ${
                                    isSelected ? "bg-primary/5" : ""
                                  }`}
                                >
                                  <div className="flex items-start justify-between gap-3">
                                    <div className="min-w-0">
                                      <div className="truncate font-medium">{display.name}</div>
                                      <div className="mt-1 text-xs text-muted-foreground">
                                        {display.source_type} · {inputMode}
                                      </div>
                                    </div>
                                    <div className="flex shrink-0 flex-col items-end gap-1">
                                      <Badge variant="info">{display.provider || "direct"}</Badge>
                                      {source.draft_snapshot ? <Badge variant="warning">Draft</Badge> : null}
                                    </div>
                                  </div>
                                </button>
                              );
                            })}
                          </div>
                          <PaginationBar
                            currentPage={currentSourcesPage}
                            totalPages={totalSourcesPages}
                            onPageChange={setCurrentSourcesPage}
                          />
                        </>
                      ) : (
                        <div className="p-4">
                          <EmptyState icon={Database} label="No execution sources yet." />
                        </div>
                      )}
                    </CardContent>
                  </Card>

                  <Card className="workspace-panel flex h-[720px] flex-col rounded-2xl shadow-none">
                    <CardHeader className="border-b border-border p-4">
                      <CardTitle>{labels.sections.inspector}</CardTitle>
                    </CardHeader>
                    <CardContent className="flex-1 space-y-4 overflow-auto p-4">
                      {!selectedExecutionSource ? (
                        <EmptyState icon={Eye} label="Select a source to inspect." />
                      ) : (
                        (() => {
                          const display = selectedExecutionSource.draft_snapshot || selectedExecutionSource;
                          const config = (display.config || {}) as Record<string, unknown>;
                          return (
                            <>
                              <div className="space-y-3">
                                <div className="flex items-start justify-between gap-3">
                                  <div>
                                    <div className="text-lg font-semibold">{display.name}</div>
                                    <div className="mt-1 text-xs text-muted-foreground">
                                      {display.source_type} · {display.provider || "direct"}
                                    </div>
                                  </div>
                                  <div className="flex flex-wrap gap-2">
                                    <Badge variant={selectedExecutionSource.status === "approved" ? "success" : "warning"}>
                                      {selectedExecutionSource.status === "approved" ? labels.status.approved : labels.status.draft}
                                    </Badge>
                                    {selectedExecutionSource.draft_snapshot ? <Badge variant="warning">Draft changes</Badge> : null}
                                  </div>
                                </div>
                                <div className="grid gap-2 sm:grid-cols-2">
                                  <MetaPill label="Source ID" value={selectedExecutionSource.id} icon={Fingerprint} />
                                  <MetaPill label="Updated" value={formatDate(selectedExecutionSource.updated_at, language)} />
                                  <MetaPill label="Type" value={display.source_type} />
                                  <MetaPill label="Input" value={String(config.input_mode || "document")} />
                                </div>
                              </div>

                              {display.description ? (
                                <div className="rounded-xl border border-border/70 bg-muted/15 p-4 text-sm leading-6">
                                  {display.description}
                                </div>
                              ) : null}

                              {String(config.reference_uri || "") ? (
                                <MetaPill label="Reference" value={String(config.reference_uri || "")} />
                              ) : null}
                              {String(config.manual_notes || "") ? (
                                <div className="rounded-xl border border-border/70 bg-muted/15 p-4 text-sm leading-6">
                                  {String(config.manual_notes || "")}
                                </div>
                              ) : null}
                              {selectedExecutionSource.draft_snapshot ? (
                                <DraftCompareCard
                                  approved={selectedExecutionSource.approved_snapshot}
                                  draft={selectedExecutionSource.draft_snapshot}
                                />
                              ) : null}

                              <div className="flex flex-wrap gap-2 border-t border-border pt-4">
                                <Button onClick={() => setOpenDrawer("source")} disabled={!canEditCurrentView}>
                                  <PencilLine className="h-4 w-4" />
                                  {labels.actions.openEditor}
                                </Button>
                                <Button
                                  variant="destructive"
                                  onClick={() => void deleteExecutionSource()}
                                  disabled={!canEditCurrentView || submitting === "delete-execution-source"}
                                >
                                  {submitting === "delete-execution-source" ? (
                                    <LoaderCircle className="h-4 w-4 animate-spin" />
                                  ) : (
                                    <Trash2 className="h-4 w-4" />
                                  )}
                                  {labels.actions.delete}
                                </Button>
                              </div>
                            </>
                          );
                        })()
                      )}
                    </CardContent>
                  </Card>
                </div>
              </section>
            ) : null}

            {activeModule === "governance" ? (
              <section className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
                <Card className="workspace-panel flex h-[720px] flex-col rounded-2xl shadow-none">
                  <CardHeader className="border-b border-border p-4">
                    <div className="flex items-center justify-between gap-3">
                      <CardTitle>{labels.sections.review}</CardTitle>
                      <Badge variant="warning">{sortedProposals.length}</Badge>
                    </div>
                  </CardHeader>
                  <CardContent className="flex flex-1 flex-col p-0">
                    {loading ? (
                      <div className="p-4">
                        <RegistryPlaceholder label={labels.status.loading} />
                      </div>
                    ) : sortedProposals.length ? (
                      <>
                        <div className="flex items-center justify-between border-b border-border px-3 py-2">
                          <label className="flex items-center gap-2 text-[12px] text-muted-foreground">
                            <input
                              type="checkbox"
                              checked={allPageProposalsSelected}
                              onChange={(event) => {
                                if (event.target.checked) {
                                  setSelectedProposalIds((current) => Array.from(new Set([...current, ...paginatedProposals.map((p) => p.id)])));
                                } else {
                                  setSelectedProposalIds((current) =>
                                    current.filter((id) => !paginatedProposals.some((proposal) => proposal.id === id))
                                  );
                                }
                              }}
                            />
                            <span>Select page</span>
                          </label>
                          <div className="flex gap-2">
                            <Button
                              size="sm"
                              className="h-8 px-2.5 text-[11px]"
                              onClick={() => void reviewSelectedProposals("approve")}
                              disabled={!selectedProposalIds.length || submitting === "bulk-approve"}
                            >
                              {submitting === "bulk-approve" ? <LoaderCircle className="h-4 w-4 animate-spin" /> : <CheckCircle2 className="h-4 w-4" />}
                              Approve all
                            </Button>
                            <Button
                              size="sm"
                              variant="outline"
                              className="h-8 px-2.5 text-[11px]"
                              onClick={() => void reviewSelectedProposals("reject")}
                              disabled={!selectedProposalIds.length || submitting === "bulk-reject"}
                            >
                              <AlertTriangle className="h-4 w-4" />
                              Reject all
                            </Button>
                          </div>
                        </div>
                        <div className="flex-1 space-y-1 overflow-auto p-3">
                      {paginatedProposals.map((proposal) => {
                        const isSelected = proposal.id === selectedProposalId;
                        const isChecked = selectedProposalIds.includes(proposal.id);
                        return (
                          <div
                            key={proposal.id}
                            className={`w-full rounded-xl border px-2.5 py-2 transition ${
                              isSelected
                                ? "border-primary/50 bg-primary/5"
                                : "border-border/80 bg-muted/15 hover:bg-muted/25"
                            }`}
                          >
                            <div className="flex items-start gap-3">
                              <input
                                type="checkbox"
                                checked={isChecked}
                                onChange={(event) => {
                                  setSelectedProposalIds((current) =>
                                    event.target.checked ? [...current, proposal.id] : current.filter((id) => id !== proposal.id)
                                  );
                                }}
                                onClick={(event) => event.stopPropagation()}
                                className="mt-0.5"
                              />
                              <button
                                type="button"
                                onClick={() => setSelectedProposalId(proposal.id)}
                                className="block min-w-0 flex-1 text-left"
                              >
                                <div className="flex items-center gap-2">
                                  <Badge className="px-1.5 py-0 text-[10px]" variant="warning">{labels.status.pending}</Badge>
                                  <Badge className="px-1.5 py-0 text-[10px]" variant="default">{proposal.entity_type}</Badge>
                                  <Badge className="px-1.5 py-0 text-[10px]" variant="info">{proposal.change_type}</Badge>
                                </div>
                                <div className="mt-1 flex items-center justify-between gap-3">
                                  <div className="min-w-0 truncate text-[12px] font-medium leading-4">{proposal.title}</div>
                                  <div className="shrink-0 text-[10px] text-muted-foreground">{formatDate(proposal.created_at, language)}</div>
                                </div>
                              </button>
                            </div>
                          </div>
                        );
                      })
                        }
                        </div>
                        <PaginationBar
                          currentPage={currentProposalsPage}
                          totalPages={totalProposalPages}
                          onPageChange={setCurrentProposalsPage}
                        />
                      </>
                    ) : (
                      <div className="p-4">
                        <EmptyState icon={FileText} label={labels.status.emptyProposals} />
                      </div>
                    )}
                  </CardContent>
                </Card>

                <Card className="workspace-panel flex h-[720px] flex-col rounded-2xl shadow-none">
                  <CardHeader className="border-b border-border p-4">
                    <CardTitle>{labels.sections.inspector}</CardTitle>
                  </CardHeader>
                  <CardContent className="flex-1 space-y-3 overflow-auto p-4">
                    {!selectedProposal ? (
                      <EmptyState icon={Eye} label={labels.status.emptyReviewSelection} />
                    ) : (
                      <>
                        <div className="flex items-start justify-between gap-3">
                          <div>
                            <div className="text-base font-semibold">{selectedProposal.title}</div>
                            <div className="mt-1 text-xs text-muted-foreground">
                              {selectedProposal.entity_type} · {selectedProposal.change_type}
                            </div>
                          </div>
                          <Badge variant="warning">{labels.status.pending}</Badge>
                        </div>

                        <div className="grid gap-2 sm:grid-cols-2">
                          <MetaPill label="Proposal ID" value={selectedProposal.id} icon={Fingerprint} />
                          <MetaPill label="Created" value={formatDate(selectedProposal.created_at, language)} />
                          <MetaPill label="Entity" value={selectedProposal.entity_id || "-"} />
                          <MetaPill label="Status" value={selectedProposal.status} />
                        </div>

                        <div className="rounded-xl border border-border/80 bg-muted/15 p-4">
                          <div className="mb-2 text-xs uppercase tracking-[0.14em] text-muted-foreground">Change Diff</div>
                          <ProposalDiff proposal={selectedProposal} />
                        </div>

                        <div className="flex gap-2 border-t border-border pt-4">
                          <Button
                            size="sm"
                            onClick={() => void reviewProposal(selectedProposal.id, "approve")}
                            disabled={submitting === selectedProposal.id}
                          >
                            {submitting === selectedProposal.id ? (
                              <LoaderCircle className="h-4 w-4 animate-spin" />
                            ) : (
                              <CheckCircle2 className="h-4 w-4" />
                            )}
                            {labels.actions.approve}
                          </Button>
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() => void reviewProposal(selectedProposal.id, "reject")}
                            disabled={submitting === selectedProposal.id}
                          >
                            <AlertTriangle className="h-4 w-4" />
                            {labels.actions.reject}
                          </Button>
                        </div>
                      </>
                    )}
                  </CardContent>
                </Card>
              </section>
            ) : null}
          </main>
        </div>
      </div>

      <DrawerShell
        open={openDrawer === "create"}
        title={labels.drawer.create}
        onClose={() => setOpenDrawer(null)}
        closeLabel={labels.actions.close}
      >
        <form className="space-y-4" onSubmit={submitCreateSemanticType}>
          <SemanticTypeFields
            labels={labels}
            form={createSemanticTypeForm}
            setForm={setCreateSemanticTypeForm}
            entityOptions={entitySemanticTypes}
          />
          <div className="flex flex-wrap gap-2 border-t border-border pt-4">
            <Button type="submit" disabled={submitting === "create-semantic-type"}>
              {submitting === "create-semantic-type" ? (
                <LoaderCircle className="h-4 w-4 animate-spin" />
              ) : (
                <Sparkles className="h-4 w-4" />
              )}
              {labels.actions.createSubmit}
            </Button>
          </div>
        </form>
      </DrawerShell>

      <DrawerShell
        open={openDrawer === "semantic" && !!selectedSemanticType}
        title={selectedSemanticType?.name || labels.drawer.semantic}
        onClose={() => setOpenDrawer(null)}
        closeLabel={labels.actions.close}
      >
        {selectedSemanticType ? (
          <form className="space-y-4" onSubmit={submitSemanticType}>
            <div className="rounded-xl border border-border/80 bg-muted/20 p-4">
              <div className="grid gap-2 lg:grid-cols-2">
                <MetaPill label="Semantic ID" value={selectedSemanticType.id} icon={Fingerprint} />
                <MetaPill label="Status" value={selectedSemanticType.status || "draft"} />
              </div>
            </div>
            <SemanticTypeFields
              labels={labels}
              form={semanticTypeForm}
              setForm={setSemanticTypeForm}
              entityOptions={entitySemanticTypes.filter((item) => item.id !== selectedSemanticType.id)}
            />
            <div className="flex flex-wrap gap-2 border-t border-border pt-4">
              <Button type="submit" disabled={submitting === "semantic-type"}>
                {submitting === "semantic-type" ? (
                  <LoaderCircle className="h-4 w-4 animate-spin" />
                ) : (
                  <PencilLine className="h-4 w-4" />
                )}
                {labels.actions.save}
              </Button>
              <Button
                type="button"
                variant="destructive"
                onClick={() => void deleteSemanticType()}
                disabled={submitting === "delete-semantic-type"}
              >
                {submitting === "delete-semantic-type" ? (
                  <LoaderCircle className="h-4 w-4 animate-spin" />
                ) : (
                  <Trash2 className="h-4 w-4" />
                )}
                {labels.actions.delete}
              </Button>
            </div>
          </form>
        ) : null}
      </DrawerShell>

      <DrawerShell
        open={openDrawer === "create-source"}
        title={labels.drawer.createSource}
        onClose={() => setOpenDrawer(null)}
        closeLabel={labels.actions.close}
      >
        <form className="space-y-4" onSubmit={submitCreateExecutionSource}>
          <ExecutionSourceFields
            labels={labels}
            form={createExecutionSourceForm}
            setForm={setCreateExecutionSourceForm}
          />
          <div className="flex flex-wrap gap-2 border-t border-border pt-4">
            <Button type="submit" disabled={submitting === "create-execution-source"}>
              {submitting === "create-execution-source" ? (
                <LoaderCircle className="h-4 w-4 animate-spin" />
              ) : (
                <Sparkles className="h-4 w-4" />
              )}
              {labels.actions.createSource}
            </Button>
          </div>
        </form>
      </DrawerShell>

      <DrawerShell
        open={openDrawer === "create-relationship"}
        title={
          relationshipForm.sourceId && relationshipForm.targetId
            ? `${semanticNameById(entitySemanticTypes, relationshipForm.sourceId)} -> ${semanticNameById(entitySemanticTypes, relationshipForm.targetId)}`
            : labels.drawer.createRelationship
        }
        onClose={() => setOpenDrawer(null)}
        closeLabel={labels.actions.close}
      >
        <RelationshipCreateForm
          labels={labels}
          semanticTypes={entitySemanticTypes}
          form={relationshipForm}
          setForm={setRelationshipForm}
          submitting={submitting}
          onSubmit={submitRelationship}
        />
      </DrawerShell>

      <DrawerShell
        open={openDrawer === "relationship" && !!selectedRelationship}
        title={selectedRelationship?.relation_type || labels.drawer.relationship}
        onClose={() => setOpenDrawer(null)}
        closeLabel={labels.actions.close}
      >
        {selectedRelationship ? (
          <form className="space-y-4" onSubmit={submitRelationshipEdit}>
            <div className="rounded-xl border border-border/80 bg-muted/20 p-4">
              <div className="grid gap-2 lg:grid-cols-2">
                <MetaPill label="Relationship ID" value={selectedRelationship.id} icon={GitBranch} />
                <MetaPill label="Status" value={selectedRelationship.status || "draft"} />
              </div>
            </div>
            {selectedRelationship.draft_snapshot ? (
              <RelationshipCompareCard
                approved={selectedRelationship.approved_snapshot}
                draft={selectedRelationship.draft_snapshot}
              />
            ) : null}
            <div className="grid gap-4 md:grid-cols-2">
              <label className="space-y-2 text-sm">
                <span>{labels.fields.source}</span>
                <select
                  className="h-10 w-full rounded-lg border border-border/80 bg-muted/50 px-3 text-sm outline-none focus:border-primary/70 focus:ring-2 focus:ring-primary/20"
                  value={relationshipEditForm.sourceId}
                  onChange={(event) => setRelationshipEditForm((current) => ({ ...current, sourceId: event.target.value }))}
                >
                  {entitySemanticTypes.map((semanticType) => (
                    <option key={semanticType.id} value={semanticType.id}>
                      {semanticType.name}
                    </option>
                  ))}
                </select>
              </label>
              <label className="space-y-2 text-sm">
                <span>{labels.fields.target}</span>
                <select
                  className="h-10 w-full rounded-lg border border-border/80 bg-muted/50 px-3 text-sm outline-none focus:border-primary/70 focus:ring-2 focus:ring-primary/20"
                  value={relationshipEditForm.targetId}
                  onChange={(event) => setRelationshipEditForm((current) => ({ ...current, targetId: event.target.value }))}
                >
                  {entitySemanticTypes.map((semanticType) => (
                    <option key={semanticType.id} value={semanticType.id}>
                      {semanticType.name}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            <label className="space-y-2 text-sm">
              <span>{labels.fields.relation}</span>
              <Input
                value={relationshipEditForm.relationType}
                onChange={(event) => setRelationshipEditForm((current) => ({ ...current, relationType: event.target.value }))}
              />
            </label>
            <div className="flex flex-wrap gap-2 border-t border-border pt-4">
              <Button type="submit" disabled={submitting === "relationship-edit"}>
                {submitting === "relationship-edit" ? (
                  <LoaderCircle className="h-4 w-4 animate-spin" />
                ) : (
                  <GitBranch className="h-4 w-4" />
                )}
                {labels.actions.updateRelation}
              </Button>
              <Button
                type="button"
                variant="destructive"
                onClick={() => void deleteRelationship()}
                disabled={submitting === "delete-relationship"}
              >
                {submitting === "delete-relationship" ? (
                  <LoaderCircle className="h-4 w-4 animate-spin" />
                ) : (
                  <Trash2 className="h-4 w-4" />
                )}
                {labels.actions.deleteRelation}
              </Button>
            </div>
          </form>
        ) : null}
      </DrawerShell>

      <DrawerShell
        open={openDrawer === "source" && !!selectedExecutionSource}
        title={selectedExecutionSource?.name || labels.drawer.source}
        onClose={() => setOpenDrawer(null)}
        closeLabel={labels.actions.close}
      >
        {selectedExecutionSource ? (
          <form className="space-y-4" onSubmit={submitExecutionSource}>
            <div className="rounded-xl border border-border/80 bg-muted/20 p-4">
              <div className="grid gap-2 lg:grid-cols-2">
                <MetaPill label="Source ID" value={selectedExecutionSource.id} icon={Fingerprint} />
                <MetaPill label="Status" value={selectedExecutionSource.status || "draft"} />
              </div>
            </div>
            {selectedExecutionSource.draft_snapshot ? (
              <DraftCompareCard
                approved={selectedExecutionSource.approved_snapshot}
                draft={selectedExecutionSource.draft_snapshot}
              />
            ) : null}
            <ExecutionSourceFields labels={labels} form={executionSourceForm} setForm={setExecutionSourceForm} />
            <div className="flex flex-wrap gap-2 border-t border-border pt-4">
              <Button type="submit" disabled={submitting === "execution-source"}>
                {submitting === "execution-source" ? (
                  <LoaderCircle className="h-4 w-4 animate-spin" />
                ) : (
                  <PencilLine className="h-4 w-4" />
                )}
                {labels.actions.save}
              </Button>
              <Button
                type="button"
                variant="destructive"
                onClick={() => void deleteExecutionSource()}
                disabled={submitting === "delete-execution-source"}
              >
                {submitting === "delete-execution-source" ? (
                  <LoaderCircle className="h-4 w-4 animate-spin" />
                ) : (
                  <Trash2 className="h-4 w-4" />
                )}
                {labels.actions.delete}
              </Button>
            </div>
          </form>
        ) : null}
      </DrawerShell>

      {toast ? (
        <div className="pointer-events-none fixed bottom-5 right-5 z-50">
          <div
            className={`pointer-events-auto flex min-w-[320px] max-w-[420px] items-start gap-3 rounded-2xl border px-4 py-3 shadow-[0_20px_50px_rgba(15,23,42,0.18)] backdrop-blur ${
              toast.tone === "success"
                ? "border-emerald-500/20 bg-gradient-to-br from-emerald-500/18 to-background text-emerald-950 dark:from-emerald-500/16 dark:to-card dark:text-emerald-100"
                : "border-red-500/20 bg-gradient-to-br from-red-500/18 to-background text-red-950 dark:from-red-500/16 dark:to-card dark:text-red-100"
            }`}
          >
            <div
              className={`mt-0.5 inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full ${
                toast.tone === "success" ? "bg-emerald-500/15" : "bg-red-500/15"
              }`}
            >
              {toast.tone === "success" ? (
                <CheckCircle2 className="h-4 w-4" />
              ) : (
                <AlertTriangle className="h-4 w-4" />
              )}
            </div>
            <div className="min-w-0 flex-1">
              <div className="text-sm font-semibold">{toast.tone === "success" ? "Updated" : "Attention"}</div>
              <div className="mt-0.5 text-sm">{toast.text}</div>
            </div>
            <button
              type="button"
              className="rounded-lg px-2 py-1 text-xs text-muted-foreground transition hover:bg-black/5 hover:text-foreground dark:hover:bg-white/5"
              onClick={() => setToast(null)}
            >
              Close
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function SemanticTypeFields({
  labels,
  form,
  setForm,
  entityOptions
}: {
  labels: (typeof text)["ko"];
  form: SemanticTypeForm;
  setForm: React.Dispatch<React.SetStateAction<SemanticTypeForm>>;
  entityOptions: SemanticType[];
}) {
  return (
    <>
      <div className="grid gap-4 md:grid-cols-2">
        <label className="space-y-2 text-sm">
          <span>{labels.fields.name}</span>
          <Input
            required
            placeholder="ContractAmount"
            value={form.name}
            onChange={(event) => setForm((current) => ({ ...current, name: event.target.value }))}
          />
        </label>
        <label className="space-y-2 text-sm">
          <span>{labels.fields.datatype}</span>
          <select
            className="h-10 w-full rounded-lg border border-border/80 bg-muted/50 px-3 text-sm outline-none focus:border-primary/70 focus:ring-2 focus:ring-primary/20"
            value={form.datatype}
            onChange={(event) => setForm((current) => ({ ...current, datatype: event.target.value }))}
          >
            {["string", "number", "integer", "boolean", "date", "datetime", "object", "array"].map((datatype) => (
              <option key={datatype} value={datatype}>
                {datatype}
              </option>
            ))}
          </select>
        </label>
      </div>
      <div className="grid gap-4 md:grid-cols-2">
        <label className="space-y-2 text-sm">
          <span>{labels.fields.kind}</span>
          <select
            className="h-10 w-full rounded-lg border border-border/80 bg-muted/50 px-3 text-sm outline-none focus:border-primary/70 focus:ring-2 focus:ring-primary/20"
            value={form.entityKind}
            onChange={(event) =>
              setForm((current) => ({
                ...current,
                entityKind: event.target.value,
                parentEntityId: event.target.value === "attribute" ? current.parentEntityId : ""
              }))
            }
          >
            {["entity", "attribute"].map((kind) => (
              <option key={kind} value={kind}>
                {kind}
              </option>
            ))}
          </select>
        </label>
        {form.entityKind === "attribute" ? (
          <label className="space-y-2 text-sm">
            <span>{labels.fields.parentEntity}</span>
            <select
              className="h-10 w-full rounded-lg border border-border/80 bg-muted/50 px-3 text-sm outline-none focus:border-primary/70 focus:ring-2 focus:ring-primary/20"
              value={form.parentEntityId}
              onChange={(event) => setForm((current) => ({ ...current, parentEntityId: event.target.value }))}
            >
              <option value="">Select entity</option>
              {entityOptions.map((entity) => (
                <option key={entity.id} value={entity.id}>
                  {entity.name}
                </option>
              ))}
            </select>
          </label>
        ) : (
          <div />
        )}
      </div>
      <label className="space-y-2 text-sm">
        <span>{labels.fields.description}</span>
        <textarea
          className="min-h-28 w-full rounded-lg border border-border/80 bg-muted/50 px-3 py-2 text-sm outline-none focus:border-primary/70 focus:ring-2 focus:ring-primary/20"
          value={form.description}
          onChange={(event) => setForm((current) => ({ ...current, description: event.target.value }))}
        />
      </label>
      <div className="grid gap-4 md:grid-cols-2">
        <label className="space-y-2 text-sm">
          <span>{labels.fields.aliases}</span>
          <Input
            placeholder="contract_amount, cntrct_amt"
            value={form.aliases}
            onChange={(event) => setForm((current) => ({ ...current, aliases: event.target.value }))}
          />
        </label>
        <label className="space-y-2 text-sm">
          <span>{labels.fields.owners}</span>
          <Input
            value={form.owners}
            onChange={(event) => setForm((current) => ({ ...current, owners: event.target.value }))}
          />
        </label>
      </div>
    </>
  );
}

function RelationshipCreateForm({
  labels,
  semanticTypes,
  form,
  setForm,
  submitting,
  onSubmit
}: {
  labels: (typeof text)["ko"];
  semanticTypes: SemanticType[];
  form: RelationshipForm;
  setForm: React.Dispatch<React.SetStateAction<RelationshipForm>>;
  submitting: string;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
}) {
  const sourceName = semanticNameById(semanticTypes, form.sourceId);
  const targetName = semanticNameById(semanticTypes, form.targetId);
  return (
    <form className="space-y-3" onSubmit={onSubmit}>
      {form.sourceId && form.targetId ? (
        <div className="rounded-xl border border-border/80 bg-muted/20 px-3 py-3">
          <div className="text-[11px] uppercase tracking-[0.14em] text-muted-foreground">Connection</div>
          <div className="mt-1 text-sm font-medium">
            {sourceName} <span className="text-muted-foreground">→</span> {targetName}
          </div>
        </div>
      ) : null}
      <label className="space-y-2 text-sm">
        <span>{labels.fields.source}</span>
        <select
          className="h-10 w-full rounded-lg border border-border/80 bg-muted/50 px-3 text-sm outline-none focus:border-primary/70 focus:ring-2 focus:ring-primary/20"
          value={form.sourceId}
          onChange={(event) => setForm((current) => ({ ...current, sourceId: event.target.value }))}
        >
          <option value="">Select source</option>
          {semanticTypes.map((semanticType) => (
            <option key={semanticType.id} value={semanticType.id}>
              {semanticType.name}
            </option>
          ))}
        </select>
      </label>
      <label className="space-y-2 text-sm">
        <span>{labels.fields.target}</span>
        <select
          className="h-10 w-full rounded-lg border border-border/80 bg-muted/50 px-3 text-sm outline-none focus:border-primary/70 focus:ring-2 focus:ring-primary/20"
          value={form.targetId}
          onChange={(event) => setForm((current) => ({ ...current, targetId: event.target.value }))}
        >
          <option value="">Select target</option>
          {semanticTypes.map((semanticType) => (
            <option key={semanticType.id} value={semanticType.id}>
              {semanticType.name}
            </option>
          ))}
        </select>
      </label>
      <label className="space-y-2 text-sm">
        <span>{labels.fields.relation}</span>
        <Input
          value={form.relationType}
          onChange={(event) => setForm((current) => ({ ...current, relationType: event.target.value }))}
        />
      </label>
      <Button type="submit" className="w-full" disabled={submitting === "relationship"}>
        <GitBranch className="h-4 w-4" />
        {labels.actions.createRelation}
      </Button>
    </form>
  );
}

function ExecutionSourceFields({
  labels,
  form,
  setForm
}: {
  labels: (typeof text)["ko"];
  form: ExecutionSourceForm;
  setForm: React.Dispatch<React.SetStateAction<ExecutionSourceForm>>;
}) {
  return (
    <>
      <div className="grid gap-4 md:grid-cols-2">
        <label className="space-y-2 text-sm">
          <span>{labels.fields.name}</span>
          <Input
            required
            placeholder="PpsApi"
            value={form.name}
            onChange={(event) => setForm((current) => ({ ...current, name: event.target.value }))}
          />
        </label>
        <label className="space-y-2 text-sm">
          <span>{labels.fields.provider}</span>
          <Input
            placeholder="pps"
            value={form.provider}
            onChange={(event) => setForm((current) => ({ ...current, provider: event.target.value }))}
          />
        </label>
      </div>
      <div className="grid gap-4 md:grid-cols-2">
        <label className="space-y-2 text-sm">
          <span>{labels.fields.sourceType}</span>
          <select
            className="h-10 w-full rounded-lg border border-border/80 bg-muted/50 px-3 text-sm outline-none focus:border-primary/70 focus:ring-2 focus:ring-primary/20"
            value={form.sourceType}
            onChange={(event) => setForm((current) => ({ ...current, sourceType: event.target.value }))}
          >
            {["api", "table", "file", "stream", "queue", "other"].map((sourceType) => (
              <option key={sourceType} value={sourceType}>
                {sourceType}
              </option>
            ))}
          </select>
        </label>
        <label className="space-y-2 text-sm">
          <span>{labels.fields.inputMode}</span>
          <select
            className="h-10 w-full rounded-lg border border-border/80 bg-muted/50 px-3 text-sm outline-none focus:border-primary/70 focus:ring-2 focus:ring-primary/20"
            value={form.inputMode}
            onChange={(event) => setForm((current) => ({ ...current, inputMode: event.target.value }))}
          >
            {["document", "manual"].map((inputMode) => (
              <option key={inputMode} value={inputMode}>
                {inputMode}
              </option>
            ))}
          </select>
        </label>
      </div>
      <label className="space-y-2 text-sm">
        <span>{labels.fields.description}</span>
        <textarea
          className="min-h-24 w-full rounded-lg border border-border/80 bg-muted/50 px-3 py-2 text-sm outline-none focus:border-primary/70 focus:ring-2 focus:ring-primary/20"
          value={form.description}
          onChange={(event) => setForm((current) => ({ ...current, description: event.target.value }))}
        />
      </label>
      <label className="space-y-2 text-sm">
        <span>{labels.fields.referenceUri}</span>
        <Input
          placeholder="https://provider/docs or schema.table"
          value={form.referenceUri}
          onChange={(event) => setForm((current) => ({ ...current, referenceUri: event.target.value }))}
        />
      </label>
      {form.inputMode === "manual" ? (
        <label className="space-y-2 text-sm">
          <span>{labels.fields.manualNotes}</span>
          <textarea
            className="min-h-28 w-full rounded-lg border border-border/80 bg-muted/50 px-3 py-2 text-sm outline-none focus:border-primary/70 focus:ring-2 focus:ring-primary/20"
            value={form.manualNotes}
            onChange={(event) => setForm((current) => ({ ...current, manualNotes: event.target.value }))}
          />
        </label>
      ) : null}
    </>
  );
}

function GraphCanvas({
  semanticTypes,
  relationships,
  selectedSemanticTypeId,
  selectedRelationshipId,
  graphViewMode,
  onSelectSemanticType,
  onSelectRelationship,
  onCreateRelationship,
  onClearSelection
}: {
  semanticTypes: SemanticType[];
  relationships: SemanticRelationship[];
  selectedSemanticTypeId: string;
  selectedRelationshipId: string;
  graphViewMode: "approved" | "draft";
  onSelectSemanticType: (semanticType: SemanticType) => void;
  onSelectRelationship: (relationship: SemanticRelationship) => void;
  onCreateRelationship: (sourceId: string, targetId: string) => void;
  onClearSelection: () => void;
}) {
  const [expandedEntityIds, setExpandedEntityIds] = useState<string[]>([]);
  const [nodes, setNodes] = useState<Node[]>([]);
  const [connectStart, setConnectStart] = useState<{ nodeId: string; handleType: HandleType | null } | null>(null);
  const nodeTypes = useMemo(() => ({ semanticNode: SemanticGraphNode }), []);
  const edgeTypes = useMemo(() => ({ semanticEdge: SemanticGraphEdge }), []);
  const attributeMap = useMemo(() => {
    const map = new Map<string, SemanticType[]>();
    semanticTypes.forEach((item) => {
      const display = graphViewMode === "draft" && item.draft_snapshot ? item.draft_snapshot : item;
      if (display.entity_kind === "attribute" && display.parent_entity_id) {
        map.set(display.parent_entity_id, [...(map.get(display.parent_entity_id) || []), item]);
      }
    });
    return map;
  }, [graphViewMode, semanticTypes]);
  useEffect(() => {
    const entityTypes = semanticTypes.filter((item) => (item.draft_snapshot || item).entity_kind === "entity");
    const computedNodes: Node[] = [];
    let currentY = 60;

    entityTypes.forEach((semanticType, index) => {
      const displaySemanticType =
        graphViewMode === "draft" && semanticType.draft_snapshot ? semanticType.draft_snapshot : semanticType;
      const attributes = attributeMap.get(semanticType.id) || [];
      const expanded = expandedEntityIds.includes(semanticType.id);
      const extraRows = expanded ? Math.max(0, attributes.length) : 0;
      const defaultPosition = { x: 120 + (index % 2) * 340, y: currentY };
      const existingPosition = nodes.find((item) => item.id === semanticType.id)?.position;
      const position = existingPosition || defaultPosition;
      computedNodes.push({
        id: semanticType.id,
        type: "semanticNode",
        position,
        dragHandle: ".erd-drag-handle",
        data: {
          label: displaySemanticType.name,
          kind: displaySemanticType.entity_kind || "attribute",
          attributes: attributes.map((attribute) => {
            const displayAttribute =
              graphViewMode === "draft" && attribute.draft_snapshot ? attribute.draft_snapshot : attribute;
            return displayAttribute.name;
          }),
          expanded,
          hasDraft: Boolean(semanticType.draft_snapshot),
          selected: semanticType.id === selectedSemanticTypeId,
          onToggle: () =>
            setExpandedEntityIds((current) =>
              current.includes(semanticType.id)
                ? current.filter((item) => item !== semanticType.id)
                : [...current, semanticType.id]
            )
        }
      });
      if (index % 2 === 1) {
        currentY += 150 + extraRows * 28;
      }
    });

    setNodes(computedNodes);
  }, [attributeMap, expandedEntityIds, graphViewMode, semanticTypes, selectedSemanticTypeId]);

  const edges = useMemo<Edge[]>(() => {
    const nodePositionMap = new Map(nodes.map((node) => [node.id, node.position]));
    const relationshipEdges = relationships.map((relationship) => {
      const sourcePosition = nodePositionMap.get(relationship.source_id);
      const targetPosition = nodePositionMap.get(relationship.target_id);
      const sourceX = sourcePosition?.x || 0;
      const sourceY = sourcePosition?.y || 0;
      const targetX = targetPosition?.x || 0;
      const targetY = targetPosition?.y || 0;
      const deltaX = targetX - sourceX;
      const deltaY = targetY - sourceY;
      const horizontalDominant = Math.abs(deltaX) >= Math.abs(deltaY);
      const sourceHandle = horizontalDominant
        ? deltaX >= 0
          ? "relation-source-right"
          : "relation-source-left"
        : deltaY >= 0
          ? "relation-source-bottom"
          : "relation-source-top";
      const targetHandle = horizontalDominant
        ? deltaX >= 0
          ? "relation-target-left"
          : "relation-target-right"
        : deltaY >= 0
          ? "relation-target-top"
          : "relation-target-bottom";
      return ({
      id: relationship.id,
      source: relationship.source_id,
      target: relationship.target_id,
      type: "semanticEdge",
      data: {
        label:
          graphViewMode === "draft" && relationship.draft_snapshot
            ? relationship.draft_snapshot.relation_type
            : relationship.relation_type,
        selected: relationship.id === selectedRelationshipId,
        hasDraft: Boolean(relationship.draft_snapshot),
        draftChangeType: relationship.draft_change_type || "",
        graphViewMode,
        showLabel: true
      },
      sourceHandle,
      targetHandle,
      markerEnd: {
        type: MarkerType.ArrowClosed,
        width: 16,
        height: 16,
        color:
          graphViewMode === "draft" && relationship.draft_change_type === "delete"
            ? "rgba(220,38,38,0.92)"
            : relationship.id === selectedRelationshipId
            ? "rgb(37 99 235)"
            : graphViewMode === "draft" && relationship.draft_snapshot
              ? "rgba(245,158,11,0.9)"
              : "rgba(100,116,139,0.72)"
      },
      style: {
        stroke:
          graphViewMode === "draft" && relationship.draft_change_type === "delete"
            ? "rgba(220,38,38,0.92)"
            : relationship.id === selectedRelationshipId
            ? "rgb(37 99 235)"
            : graphViewMode === "draft" && relationship.draft_snapshot
              ? "rgba(245,158,11,0.9)"
              : "rgba(100,116,139,0.58)",
        strokeWidth: relationship.id === selectedRelationshipId ? 2.1 : 1.35,
        strokeDasharray:
          graphViewMode === "draft" && relationship.draft_change_type === "delete" ? "6 4" : undefined
      }
      });
    });

    return relationshipEdges;
  }, [graphViewMode, nodes, relationships, selectedRelationshipId]);

  function handleNodesChange(changes: NodeChange<Node>[]) {
    setNodes((current) => applyNodeChanges(changes, current));
  }

  return (
    <div className="h-full overflow-hidden rounded-xl border border-border/80 bg-background">
      <div className="h-full min-h-0 w-full">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={handleNodesChange}
          onConnectStart={(_, params) => {
            setConnectStart({
              nodeId: params.nodeId || "",
              handleType: params.handleType || null
            });
          }}
          onConnectEnd={() => {
            setConnectStart(null);
          }}
          onConnect={(connection: Connection) => {
            if (connection.source && connection.target && connection.source !== connection.target) {
              const startedFromTarget = connectStart?.handleType === "target";
              const sourceId = startedFromTarget ? connection.target : connection.source;
              const targetId = startedFromTarget ? connection.source : connection.target;
              if (sourceId && targetId && sourceId !== targetId) {
                onCreateRelationship(sourceId, targetId);
              }
            }
            setConnectStart(null);
          }}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          fitView
          fitViewOptions={{ padding: 0.28, maxZoom: 0.78 }}
          minZoom={0.2}
          maxZoom={1.8}
          panOnDrag
          selectionOnDrag={false}
          nodesDraggable
          onPaneClick={onClearSelection}
          onNodeClick={(_, node) => {
            const semanticType = semanticTypes.find((item) => item.id === node.id);
            if (semanticType) {
              onSelectSemanticType(semanticType);
            }
          }}
          onEdgeClick={(_, edge) => {
            const relationship = relationships.find((item) => item.id === edge.id);
            if (relationship) {
              onSelectRelationship(relationship);
            }
          }}
          proOptions={{ hideAttribution: true }}
        >
          <Background gap={18} size={1} color="#dbe4ee" />
          <Controls showInteractive={false} />
        </ReactFlow>
      </div>
    </div>
  );
}

function SemanticGraphNode({
  data
}: NodeProps<
  Node<{
    label: string;
    kind: string;
    attributes: string[];
    expanded: boolean;
    hasDraft: boolean;
    selected: boolean;
    onToggle: () => void;
  }>
>) {
  return (
    <div
      className={`min-w-[260px] rounded-[18px] border bg-card text-left shadow-[0_16px_36px_rgba(15,23,42,0.08)] transition ${
        data.selected
          ? "border-blue-500 shadow-[0_18px_44px_rgba(37,99,235,0.18)]"
          : data.hasDraft
            ? "border-amber-300"
            : "border-border"
      }`}
    >
      <Handle
        id="relation-target-left"
        type="target"
        position={Position.Left}
        className="!h-2.5 !w-2.5 !border-2 !border-background !bg-border"
      />
      <Handle
        id="relation-source-right"
        type="source"
        position={Position.Right}
        className="!h-2.5 !w-2.5 !border-2 !border-background !bg-border"
      />
      <Handle
        id="relation-target-right"
        type="target"
        position={Position.Right}
        className="!h-2.5 !w-2.5 !border-2 !border-background !bg-border !opacity-0"
      />
      <Handle
        id="relation-source-left"
        type="source"
        position={Position.Left}
        className="!h-2.5 !w-2.5 !border-2 !border-background !bg-border !opacity-0"
      />
      <Handle
        id="relation-target-top"
        type="target"
        position={Position.Top}
        className="!h-2.5 !w-2.5 !border-2 !border-background !bg-border !opacity-0"
      />
      <Handle
        id="relation-source-top"
        type="source"
        position={Position.Top}
        className="!h-2.5 !w-2.5 !border-2 !border-background !bg-border !opacity-0"
      />
      <Handle
        id="relation-target-bottom"
        type="target"
        position={Position.Bottom}
        className="!h-2.5 !w-2.5 !border-2 !border-background !bg-border !opacity-0"
      />
      <Handle
        id="relation-source-bottom"
        type="source"
        position={Position.Bottom}
        className="!h-2.5 !w-2.5 !border-2 !border-background !bg-border !opacity-0"
      />
      <div className="border-b border-border/80 px-4 py-2.5">
        <div className="erd-drag-handle flex cursor-grab items-center justify-between gap-3 active:cursor-grabbing">
          <div className="truncate text-[13px] font-semibold text-foreground">{data.label}</div>
          <div className="flex items-center gap-1.5">
            {data.hasDraft ? (
              <span className="rounded-full bg-amber-100 px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-[0.12em] text-amber-700 dark:bg-amber-500/15 dark:text-amber-200">
                D
              </span>
            ) : null}
            <button
              type="button"
              className="rounded-md border border-border/80 bg-muted/30 p-1 text-muted-foreground transition hover:bg-accent hover:text-foreground"
              onClick={(event) => {
                event.stopPropagation();
                data.onToggle();
              }}
            >
              {data.expanded ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
            </button>
          </div>
        </div>
      </div>
      {data.expanded ? (
        <div className="px-4 py-2">
          <div className="space-y-1.5">
            {data.attributes.length ? (
              data.attributes.map((attribute) => (
                <div
                  key={attribute}
                  className="truncate rounded-md border border-border/60 bg-muted/20 px-2.5 py-1.5 text-[11px] text-foreground/90"
                >
                  {attribute}
                </div>
              ))
            ) : (
              <div className="text-[11px] text-muted-foreground">No attributes</div>
            )}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function SemanticGraphEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  markerEnd,
  style,
  data
}: EdgeProps<
  Edge<{
    label: string;
    selected: boolean;
    hasDraft: boolean;
    draftChangeType?: string;
    graphViewMode: "approved" | "draft";
    showLabel?: boolean;
  }>
>) {
  const [edgePath, labelX, labelY] = getSmoothStepPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    borderRadius: 12,
    offset: 12
  });
  const tone = data?.draftChangeType === "delete"
    ? "text-red-950 border-red-300 dark:text-red-100 dark:border-red-500/40"
    : data?.selected
    ? "text-blue-950 border-blue-300 dark:text-blue-100 dark:border-blue-500/40"
    : data?.hasDraft && data?.graphViewMode === "draft"
      ? "text-amber-950 border-amber-300 dark:text-amber-100 dark:border-amber-500/40"
      : "text-slate-700 border-slate-300 dark:text-slate-100 dark:border-slate-500/40";
  return (
    <>
      <BaseEdge id={id} path={edgePath} markerEnd={markerEnd} style={style} />
      {data?.showLabel && data?.label ? (
        <EdgeLabelRenderer>
          <div
            className={`pointer-events-none absolute z-10 rounded-full border bg-background px-3 py-1 text-[12px] font-semibold shadow-lg ring-4 ring-background dark:bg-card dark:ring-card ${tone}`}
            style={{
              transform: `translate(-50%, -50%) translate(${labelX + 2}px, ${labelY - 2}px)`
            }}
          >
            {data?.label}
            {data?.draftChangeType === "delete" ? " · pending delete" : ""}
          </div>
        </EdgeLabelRenderer>
      ) : null}
    </>
  );
}

function DrawerShell({
  open,
  title,
  onClose,
  closeLabel,
  children
}: {
  open: boolean;
  title: string;
  onClose: () => void;
  closeLabel: string;
  children: React.ReactNode;
}) {
  if (!open) {
    return null;
  }
  return (
    <div className="fixed inset-0 z-40 flex justify-end">
      <button type="button" aria-label={closeLabel} className="flex-1 cursor-default bg-transparent" onClick={onClose} />
      <section className="relative h-full w-full max-w-lg overflow-y-auto border-l border-border bg-background shadow-2xl">
        <div className="sticky top-0 z-10 border-b border-border bg-background/95 px-4 py-3 backdrop-blur">
          <div className="flex items-start justify-between gap-4">
            <h2 className="text-lg font-semibold">{title}</h2>
            <Button variant="ghost" size="sm" onClick={onClose}>
              {closeLabel}
            </Button>
          </div>
        </div>
        <div className="p-4">{children}</div>
      </section>
    </div>
  );
}

function InlineStat({ label, value }: { label: string; value: number }) {
  return (
    <span className="rounded-full border border-border/80 bg-card px-2 py-1 text-[11px] font-medium text-muted-foreground">
      {label} {value}
    </span>
  );
}

function PaginationBar({
  currentPage,
  totalPages,
  onPageChange
}: {
  currentPage: number;
  totalPages: number;
  onPageChange: (page: number) => void;
}) {
  const pages = buildPaginationPages(currentPage, totalPages);
  return (
    <div className="mt-auto flex items-center justify-center gap-1 border-t border-border px-3 py-2.5">
      <button
        type="button"
        onClick={() => onPageChange(Math.max(1, currentPage - 1))}
        disabled={currentPage === 1}
        className="h-7 min-w-7 rounded-lg border border-border bg-background px-1.5 text-xs text-muted-foreground transition hover:text-foreground disabled:opacity-40"
      >
        ←
      </button>
      {pages.map((page, index) =>
        page === "…" ? (
          <span key={`ellipsis-${index}`} className="px-1 text-xs text-muted-foreground">
            …
          </span>
        ) : (
          <button
            key={page}
            type="button"
            onClick={() => onPageChange(page)}
            className={`h-7 min-w-7 rounded-lg border px-1.5 text-xs transition ${
              currentPage === page
                ? "border-primary/30 bg-primary text-primary-foreground"
                : "border-border bg-background text-muted-foreground hover:bg-accent hover:text-foreground"
            }`}
          >
            {page}
          </button>
        )
      )}
      <button
        type="button"
        onClick={() => onPageChange(Math.min(totalPages, currentPage + 1))}
        disabled={currentPage === totalPages}
        className="h-7 min-w-7 rounded-lg border border-border bg-background px-1.5 text-xs text-muted-foreground transition hover:text-foreground disabled:opacity-40"
      >
        →
      </button>
    </div>
  );
}

function MetaPill({
  label,
  value,
  icon: Icon
}: {
  label: string;
  value: string;
  icon?: typeof Database;
}) {
  return (
    <div className="rounded-lg border border-border/70 bg-background px-3 py-2">
      <div className="flex items-center gap-1.5 text-[11px] uppercase tracking-[0.14em] text-muted-foreground">
        {Icon ? <Icon className="h-3.5 w-3.5" /> : null}
        <span>{label}</span>
      </div>
      <div className="mt-1 truncate text-sm font-medium text-foreground">{value}</div>
    </div>
  );
}

function InspectorList({
  label,
  values,
  emptyLabel
}: {
  label: string;
  values: string[];
  emptyLabel: string;
}) {
  return (
    <div className="rounded-xl border border-border/70 bg-muted/15 p-4">
      <div className="mb-3 text-xs uppercase tracking-[0.14em] text-muted-foreground">{label}</div>
      {values.length ? (
        <div className="flex flex-wrap gap-2">
          {values.map((value) => (
            <Badge key={value} variant="default">
              {value}
            </Badge>
          ))}
        </div>
      ) : (
        <div className="text-sm text-muted-foreground">{emptyLabel}</div>
      )}
    </div>
  );
}

function AttributeList({
  attributes,
  onSelectAttribute,
  onCreateAttribute,
  createLabel
}: {
  attributes: SemanticType[];
  onSelectAttribute: (attribute: SemanticType) => void;
  onCreateAttribute: () => void;
  createLabel: string;
}) {
  return (
    <div className="rounded-xl border border-border/70 bg-muted/15 p-4">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div className="text-xs uppercase tracking-[0.14em] text-muted-foreground">Attributes</div>
        <Button size="sm" variant="outline" onClick={onCreateAttribute}>
          <Plus className="h-4 w-4" />
          {createLabel}
        </Button>
      </div>
      {attributes.length ? (
        <div className="space-y-2">
          {attributes.map((attribute) => {
            const display = attribute.draft_snapshot || attribute;
            return (
              <button
                key={attribute.id}
                type="button"
                onClick={() => onSelectAttribute(attribute)}
                className="block w-full rounded-lg border border-border/70 bg-background px-3 py-2 text-left text-sm transition hover:bg-muted/30"
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="font-medium">{display.name}</div>
                  <Badge variant="info">{display.datatype || "string"}</Badge>
                </div>
                <div className="mt-1 text-xs text-muted-foreground">
                  {attribute.draft_snapshot ? "Draft changes" : display.id}
                </div>
              </button>
            );
          })}
        </div>
      ) : (
        <div className="text-sm text-muted-foreground">No attributes</div>
      )}
    </div>
  );
}

function RelationshipList({
  relationships,
  selectedSemanticTypeId,
  onSelectRelationship
}: {
  relationships: SemanticRelationship[];
  selectedSemanticTypeId: string;
  onSelectRelationship: (relationship: SemanticRelationship) => void;
}) {
  return (
    <div className="rounded-xl border border-border/70 bg-muted/15 p-4">
      <div className="mb-3 text-xs uppercase tracking-[0.14em] text-muted-foreground">Linked Relationships</div>
      {relationships.length ? (
        <div className="space-y-2">
          {relationships.map((relationship) => {
            const direction =
              relationship.source_id === selectedSemanticTypeId
                ? `${relationship.relation_type} → ${relationship.target_name}`
                : `${relationship.source_name} → ${relationship.relation_type}`;
            return (
              <button
                key={relationship.id}
                type="button"
                onClick={() => onSelectRelationship(relationship)}
                className="block w-full rounded-lg border border-border/70 bg-background px-3 py-2 text-left text-sm transition hover:bg-muted/30"
              >
                <div className="font-medium">{direction}</div>
                <div className="mt-1 text-xs text-muted-foreground">{relationship.id}</div>
              </button>
            );
          })}
        </div>
      ) : (
        <div className="text-sm text-muted-foreground">No linked relationships</div>
      )}
    </div>
  );
}

function DraftCompareCard({
  approved,
  draft
}: {
  approved?: SemanticType | null;
  draft?: SemanticType | null;
}) {
  if (!draft) {
    return null;
  }
  return (
    <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-4">
      <div className="mb-3 flex items-center gap-2">
        <Badge variant="warning">Draft changes</Badge>
        <div className="text-xs text-muted-foreground">Approved and draft are shown together until review.</div>
      </div>
      <div className="grid gap-4 md:grid-cols-2">
        <SnapshotCard title="Approved" semanticType={approved} />
        <SnapshotCard title="Draft" semanticType={draft} />
      </div>
    </div>
  );
}

function SnapshotCard({
  title,
  semanticType
}: {
  title: string;
  semanticType?: SemanticType | null;
}) {
  return (
    <div className="rounded-lg border border-border/70 bg-background px-3 py-3">
      <div className="mb-2 text-xs uppercase tracking-[0.14em] text-muted-foreground">{title}</div>
      {semanticType ? (
        <div className="space-y-2 text-sm">
          <div className="font-medium">{semanticType.name}</div>
          <div className="text-muted-foreground">
            {semanticType.entity_kind || "attribute"} · {semanticType.datatype || "string"}
          </div>
          <div>{semanticType.description || "-"}</div>
        </div>
      ) : (
        <div className="text-sm text-muted-foreground">No approved snapshot</div>
      )}
    </div>
  );
}

function RelationshipCompareCard({
  approved,
  draft
}: {
  approved?: SemanticRelationship | null;
  draft?: SemanticRelationship | null;
}) {
  if (!draft) {
    return null;
  }
  return (
    <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-4">
      <div className="mb-3 flex items-center gap-2">
        <Badge variant="warning">Draft changes</Badge>
        <div className="text-xs text-muted-foreground">Approved and draft are shown together until review.</div>
      </div>
      <div className="grid gap-4 md:grid-cols-2">
        <RelationshipSnapshotCard title="Approved" relationship={approved} />
        <RelationshipSnapshotCard title="Draft" relationship={draft} />
      </div>
    </div>
  );
}

function RelationshipSnapshotCard({
  title,
  relationship
}: {
  title: string;
  relationship?: SemanticRelationship | null;
}) {
  return (
    <div className="rounded-lg border border-border/70 bg-background px-3 py-3">
      <div className="mb-2 text-xs uppercase tracking-[0.14em] text-muted-foreground">{title}</div>
      {relationship ? (
        <div className="space-y-2 text-sm">
          <div className="font-medium">
            {relationship.source_name} <span className="text-muted-foreground">{relationship.relation_type}</span>{" "}
            {relationship.target_name}
          </div>
          <div className="text-muted-foreground">{relationship.id}</div>
        </div>
      ) : (
        <div className="text-sm text-muted-foreground">No approved snapshot</div>
      )}
    </div>
  );
}

function semanticNameById(items: SemanticType[], id: string) {
  const match = items.find((item) => item.id === id);
  const display = match?.draft_snapshot || match;
  return display?.name || id || "-";
}

function ProposalDiff({ proposal }: { proposal: Proposal }) {
  const payload = (proposal.payload || {}) as Record<string, unknown>;
  const approved = payload.approved_snapshot;
  const draft = payload.draft_snapshot;
  if (approved && draft) {
    return (
      <div className="grid gap-4 md:grid-cols-2">
        <DiffCard title="Approved" value={approved} />
        <DiffCard title="Draft" value={draft} />
      </div>
    );
  }
  return <DiffCard title="Payload" value={payload} />;
}

function DiffCard({ title, value }: { title: string; value: unknown }) {
  return (
    <div className="rounded-lg border border-border/70 bg-background px-3 py-3">
      <div className="mb-2 text-xs uppercase tracking-[0.14em] text-muted-foreground">{title}</div>
      <pre className="overflow-x-auto text-xs leading-6 text-foreground">{JSON.stringify(value, null, 2)}</pre>
    </div>
  );
}

function RegistryPlaceholder({ label }: { label: string }) {
  return (
    <div className="flex items-center gap-2 rounded-lg border border-dashed border-border px-4 py-6 text-sm text-muted-foreground">
      <LoaderCircle className="h-4 w-4 animate-spin" />
      {label}
    </div>
  );
}

function EmptyState({ icon: Icon, label }: { icon: typeof Database; label: string }) {
  return (
    <div className="flex items-center gap-2 rounded-lg border border-dashed border-border px-4 py-6 text-sm text-muted-foreground">
      <Icon className="h-4 w-4" />
      {label}
    </div>
  );
}

function buildPaginationPages(currentPage: number, totalPages: number): Array<number | "…"> {
  if (totalPages <= 7) {
    return Array.from({ length: totalPages }, (_, index) => index + 1);
  }
  if (currentPage <= 3) {
    return [1, 2, 3, 4, "…", totalPages];
  }
  if (currentPage >= totalPages - 2) {
    return [1, "…", totalPages - 3, totalPages - 2, totalPages - 1, totalPages];
  }
  return [1, "…", currentPage - 1, currentPage, currentPage + 1, "…", totalPages];
}

function commaList(value: string) {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function executionSourcePayloadFromForm(form: ExecutionSourceForm) {
  return {
    name: form.name.trim(),
    provider: form.provider.trim(),
    source_type: form.sourceType,
    description: form.description.trim(),
    config: {
      input_mode: form.inputMode,
      reference_uri: form.referenceUri.trim(),
      manual_notes: form.manualNotes.trim()
    }
  };
}

function formatDate(value: string | null | undefined, language: Language) {
  if (!value) {
    return "-";
  }
  return new Date(value).toLocaleString(language === "ko" ? "ko-KR" : "en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit"
  });
}

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers || {})
    }
  });
  if (!response.ok) {
    const detail = await readErrorDetail(response);
    throw new Error(detail || `Request failed: ${response.status}`);
  }
  return (await response.json()) as T;
}

async function readErrorDetail(response: Response) {
  try {
    const payload = (await response.json()) as { detail?: string };
    return payload.detail || "";
  } catch {
    return "";
  }
}
