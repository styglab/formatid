"use client";

import type { FormEvent } from "react";
import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import { ArrowLeft, RefreshCw, Sparkles, CheckCircle2, PlayCircle } from "lucide-react";
import {
  cancelOnboardingRun,
  completeOnboardingTask,
  createCanonicalAttribute,
  createCanonicalEntity,
  createMapping,
  createSemanticType,
  generateOnboardingTaskDraft,
  getOnboardingRun,
  pauseOnboardingRun,
  resumeOnboardingRun,
  reviewProposal,
  suggestOperationFieldMapping,
  type MappingSuggestion,
  type TransformSuggestion,
} from "@/api/semantic-admin";
import { InspectorPanel } from "@/components/layout/inspector-panel";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { TablePanel } from "@/components/layout/table-panel";
import { ActionToast } from "@/components/semantic/common/action-toast";
import { ErrorModal } from "@/components/semantic/common/error-modal";
import { MappingForm, mappingFormDefaults, type MappingFormState } from "@/components/semantic/forms/mapping-form";
import { ProposalInspector } from "@/components/semantic/inspector/proposal-inspector";
import { EmptyPanel, ErrorPanel, LoadingPanel } from "@/components/semantic/common/state-panel";
import { MetaCard, MetricCard } from "@/components/semantic/common/meta-card";
import { useCanonicalModel } from "@/hooks/semantic/use-canonical-model";
import { useSemanticRegistry } from "@/hooks/semantic/use-proposals";
import { parseJsonObject, stringifyJson } from "@/lib/semantic/forms";
import {
  STAGE_LABELS_FULL,
  stageState,
  type WorkspaceStage,
} from "@/lib/semantic/onboarding";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import type { OperationField, OnboardingRunDetail } from "@/types/semantic";

type WorkbenchStep = "evidence" | "semantic_model" | "binding" | "bundle_publish";

const WORKBENCH_STEPS: WorkbenchStep[] = ["evidence", "semantic_model", "binding", "bundle_publish"];

const WORKBENCH_STEP_LABELS: Record<WorkbenchStep, string> = {
  evidence: "Evidence",
  semantic_model: "Semantic Model",
  binding: "Binding",
  bundle_publish: "Changes & Publish",
};

const STEP_TO_STAGE: Record<WorkbenchStep, WorkspaceStage> = {
  evidence: "source_review",
  semantic_model: "semantic_model_approval",
  binding: "binding_approval",
  bundle_publish: "proposal_review",
};

function stepForStage(stage?: WorkspaceStage): WorkbenchStep {
  if (!stage || stage === "source_review" || stage === "asset_discovery" || stage === "structure_review") return "evidence";
  if (stage === "semantic_model_drafting" || stage === "semantic_model_approval") return "semantic_model";
  if (stage === "binding_drafting" || stage === "binding_approval") return "binding";
  return "bundle_publish";
}

function badgeVariantForDependency(status?: string) {
  if (status === "ready") return "success" as const;
  if (status === "needs_rebase") return "warning" as const;
  return "danger" as const;
}

function laneClass(isFocused: boolean) {
  return `rounded-2xl border p-4 ${isFocused ? "border-primary/60 bg-primary/[0.05]" : "border-border/70 bg-background/70"}`;
}

function proposalContextFromPayload(payload?: Record<string, unknown>) {
  if (!payload || typeof payload !== "object") return null;
  const context = payload.proposal_context;
  if (!context || typeof context !== "object") return null;
  return context as Record<string, unknown>;
}

function semanticEvidenceClustersFromDraft(payload?: Record<string, unknown>) {
  const clusters = payload?.semantic_evidence_clusters;
  return Array.isArray(clusters) ? clusters.filter((item) => typeof item === "object" && item !== null) as Array<Record<string, unknown>> : [];
}

type RunTab = "review" | "source" | "proposals" | "assets" | "operations" | "schemas" | "work_queue" | "bundle";

const TAB_LABELS: Record<RunTab, string> = {
  review: "Review",
  source: "Source Context",
  proposals: "Proposals",
  assets: "Assets & Access Paths",
  operations: "Operations & Bindings",
  schemas: "Structures & Fields",
  work_queue: "Review Tasks",
  bundle: "Proposal Bundle",
};

const TAB_ORDER: RunTab[] = ["review", "source"];

function stepperCircleClass(state: "completed" | "current" | "upcoming" | "blocked") {
  if (state === "completed") return "border-emerald-500 bg-emerald-500 text-white";
  if (state === "current") return "border-primary bg-primary text-primary-foreground";
  if (state === "blocked") return "border-amber-500 bg-amber-500 text-white";
  return "border-border bg-background text-muted-foreground";
}

function stepperLineClass(state: "completed" | "current" | "upcoming" | "blocked") {
  if (state === "completed") return "bg-emerald-500";
  if (state === "current") return "bg-primary/40";
  if (state === "blocked") return "bg-amber-500";
  return "bg-border";
}

export default function OnboardingRunDetailPage() {
  const router = useRouter();
  const params = useParams<{ runId: string }>();
  const runId = Array.isArray(params?.runId) ? params.runId[0] : params?.runId || "";
  const [detail, setDetail] = useState<OnboardingRunDetail | null>(null);
  const [activeTab, setActiveTab] = useState<RunTab>("review");
  const [focusedStage, setFocusedStage] = useState<WorkspaceStage>("source_review");
  const [selectedKey, setSelectedKey] = useState("");
  const [selectedAssetId, setSelectedAssetId] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [createOpen, setCreateOpen] = useState(false);
  const [createOperationFieldId, setCreateOperationFieldId] = useState("");
  const [form, setForm] = useState<MappingFormState>(mappingFormDefaults);
  const [submitting, setSubmitting] = useState(false);
  const [reviewing, setReviewing] = useState("");
  const [actionError, setActionError] = useState("");
  const [actionMessage, setActionMessage] = useState("");
  const [actionLinkProposalIds, setActionLinkProposalIds] = useState<string[]>([]);
  const [mappingSuggestion, setMappingSuggestion] = useState<MappingSuggestion | null>(null);
  const [suggestionLoading, setSuggestionLoading] = useState(false);
  const [suggestionError, setSuggestionError] = useState("");
  const { semanticTypes, reload: reloadSemanticRegistry } = useSemanticRegistry();
  const { entities: canonicalEntities, attributes: canonicalAttributes, reload: reloadCanonicalModel } = useCanonicalModel();

  async function reload() {
    if (!runId) return;
    setLoading(true);
    setError("");
    try {
      const next = await getOnboardingRun(runId);
      setDetail(next);
      setSelectedKey("");
      setFocusedStage((next.run.current_stage || "source_review") as WorkspaceStage);
      const firstAssetId = next.operations.find((item) => item.asset_id)?.asset_id || "";
      setSelectedAssetId((current) => current || firstAssetId);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load onboarding run detail.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, [runId]);

  const selectedOperation = detail?.operations.find((item) => item.id === selectedKey) || detail?.operations[0] || null;
  const runAssets = useMemo(
    () =>
      Array.from(
        new Map(
          (detail?.operations || [])
            .filter((item) => item.asset_id)
            .map((item) => [
              item.asset_id as string,
              {
                id: item.asset_id as string,
                name: item.asset_name || item.asset_id || "Asset",
                asset_type: item.asset_type || "",
              },
            ])
        ).values()
      ),
    [detail?.operations]
  );
  const selectedAsset = runAssets.find((item) => item.id === selectedAssetId) || runAssets[0] || null;
  const filteredRunOperations = useMemo(
    () => (selectedAsset ? (detail?.operations || []).filter((item) => item.asset_id === selectedAsset.id) : detail?.operations || []),
    [detail?.operations, selectedAsset]
  );
  const filteredRunFields = useMemo(() => {
    const operationIds = new Set(filteredRunOperations.map((item) => item.id));
    return (detail?.fields || []).filter((item) => operationIds.has(item.operation_id));
  }, [detail?.fields, filteredRunOperations]);
  const selectedField = filteredRunFields.find((item) => item.id === selectedKey) || filteredRunFields[0] || null;
  const selectedTask = detail?.work_queue_tasks.find((item) => item.id === selectedKey) || detail?.work_queue_tasks[0] || null;
  const selectedProposal = detail?.proposals.find((item) => item.id === selectedKey) || detail?.proposals[0] || null;
  const evidence = detail?.evidence_snapshots[0] || null;
  const isPreparing = detail?.run.preparation_status === "preparing" || detail?.run.preparation_status === "blocked";
  const currentStage = focusedStage;
  const currentWorkbenchStep = stepForStage(currentStage);
  const currentStageTasks = useMemo(
    () => (detail?.work_queue_tasks || []).filter((item) => (item.stage || "source_review") === currentStage),
    [detail?.work_queue_tasks, currentStage]
  );
  const semanticDraftTask = detail?.work_queue_tasks.find((item) => item.stage === "semantic_model_drafting") || null;
  const semanticApprovalTask = detail?.work_queue_tasks.find((item) => item.stage === "semantic_model_approval") || null;
  const bindingDraftTask = detail?.work_queue_tasks.find((item) => item.stage === "binding_drafting") || null;
  const bindingApprovalTask = detail?.work_queue_tasks.find((item) => item.stage === "binding_approval") || null;
  const unmappedFields = useMemo(
    () =>
      filteredRunFields.filter(
        (item) => !detail?.mappings.find((mapping) => mapping.field_id === item.id || mapping.field_path === item.field_path)
      ),
    [detail?.mappings, filteredRunFields]
  );
  const meaningProposals = useMemo(
    () => (detail?.proposals || []).filter((item) => item.entity_type === "semantic_type"),
    [detail?.proposals]
  );
  const canonicalProposals = useMemo(
    () => (detail?.proposals || []).filter((item) => item.entity_type === "canonical_entity" || item.entity_type === "canonical_attribute" || item.entity_type === "canonical_relation"),
    [detail?.proposals]
  );
  const bindingProposals = useMemo(
    () => (detail?.proposals || []).filter((item) => ["field_mapping", "operation_variant", "capability_binding", "control_semantics"].includes(item.entity_type)),
    [detail?.proposals]
  );
  const bindingDependencies = useMemo(
    () =>
      bindingProposals.map((item) => {
        const context = proposalContextFromPayload(item.payload);
        return {
          id: item.id,
          title: item.title,
          dependency_status: String(context?.dependency_status || "blocked"),
          resolution_basis: String(context?.resolution_basis || "missing"),
          depends_on_count: Array.isArray(context?.depends_on_proposal_ids) ? context.depends_on_proposal_ids.length : 0,
        };
      }),
    [bindingProposals]
  );
  const registryGapCount = useMemo(() => {
    const blockedBindings = bindingDependencies.filter((item) => item.dependency_status !== "ready").length;
    return blockedBindings + unmappedFields.length;
  }, [bindingDependencies, unmappedFields.length]);
  const approvedSemanticTypeCount = useMemo(
    () => semanticTypes.filter((item) => item.status === "approved").length,
    [semanticTypes]
  );
  const approvedCanonicalAttributeCount = useMemo(
    () => canonicalAttributes.filter((item) => item.status === "approved").length,
    [canonicalAttributes]
  );
  const canonicalBlockedByMeaning = approvedSemanticTypeCount === 0;
  const bindingBlockedByMeaning = approvedSemanticTypeCount === 0;
  const bindingBlockedByCanonical = !bindingBlockedByMeaning && approvedCanonicalAttributeCount === 0;
  const createFormValidationError = !createOperationFieldId
    ? "Select one source field."
    : !form.semanticTypeId
      ? "Select one semantic type before creating the mapping."
      : "";

  async function generateMappingSuggestion(fieldId = createOperationFieldId, semanticTypeId = form.semanticTypeId, applyTopCandidate = false) {
    if (!fieldId) return;
    setSuggestionLoading(true);
    setSuggestionError("");
    try {
      const suggestion = await suggestOperationFieldMapping(fieldId, { semantic_type_id: semanticTypeId || null });
      setMappingSuggestion(suggestion);
      const selectedSemanticTypeId = semanticTypeId || (applyTopCandidate ? suggestion.semantic_type_suggestions[0]?.semantic_type_id || "" : "");
      const transformSuggestion = suggestion.transform_suggestion;
      setForm((current) => ({
        ...current,
        semanticTypeId: selectedSemanticTypeId || current.semanticTypeId,
        mappingType: transformSuggestion?.mapping_type || current.mappingType,
        mappingKind: transformSuggestion?.mapping_kind || current.mappingKind,
        transformSpec: transformSuggestion ? stringifyJson(transformSuggestion.transform_spec || {}) : current.transformSpec,
        enumMapping: transformSuggestion ? stringifyJson(transformSuggestion.enum_mapping || {}) : current.enumMapping,
      }));
    } catch (requestError) {
      setSuggestionError(requestError instanceof Error ? requestError.message : "Failed to generate mapping suggestion.");
    } finally {
      setSuggestionLoading(false);
    }
  }

  function applySemanticTypeSuggestion(semanticTypeId: string) {
    setForm((current) => ({ ...current, semanticTypeId }));
    void generateMappingSuggestion(createOperationFieldId, semanticTypeId, false);
  }

  async function openCreateMapping(field: OperationField) {
    if (!detail) return;
    setCreateOperationFieldId(field.id);
    setMappingSuggestion(null);
    setSuggestionError("");
    setActionError("");
    setActionMessage("");
    setForm({
      ...mappingFormDefaults,
      operationFieldId: field.id,
      sourceId: detail.source.id,
      operationId: field.operation_id,
      fieldPath: String(field.field_path || field.raw_name),
      mappingType: "exact",
      mappingKind: field.scope === "control" ? "control" : "direct",
      transformSpec: "{}",
      enumMapping: "{}",
    });
    setCreateOpen(true);
    await generateMappingSuggestion(field.id, "", true);
  }

  async function handleCreateSemanticType(payload: { name: string; description: string; datatype: string }) {
    const response = await createSemanticType({
      name: payload.name.trim(),
      description: payload.description.trim(),
      datatype: payload.datatype.trim() || "string",
      entity_kind: "attribute",
      status: "draft",
    });
    await reloadSemanticRegistry();
    setActionMessage("Semantic type proposal created.");
    return response.semantic_type.id;
  }

  async function handleCreateCanonicalEntity(payload: { name: string; description: string; semanticTypeId: string }) {
    const response = await createCanonicalEntity({
      name: payload.name.trim(),
      semantic_type_id: payload.semanticTypeId || null,
      description: payload.description.trim(),
      status: "draft",
    });
    await reloadCanonicalModel();
    setActionMessage("Canonical entity proposal created.");
    return response.canonical_entity.id;
  }

  async function handleCreateCanonicalAttribute(payload: {
    entityId: string;
    name: string;
    description: string;
    datatype: string;
    semanticTypeId: string;
    identityRole: string;
  }) {
    const response = await createCanonicalAttribute({
      entity_id: payload.entityId,
      semantic_type_id: payload.semanticTypeId || null,
      name: payload.name.trim(),
      datatype: payload.datatype.trim() || "string",
      description: payload.description.trim(),
      identity_role: payload.identityRole.trim(),
      status: "draft",
    });
    await reloadCanonicalModel();
    setActionMessage("Canonical attribute proposal created.");
    return response.canonical_attribute.id;
  }

  async function submitCreateMapping(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const selectedFieldRecord = detail?.fields.find((item) => item.id === createOperationFieldId);
    if (!selectedFieldRecord) {
      setActionError("Select one source field.");
      return;
    }
    if (!form.semanticTypeId) {
      setActionError("Select one semantic type.");
      return;
    }
    const mappingExists = detail?.mappings.some((item) => item.field_id === selectedFieldRecord.id || (item.operation_id === selectedFieldRecord.operation_id && item.field_path === String(selectedFieldRecord.field_path || selectedFieldRecord.raw_name)));
    if (mappingExists) {
      setActionError("A mapping for the selected source field already exists.");
      return;
    }
    setSubmitting(true);
    setActionError("");
    setActionMessage("");
    setActionLinkProposalIds([]);
    try {
      const result = await createMapping({
        field_id: selectedFieldRecord.id,
        source_id: detail?.source.id || null,
        operation_id: selectedFieldRecord.operation_id,
        field_path: String(selectedFieldRecord.field_path || selectedFieldRecord.raw_name),
        semantic_type_id: form.semanticTypeId,
        mapping_type: form.mappingType,
        mapping_kind: form.mappingKind,
        namespace: "public",
        lifecycle: "draft",
        version: "1.0.0",
        transform_spec: parseJsonObject(form.transformSpec || "{}", "transform_spec"),
        enum_mapping: parseJsonObject(form.enumMapping || "{}", "enum_mapping"),
        notes: form.notes.trim(),
      });
      await reload();
      setCreateOpen(false);
      setCreateOperationFieldId("");
      setMappingSuggestion(null);
      setActionLinkProposalIds(result.proposal?.id ? [result.proposal.id] : []);
      setActionMessage("Mapping proposal created.");
      setActiveTab("review");
      setFocusedStage("proposal_review");
      setSelectedKey(result.proposal?.id || "");
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to create mapping.");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleReview(proposalId: string, decision: "approve" | "reject") {
    setReviewing(`${proposalId}:${decision}`);
    setActionError("");
    try {
      await reviewProposal(proposalId, decision);
      await reload();
      setActionMessage(decision === "approve" ? "Proposal approved." : "Proposal rejected.");
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to review proposal.");
    } finally {
      setReviewing("");
    }
  }

  async function handleGenerateTaskDraft(taskId: string) {
    setActionError("");
    setActionMessage("");
    try {
      await generateOnboardingTaskDraft(taskId);
      await reload();
      setActionMessage("AI task draft generated.");
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to generate task draft.");
    }
  }

  async function handleCompleteTask(taskId: string) {
    setActionError("");
    setActionMessage("");
    try {
      await completeOnboardingTask(taskId);
      await reload();
      setActionMessage("Approval applied and workspace moved to the next step.");
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to complete task.");
    }
  }

  async function handleResumeRun() {
    if (!detail) return;
    setActionError("");
    setActionMessage("");
    try {
      await resumeOnboardingRun(detail.run.id);
      await reload();
      setActionMessage("Workspace resumed.");
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to resume workspace.");
    }
  }

  async function handlePauseRun() {
    if (!detail) return;
    setActionError("");
    setActionMessage("");
    try {
      await pauseOnboardingRun(detail.run.id);
      await reload();
      setActionMessage("Workspace paused.");
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to pause workspace.");
    }
  }

  async function handleCancelRun() {
    if (!detail) return;
    const confirmed = typeof window === "undefined" ? true : window.confirm("Cancel this workspace? Drafts and review history will be kept, but this workspace will stop progressing.");
    if (!confirmed) return;
    setActionError("");
    setActionMessage("");
    try {
      await cancelOnboardingRun(detail.run.id);
      setActionMessage("Workspace cancelled.");
      router.push(`/sources/${detail.run.source_id}`);
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to cancel workspace.");
    }
  }

  function focusStage(stage: WorkspaceStage) {
    if (!detail) return;
    setFocusedStage(stage);
    setActiveTab("review");
    if (stage === "source_review") {
      setSelectedKey("");
      return;
    }
    if (stage === "asset_discovery") {
      const nextAssetId = runAssets[0]?.id || "";
      setSelectedAssetId(nextAssetId);
      setSelectedKey("");
      return;
    }
    if (stage === "structure_review") {
      const nextField = filteredRunFields[0] || detail.fields[0];
      setSelectedKey(nextField?.id || "");
      return;
    }
    if (stage === "semantic_model_drafting" || stage === "semantic_model_approval" || stage === "binding_drafting" || stage === "binding_approval") {
      const nextField = unmappedFields[0] || filteredRunFields[0];
      setSelectedKey(nextField?.id || "");
      return;
    }
    if (stage === "proposal_review" || stage === "publish_readiness") {
      setSelectedKey(detail.proposals[0]?.id || "");
      return;
    }
    const matchingTask = detail.work_queue_tasks.find((task) => task.stage === stage) || detail.work_queue_tasks[0];
    setSelectedKey(matchingTask?.id || "");
  }

  function handleWorkbenchStepClick(step: WorkbenchStep) {
    if (!detail) return;
    const current = (detail.run.current_stage || "source_review") as WorkspaceStage;
    if (step === "evidence") {
      if (current === "source_review" || current === "asset_discovery" || current === "structure_review") {
        focusStage(current);
        return;
      }
      focusStage("source_review");
      return;
    }
    if (step === "semantic_model") {
      if (current === "semantic_model_drafting" || current === "semantic_model_approval") {
        focusStage(current);
        return;
      }
      focusStage("semantic_model_approval");
      return;
    }
    if (step === "binding") {
      if (current === "binding_drafting" || current === "binding_approval") {
        focusStage(current);
        return;
      }
      focusStage("binding_approval");
      return;
    }
    if (current === "proposal_review" || current === "publish_readiness") {
      focusStage(current);
      return;
    }
    focusStage("proposal_review");
  }

  return (
    <SectionPlaceholder
      title={detail?.run.source_name || "Workspace"}
      description="Move through the onboarding workspace one stage at a time. Review the current stage first, then inspect supporting assets, structures, operations, tasks, and proposals in context."
      actions={
        <div className="flex flex-wrap items-center gap-2">
          <Button type="button" variant="ghost" asChild>
            <Link href="/onboarding-runs">
              <ArrowLeft className="h-4 w-4" />
              Back
            </Link>
          </Button>
          <Button type="button" variant="outline" onClick={() => void reload()} disabled={loading}>
            <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
          <Button type="button" variant="outline" onClick={() => void handlePauseRun()} disabled={loading || !detail || detail.run.status === "cancelled"}>
            Pause
          </Button>
          <Button type="button" variant="outline" onClick={() => void handleCancelRun()} disabled={loading || !detail || detail.run.status === "cancelled"}>
            Cancel Workspace
          </Button>
        </div>
      }
      body={
        <div className="space-y-5">
          {error ? <ErrorPanel message={error} /> : null}
          {loading ? <LoadingPanel message="Loading onboarding run detail..." /> : null}
          {!loading && !error && !detail ? <EmptyPanel message="Onboarding run not found." /> : null}
          {!loading && !error && detail ? (
            <>
              <div className="rounded-xl border border-border/70 bg-background/70 p-4">
                <div className="mb-3 text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Workflow</div>
                <div className="flex items-start gap-2 overflow-x-auto pb-1">
                  {WORKBENCH_STEPS.map((step, index) => {
                    const stage = STEP_TO_STAGE[step];
                    const activeStep = stepForStage((detail.run.current_stage || "source_review") as WorkspaceStage);
                    const state = stageState(stage, STEP_TO_STAGE[activeStep], detail.run.stage_status);
                    return (
                      <div key={step} className="flex min-w-[120px] flex-1 items-center gap-2">
                        <button type="button" className="flex min-w-0 flex-1 items-center gap-2 text-left" onClick={() => handleWorkbenchStepClick(step)}>
                          <div className={`flex h-7 w-7 shrink-0 items-center justify-center rounded-full border text-xs font-semibold ${stepperCircleClass(state)}`}>
                            {state === "completed" ? "✓" : index + 1}
                          </div>
                          <div className="min-w-0 flex-1">
                            <div className={`text-xs font-semibold ${step === currentWorkbenchStep ? "text-foreground" : "text-muted-foreground"}`}>{WORKBENCH_STEP_LABELS[step]}</div>
                            <div className="mt-1 h-0.5 w-full rounded-full bg-border">
                              <div className={`h-0.5 rounded-full ${stepperLineClass(state)} ${state === "current" ? "w-2/3" : state === "completed" || state === "blocked" ? "w-full" : "w-0"}`} />
                            </div>
                          </div>
                        </button>
                        {index < WORKBENCH_STEPS.length - 1 ? <div className="hidden w-2 shrink-0 md:block" /> : null}
                      </div>
                    );
                  })}
                </div>
              </div>

              {isPreparing ? (
                <div className="grid gap-5 xl:grid-cols-[minmax(0,1.15fr)_360px]">
                  <TablePanel>
                    <div className="space-y-4 rounded-2xl border border-border/70 bg-background/70 p-5">
                      <div>
                        <div className="text-sm font-semibold text-foreground">
                          {detail.run.preparation_status === "blocked" ? "Draft preparation blocked" : "Preparing AI drafts"}
                        </div>
                        <div className="mt-1 text-sm text-muted-foreground">
                          {detail.run.worker_current_task || "Generating AI drafts for workspace tasks."}
                        </div>
                      </div>
                      <div>
                        <div className="flex items-center justify-between text-[11px] text-muted-foreground">
                          <span>{detail.run.draft_ready_count || 0} ready · {detail.run.draft_active_count || 0} drafting</span>
                          <span>{detail.run.worker_progress_percent ?? 0}%</span>
                        </div>
                        <div className="mt-2 h-2 overflow-hidden rounded-full bg-muted">
                          <div
                            className={`h-full rounded-full ${detail.run.preparation_status === "blocked" ? "bg-amber-500" : "bg-primary"}`}
                            style={{ width: `${detail.run.worker_progress_percent ?? 0}%` }}
                          />
                        </div>
                        <div className="mt-2 text-[11px] text-muted-foreground">{detail.run.draft_queued_count || 0} queued · {detail.run.draft_failed_count || 0} failed</div>
                      </div>
                    </div>
                  </TablePanel>
                  <InspectorPanel>
                    <div className="space-y-4 p-4">
                      <MetaCard label="Source" value={detail.source.name} />
                      <MetaCard label="Current Stage" value={STAGE_LABELS_FULL[(detail.run.current_stage || "source_review") as WorkspaceStage] || detail.run.current_stage || "source_review"} />
                      <MetaCard label="Source Type" value={detail.source.source_type} />
                      <MetaCard label="Reference" value={String((evidence?.source_ref || {}).reference_uri || detail.source.config?.reference_uri || "n/a")} />
                    </div>
                  </InspectorPanel>
                </div>
              ) : (
                <>
                  <div className="flex flex-wrap gap-2 border-b border-border/70 pb-2">
                    {TAB_ORDER.map((tab) => (
                      <Button
                        key={tab}
                        type="button"
                        size="sm"
                        variant={activeTab === tab ? "default" : "ghost"}
                        onClick={() => {
                          setActiveTab(tab);
                          if (tab !== "review" && detail) {
                            setFocusedStage((detail.run.current_stage || "source_review") as WorkspaceStage);
                          }
                          setSelectedKey("");
                        }}
                      >
                        {TAB_LABELS[tab]}
                      </Button>
                    ))}
                  </div>
                </>
              )}

              {activeTab === "review" ? (
                <div className="grid gap-5 xl:grid-cols-[minmax(0,1.2fr)_380px]">
                  <TablePanel>
                    <div className="space-y-4">
                      <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
                        <div className="text-sm font-semibold text-foreground">{WORKBENCH_STEP_LABELS[currentWorkbenchStep]}</div>
                        <div className="mt-1 text-sm text-muted-foreground">{detail.run.next_action || "Review the current stage."}</div>
                      </div>

                      {currentWorkbenchStep === "evidence" ? (
                        <div className="space-y-3">
                          <div className="rounded-xl border border-border/70 p-4">
                            <div className="text-xs uppercase tracking-[0.12em] text-muted-foreground">Evidence Snapshot</div>
                            <div className="mt-2 grid gap-3 md:grid-cols-2">
                              <MetaCard label="Snapshot" value={evidence?.id || detail.run.evidence_snapshot_id} />
                              <MetaCard label="Type" value={evidence?.snapshot_type || "derived_on_read"} />
                              <MetaCard label="Suggestion Status" value={detail.run.suggestion_status} />
                              <MetaCard label="Source Type" value={detail.source.source_type} />
                            </div>
                          </div>
                          <div className="rounded-xl border border-border/70 p-4">
                            <div className="text-xs uppercase tracking-[0.12em] text-muted-foreground">Discovery Status</div>
                            <div className="mt-3 grid gap-3 md:grid-cols-3">
                              <MetricCard label="Assets" value={String(runAssets.length)} />
                              <MetricCard label="Operations" value={String(detail.operations.length)} />
                              <MetricCard label="Fields" value={String(detail.fields.length)} />
                            </div>
                          </div>
                        </div>
                      ) : null}

                      {currentWorkbenchStep === "semantic_model" ? (
                        <div className="space-y-3">
                          <div className="rounded-xl border border-border/70 p-4">
                            <div className="mb-3 flex items-center justify-between gap-2">
                              <div>
                                <div className="text-sm font-semibold text-foreground">Semantic Concepts</div>
                                <div className="text-xs text-muted-foreground">Existing semantic types are retrieved first; propose new concepts only when the current registry does not fit.</div>
                              </div>
                              <Badge variant={meaningProposals.length ? "warning" : "default"}>{meaningProposals.length} proposals</Badge>
                            </div>
                            <div className="space-y-2">
                              {currentStageTasks.filter((task) => task.stage === "semantic_model_drafting" || task.stage === "semantic_model_approval").map((task) => (
                                <button
                                  key={task.id}
                                  type="button"
                                  className={`w-full rounded-xl border px-3 py-3 text-left transition hover:bg-primary/[0.04] ${selectedTask?.id === task.id ? "border-primary/60 bg-primary/[0.06]" : "border-border/60"}`}
                                  onClick={() => setSelectedKey(task.id)}
                                >
                                  <div className="flex items-center justify-between gap-2">
                                    <div className="text-sm font-medium text-foreground">{task.title}</div>
                                    <Badge variant={task.draft_status === "draft_ready" ? "success" : "warning"}>{task.draft_status || "not_started"}</Badge>
                                  </div>
                                  <div className="mt-1 text-xs text-muted-foreground">{task.recommended_action || "Review semantic model candidates."}</div>
                                </button>
                              ))}
                              {!currentStageTasks.length ? <EmptyPanel message="No semantic model tasks ready." /> : null}
                            </div>
                          </div>
                          <div className="rounded-xl border border-border/70 p-4">
                            <div className="mb-3 flex items-center justify-between gap-2">
                              <div>
                                <div className="text-sm font-semibold text-foreground">Canonical Links</div>
                                <div className="text-xs text-muted-foreground">Semantic types and canonical links are reviewed together in the same approval phase.</div>
                              </div>
                              <Badge variant={canonicalProposals.length ? "warning" : "default"}>{canonicalProposals.length} proposals</Badge>
                            </div>
                            <div className="space-y-2">
                              {canonicalAttributes.slice(0, 6).map((item) => (
                                <div key={item.id} className="rounded-xl border border-border/60 px-3 py-2">
                                  <div className="flex items-center justify-between gap-2">
                                    <div className="text-sm font-medium text-foreground">{item.entity_name ? `${item.entity_name}.${item.name}` : item.name}</div>
                                    <Badge variant={item.status === "approved" ? "success" : item.pending_proposal_id ? "warning" : "default"}>{item.status || "draft"}</Badge>
                                  </div>
                                  <div className="mt-1 text-xs text-muted-foreground">{item.semantic_type_id || item.datatype || "No semantic link yet."}</div>
                                </div>
                              ))}
                              {!canonicalAttributes.length ? <EmptyPanel message="No canonical links defined yet." /> : null}
                            </div>
                          </div>
                        </div>
                      ) : null}

                      {currentWorkbenchStep === "binding" ? (
                        <div className="space-y-3">
                          <div className="rounded-xl border border-border/70 p-4">
                            <div className="mb-3 flex items-center justify-between gap-2">
                              <div>
                                <div className="text-sm font-semibold text-foreground">Binding Queue</div>
                                <div className="text-xs text-muted-foreground">Bindings are drafted against the latest approved semantic model.</div>
                              </div>
                              <Badge variant={unmappedFields.length ? "warning" : "success"}>{unmappedFields.length} unmapped</Badge>
                            </div>
                            <div className="space-y-2">
                              {(unmappedFields.length ? unmappedFields : filteredRunFields).slice(0, 8).map((item) => {
                                const mapping = detail.mappings.find((mappingItem) => mappingItem.field_id === item.id || mappingItem.field_path === item.field_path);
                                return (
                                  <button
                                    key={item.id}
                                    type="button"
                                    className={`w-full rounded-xl border px-3 py-3 text-left transition hover:bg-primary/[0.04] ${selectedField?.id === item.id ? "border-primary/60 bg-primary/[0.06]" : "border-border/60"}`}
                                    onClick={() => setSelectedKey(item.id)}
                                  >
                                    <div className="flex items-start justify-between gap-2">
                                      <div>
                                        <div className="text-sm font-medium text-foreground">{item.raw_name || item.display_name || item.field_path}</div>
                                        <div className="mt-1 font-mono text-[10px] text-muted-foreground">{item.field_path || item.id}</div>
                                      </div>
                                      <Badge variant={mapping ? "success" : "warning"}>{mapping ? "mapped" : "needs binding"}</Badge>
                                    </div>
                                  </button>
                                );
                              })}
                            </div>
                          </div>
                          <div className="rounded-xl border border-border/70 p-4">
                            <div className="mb-3 text-sm font-semibold text-foreground">Dependency Status</div>
                            <div className="space-y-2">
                              {bindingDependencies.slice(0, 6).map((item) => (
                                <div key={item.id} className="rounded-xl border border-border/60 px-3 py-2">
                                  <div className="flex items-center justify-between gap-2">
                                    <div className="text-sm font-medium text-foreground">{item.title}</div>
                                    <Badge variant={badgeVariantForDependency(item.dependency_status)}>{item.dependency_status}</Badge>
                                  </div>
                                  <div className="mt-1 text-xs text-muted-foreground">{item.resolution_basis} basis · {item.depends_on_count} dependency{item.depends_on_count === 1 ? "" : "ies"}</div>
                                </div>
                              ))}
                              {!bindingDependencies.length ? <EmptyPanel message="No binding dependencies recorded yet." /> : null}
                            </div>
                          </div>
                        </div>
                      ) : null}

                      {currentWorkbenchStep === "bundle_publish" ? (
                        <div className="space-y-3">
                          <div className="rounded-xl border border-border/70 p-4">
                            <div className="mb-3 text-sm font-semibold text-foreground">Workspace Changes</div>
                            <div className="mb-3 text-xs text-muted-foreground">
                              Semantic model and binding approvals are already applied to the authoring registry. Review the overall change set here, then publish the approved runtime snapshot.
                            </div>
                            <div className="grid gap-3 md:grid-cols-3">
                              <MetricCard label="Pending" value={String(detail.proposal_bundle.pending_count)} />
                              <MetricCard label="Approved" value={String(detail.proposal_bundle.approved_count)} />
                              <MetricCard label="Rejected" value={String(detail.proposal_bundle.rejected_count)} />
                            </div>
                          </div>
                          <div className="space-y-2">
                            {detail.proposals.map((item) => (
                              <button
                                key={item.id}
                                type="button"
                                className={`w-full rounded-xl border px-3 py-3 text-left transition hover:bg-primary/[0.04] ${selectedProposal?.id === item.id ? "border-primary/60 bg-primary/[0.06]" : "border-border/60"}`}
                                onClick={() => setSelectedKey(item.id)}
                              >
                                <div className="flex items-center justify-between gap-2">
                                  <div className="text-sm font-medium text-foreground">{item.title}</div>
                                  <Badge variant={item.status === "pending_review" ? "warning" : "default"}>{item.status}</Badge>
                                </div>
                                <div className="mt-1 text-xs text-muted-foreground">{item.entity_type} · {item.change_type}</div>
                              </button>
                            ))}
                          </div>
                        </div>
                      ) : null}
                    </div>
                  </TablePanel>
                  <InspectorPanel>
                    <div className="space-y-4 p-4">
                      <MetaCard label="Current Focus" value={WORKBENCH_STEP_LABELS[currentWorkbenchStep]} />
                      <MetaCard label="Next Action" value={detail.run.next_action || "-"} />
                      <MetaCard label="Source" value={detail.source.name} />
                      <MetaCard label="Pending Change Items" value={String(detail.proposal_bundle.pending_count)} />
                      {currentWorkbenchStep === "semantic_model" ? (
                        <div className="rounded-xl border border-border/70 bg-background/80 p-4">
                          <div className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Stage Actions</div>
                          <div className="mt-3 flex flex-wrap gap-2">
                            {semanticDraftTask ? (
                              <Button type="button" variant="outline" size="sm" onClick={() => void handleGenerateTaskDraft(semanticDraftTask.id)}>
                                <Sparkles className="h-3.5 w-3.5" />
                                Regenerate Semantic Model Drafts
                              </Button>
                            ) : null}
                            {semanticApprovalTask && semanticApprovalTask.status !== "completed" ? (
                              <Button type="button" size="sm" onClick={() => void handleCompleteTask(semanticApprovalTask.id)}>
                                <CheckCircle2 className="h-3.5 w-3.5" />
                                Approve Semantic Model
                              </Button>
                            ) : null}
                          </div>
                          <div className="mt-2 text-xs text-muted-foreground">
                            Approving the semantic model applies approved semantic concepts and canonical links to the authoring registry, then unlocks binding draft generation.
                          </div>
                        </div>
                      ) : null}
                      {currentWorkbenchStep === "semantic_model" && selectedTask ? (
                        <>
                          <MetaCard label="Selected Task" value={selectedTask.title} />
                          {semanticEvidenceClustersFromDraft(selectedTask.draft_payload).slice(0, 5).map((cluster, index) => {
                            const candidate = cluster.top_registry_candidate;
                            const candidateName = candidate && typeof candidate === "object" ? String((candidate as Record<string, unknown>).semantic_type_name || "") : "";
                            const candidateScore = candidate && typeof candidate === "object" ? Number((candidate as Record<string, unknown>).score || 0) : 0;
                            return (
                              <div key={String(cluster.field_id || index)} className="rounded-xl border border-border/60 bg-muted/20 p-3">
                                <div className="text-[12px] font-semibold text-foreground">{String(cluster.raw_name || cluster.field_path || "Field")}</div>
                                <div className="mt-1 text-[11px] text-muted-foreground">{String(cluster.cluster_summary || "-")}</div>
                                <div className="mt-2 flex flex-wrap gap-2 text-[11px]">
                                  <Badge variant={String(cluster.status || "") === "matched_existing" ? "success" : "warning"}>{String(cluster.status || "unknown")}</Badge>
                                  {candidateName ? <Badge variant="default">{candidateName} · {candidateScore.toFixed(2)}</Badge> : null}
                                </div>
                              </div>
                            );
                          })}
                        </>
                      ) : null}
                      {currentWorkbenchStep === "binding" && selectedField ? (
                        <>
                          <div className="rounded-xl border border-border/70 bg-background/80 p-4">
                            <div className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Stage Actions</div>
                            <div className="mt-3 flex flex-wrap gap-2">
                              {bindingDraftTask && (detail.run.current_stage === "binding_drafting" || bindingDraftTask.draft_status !== "draft_ready") ? (
                                <Button type="button" variant="outline" size="sm" onClick={() => void handleResumeRun()}>
                                  <Sparkles className="h-3.5 w-3.5" />
                                  Generate Binding Drafts
                                </Button>
                              ) : null}
                              {!detail.mappings.find((mapping) => mapping.field_id === selectedField.id || mapping.field_path === selectedField.field_path) ? (
                                <Button type="button" variant="outline" size="sm" onClick={() => void openCreateMapping(selectedField)}>
                                  Create Mapping
                                </Button>
                              ) : null}
                              {bindingApprovalTask && bindingApprovalTask.status !== "completed" ? (
                                <Button type="button" size="sm" onClick={() => void handleCompleteTask(bindingApprovalTask.id)}>
                                  <CheckCircle2 className="h-3.5 w-3.5" />
                                  Approve Binding
                                </Button>
                              ) : null}
                          </div>
                          <div className="mt-2 text-xs text-muted-foreground">
                              Binding drafts are generated from the latest approved semantic model, and approval applies source bindings to the authoring registry.
                          </div>
                        </div>
                          <MetaCard label="Selected Field" value={selectedField.raw_name || selectedField.field_path || selectedField.id} />
                          <MetaCard label="Path" value={selectedField.field_path || selectedField.id} />
                          <MetaCard label="Scope" value={selectedField.scope || "n/a"} />
                          <MetaCard label="Type" value={selectedField.data_type || "n/a"} />
                        </>
                      ) : null}
                      {currentWorkbenchStep === "bundle_publish" && selectedProposal ? (
                        <>
                          <div className="rounded-xl border border-border/70 bg-background/80 p-4">
                            <div className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Step Meaning</div>
                            <div className="mt-2 text-xs text-muted-foreground">
                              Use this step to inspect the full workspace change set and prepare runtime publication. Proposal decisions here govern release hygiene and publish readiness, not semantic model or binding authoring apply.
                            </div>
                          </div>
                          <ProposalInspector
                            proposal={selectedProposal}
                            submitting={reviewing}
                            onApprove={(id) => void handleReview(id, "approve")}
                            onReject={(id) => void handleReview(id, "reject")}
                          />
                        </>
                      ) : null}
                    </div>
                  </InspectorPanel>
                </div>
              ) : null}

              {activeTab === "source" ? (
                <div className="grid gap-5 xl:grid-cols-[minmax(0,1.15fr)_380px]">
                  <TablePanel>
                    <div className="space-y-4 rounded-2xl border border-border/70 bg-background/70 p-4">
                      <div className="grid gap-3 md:grid-cols-2">
                        <MetaCard label="Snapshot" value={evidence?.id || detail.run.evidence_snapshot_id} />
                        <MetaCard label="Type" value={evidence?.snapshot_type || "derived_on_read"} />
                        <MetaCard label="Content Hash" value={evidence?.content_hash || "n/a"} />
                        <MetaCard label="Suggestion Status" value={detail.run.suggestion_status} />
                      </div>
                      <div className="grid gap-4 xl:grid-cols-2">
                        <section className="rounded-xl border border-border/70 p-4">
                          <div className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Operation Evidence</div>
                          <div className="mt-3 space-y-2">
                            {(evidence?.operation_evidence as Array<Record<string, unknown>> | undefined)?.length ? (
                              (evidence?.operation_evidence as Array<Record<string, unknown>>).map((item, index) => (
                                <div key={`operation-evidence-${index}`} className="rounded-lg border border-border/60 px-3 py-2 text-sm">
                                  <div className="font-medium text-foreground">{String(item.operation_name || item.operation_id || "Operation")}</div>
                                  <div className="mt-1 font-mono text-[11px] text-muted-foreground">{String(item.http_method || "")} {String(item.access_path_locator || "")}</div>
                                </div>
                              ))
                            ) : (
                              <div className="text-sm text-muted-foreground">No operation evidence attached yet.</div>
                            )}
                          </div>
                        </section>
                        <section className="rounded-xl border border-border/70 p-4">
                          <div className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Sample Values</div>
                          <div className="mt-3 space-y-2">
                            {Object.entries(evidence?.sample_values || {}).length ? (
                              Object.entries(evidence?.sample_values || {}).slice(0, 10).map(([key, value]) => (
                                <div key={key} className="rounded-lg border border-border/60 px-3 py-2 text-sm">
                                  <div className="font-mono text-[11px] text-foreground">{key}</div>
                                  <div className="mt-1 break-all text-muted-foreground">{JSON.stringify(value)}</div>
                                </div>
                              ))
                            ) : (
                              <div className="text-sm text-muted-foreground">No extracted sample values yet.</div>
                            )}
                          </div>
                        </section>
                      </div>
                    </div>
                  </TablePanel>
                  <InspectorPanel>
                    <div className="space-y-4 p-4">
                      <div>
                        <div className="text-sm font-semibold text-foreground">{detail.source.name}</div>
                        <div className="mt-1 font-mono text-[11px] text-muted-foreground">{detail.source.id}</div>
                      </div>
                      <MetaCard label="Provider" value={detail.source.provider || "n/a"} />
                      <MetaCard label="Reference" value={String((evidence?.source_ref || {}).reference_uri || detail.source.config?.reference_uri || "n/a")} />
                      <MetaCard label="Uploaded File" value={String(((evidence?.source_ref || {}).upload as Record<string, unknown> | undefined)?.filename || "n/a")} />
                    </div>
                  </InspectorPanel>
                </div>
              ) : null}

              {activeTab === "assets" ? (
                <div className="grid gap-5 xl:grid-cols-[minmax(0,1.1fr)_380px]">
                  <TablePanel>
                    {runAssets.length ? (
                      <table className="min-w-full table-fixed text-left text-[12px]">
                        <thead className="sticky top-0 z-10 border-b border-border/70 bg-slate-50/70 text-[10px] uppercase tracking-[0.08em] text-muted-foreground backdrop-blur">
                          <tr>
                            <th className="w-[42%] px-3 py-2.5 font-medium">Asset</th>
                            <th className="w-[24%] px-3 py-2.5 font-medium">Type</th>
                            <th className="w-[18%] px-3 py-2.5 font-medium">Ops</th>
                            <th className="w-[16%] px-3 py-2.5 font-medium">Fields</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-200/80">
                          {runAssets.map((asset) => {
                            const operationCount = detail.operations.filter((item) => item.asset_id === asset.id).length;
                            const fieldCount = detail.fields.filter((item) => detail.operations.find((operation) => operation.id === item.operation_id && operation.asset_id === asset.id)).length;
                            return (
                              <tr key={asset.id} className={`cursor-pointer transition hover:bg-primary/[0.04] ${selectedAsset?.id === asset.id ? "bg-primary/[0.08] shadow-[inset_2px_0_0_0_hsl(var(--primary))]" : ""}`} onClick={() => setSelectedAssetId(asset.id)}>
                                <td className="px-3 py-2.5">
                                  <div className="truncate text-[13px] font-semibold text-foreground">{asset.name}</div>
                                  <div className="mt-0.5 truncate font-mono text-[10px] text-muted-foreground">{asset.id}</div>
                                </td>
                                <td className="px-3 py-2.5 text-muted-foreground">{asset.asset_type || "-"}</td>
                                <td className="px-3 py-2.5 text-muted-foreground">{operationCount}</td>
                                <td className="px-3 py-2.5 text-muted-foreground">{fieldCount}</td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    ) : (
                      <EmptyPanel message="No run-scoped assets discovered yet." />
                    )}
                  </TablePanel>
                  <InspectorPanel>
                    {selectedAsset ? (
                      <div className="space-y-4 p-4">
                        <div>
                          <div className="text-sm font-semibold text-foreground">{selectedAsset.name}</div>
                          <div className="mt-1 font-mono text-[11px] text-muted-foreground">{selectedAsset.id}</div>
                        </div>
                        <MetaCard label="Asset Type" value={selectedAsset.asset_type || "-"} />
                        <MetaCard label="Operations" value={String(filteredRunOperations.length)} />
                        <MetaCard label="Structures" value={String(filteredRunFields.length)} />
                      </div>
                    ) : (
                      <EmptyPanel message="No asset selected." />
                    )}
                  </InspectorPanel>
                </div>
              ) : null}

              {activeTab === "operations" ? (
                <div className="grid gap-5 xl:grid-cols-[minmax(0,1.1fr)_380px]">
                  <TablePanel>
                    <table className="min-w-full table-fixed text-left text-[12px]">
                      <thead className="sticky top-0 z-10 border-b border-border/70 bg-slate-50/70 text-[10px] uppercase tracking-[0.08em] text-muted-foreground backdrop-blur">
                        <tr>
                          <th className="w-[38%] px-3 py-2.5 font-medium">Operation</th>
                          <th className="w-[28%] px-3 py-2.5 font-medium">Path</th>
                          <th className="w-[18%] px-3 py-2.5 font-medium">Fields</th>
                          <th className="w-[16%] px-3 py-2.5 font-medium">Mappings</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-200/80">
                        {filteredRunOperations.map((item) => {
                          const fieldCount = filteredRunFields.filter((field) => field.operation_id === item.id).length;
                          const mappingCount = detail.mappings.filter((mapping) => mapping.operation_id === item.id).length;
                          return (
                            <tr key={item.id} className={`cursor-pointer transition hover:bg-primary/[0.04] ${selectedOperation?.id === item.id ? "bg-primary/[0.08] shadow-[inset_2px_0_0_0_hsl(var(--primary))]" : ""}`} onClick={() => setSelectedKey(item.id)}>
                              <td className="px-3 py-2.5">
                                <div className="truncate text-[13px] font-semibold text-foreground">{item.name}</div>
                                <div className="mt-0.5 truncate font-mono text-[10px] text-muted-foreground">{item.operation_key}</div>
                              </td>
                              <td className="px-3 py-2.5 text-muted-foreground">{item.http_method || "GET"} {item.access_path_locator || item.access_path_name}</td>
                              <td className="px-3 py-2.5 text-muted-foreground">{fieldCount}</td>
                              <td className="px-3 py-2.5 text-muted-foreground">{mappingCount}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </TablePanel>
                  <InspectorPanel>
                    {selectedOperation ? (
                      <div className="space-y-4 p-4">
                        <div>
                          <div className="text-sm font-semibold text-foreground">{selectedOperation.name}</div>
                          <div className="mt-1 font-mono text-[11px] text-muted-foreground">{selectedOperation.id}</div>
                        </div>
                        <MetaCard label="Method / Path" value={`${selectedOperation.http_method || "GET"} ${selectedOperation.access_path_locator || selectedOperation.access_path_name || ""}`} />
                        <MetaCard label="Asset" value={selectedOperation.asset_name || "n/a"} />
                        <MetaCard label="Description" value={selectedOperation.description || "No description."} />
                      </div>
                    ) : (
                      <EmptyPanel message="No operation selected." />
                    )}
                  </InspectorPanel>
                </div>
              ) : null}

              {activeTab === "schemas" ? (
                <div className="grid gap-5 xl:grid-cols-[minmax(0,1.1fr)_380px]">
                  <TablePanel>
                    <table className="min-w-full table-fixed text-left text-[12px]">
                      <thead className="sticky top-0 z-10 border-b border-border/70 bg-slate-50/70 text-[10px] uppercase tracking-[0.08em] text-muted-foreground backdrop-blur">
                        <tr>
                          <th className="w-[34%] px-3 py-2.5 font-medium">Field</th>
                          <th className="w-[16%] px-3 py-2.5 font-medium">Scope</th>
                          <th className="w-[16%] px-3 py-2.5 font-medium">Type</th>
                          <th className="w-[18%] px-3 py-2.5 font-medium">Mapped</th>
                          <th className="w-[16%] px-3 py-2.5 font-medium">Action</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-200/80">
                        {filteredRunFields.map((item) => {
                          const mapping = detail.mappings.find((mappingItem) => mappingItem.field_id === item.id || mappingItem.field_path === item.field_path);
                          return (
                            <tr key={item.id} className={`cursor-pointer transition hover:bg-primary/[0.04] ${selectedField?.id === item.id ? "bg-primary/[0.08] shadow-[inset_2px_0_0_0_hsl(var(--primary))]" : ""}`} onClick={() => setSelectedKey(item.id)}>
                              <td className="px-3 py-2.5">
                                <div className="truncate text-[13px] font-semibold text-foreground">{item.raw_name || item.display_name || item.field_path}</div>
                                <div className="mt-0.5 truncate font-mono text-[10px] text-muted-foreground">{item.field_path || item.id}</div>
                              </td>
                              <td className="px-3 py-2.5"><Badge variant="default">{item.scope}</Badge></td>
                              <td className="px-3 py-2.5 text-muted-foreground">{item.data_type}</td>
                              <td className="px-3 py-2.5">
                                {mapping ? <Badge variant="success">{mapping.semantic_type_id}</Badge> : <Badge variant="warning">unmapped</Badge>}
                              </td>
                              <td className="px-3 py-2.5">
                                {!mapping ? (
                                  <Button type="button" size="sm" variant="ghost" onClick={(event) => {
                                    event.stopPropagation();
                                    void openCreateMapping(item);
                                  }}>
                                    Create
                                  </Button>
                                ) : null}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </TablePanel>
                  <InspectorPanel>
                    {selectedField ? (
                      <div className="space-y-4 p-4">
                        <div>
                          <div className="text-sm font-semibold text-foreground">{selectedField.raw_name || selectedField.display_name || selectedField.field_path}</div>
                          <div className="mt-1 font-mono text-[11px] text-muted-foreground">{selectedField.field_path || selectedField.id}</div>
                        </div>
                        <MetaCard label="Scope" value={selectedField.scope || "n/a"} />
                        <MetaCard label="Type" value={selectedField.data_type || "n/a"} />
                        <MetaCard label="Description" value={selectedField.description || "No description."} />
                        {!detail.mappings.find((mapping) => mapping.field_id === selectedField.id || mapping.field_path === selectedField.field_path) ? (
                          <Button type="button" onClick={() => void openCreateMapping(selectedField)}>
                            Create Mapping
                          </Button>
                        ) : null}
                      </div>
                    ) : (
                      <EmptyPanel message="No field selected." />
                    )}
                  </InspectorPanel>
                </div>
              ) : null}

              {activeTab === "work_queue" ? (
                <div className="grid gap-5 xl:grid-cols-[minmax(0,1.1fr)_380px]">
                  <TablePanel>
                    <table className="min-w-full table-fixed text-left text-[12px]">
                      <thead className="sticky top-0 z-10 border-b border-border/70 bg-slate-50/70 text-[10px] uppercase tracking-[0.08em] text-muted-foreground backdrop-blur">
                        <tr>
                          <th className="w-[38%] px-3 py-2.5 font-medium">Task</th>
                          <th className="w-[24%] px-3 py-2.5 font-medium">Target</th>
                          <th className="w-[18%] px-3 py-2.5 font-medium">Type</th>
                          <th className="w-[20%] px-3 py-2.5 font-medium">Status</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-200/80">
                        {detail.work_queue_tasks.map((item) => (
                          <tr key={item.id} className={`cursor-pointer transition hover:bg-primary/[0.04] ${selectedTask?.id === item.id ? "bg-primary/[0.08] shadow-[inset_2px_0_0_0_hsl(var(--primary))]" : ""}`} onClick={() => setSelectedKey(item.id)}>
                            <td className="px-3 py-2.5">
                              <div className="truncate text-[13px] font-semibold text-foreground">{item.title}</div>
                              <div className="mt-0.5 font-mono text-[10px] text-muted-foreground">{item.id}</div>
                            </td>
                            <td className="px-3 py-2.5 text-muted-foreground">{item.field_path || item.operation_name || detail.source.name}</td>
                            <td className="px-3 py-2.5">
                              <div className="flex flex-col gap-1">
                                <Badge variant="default">{item.task_type}</Badge>
                                <span className="text-[10px] text-muted-foreground">{item.stage || "source_review"}</span>
                              </div>
                            </td>
                            <td className="px-3 py-2.5">
                              <div className="flex flex-col gap-1">
                                <Badge variant={item.status === "completed" ? "success" : item.status === "blocked" ? "danger" : "warning"}>{item.status}</Badge>
                                <span className="text-[10px] text-muted-foreground">draft {item.draft_status || "not_started"}</span>
                              </div>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </TablePanel>
                  <InspectorPanel>
                    {selectedTask ? (
                      <div className="space-y-4 p-4">
                        <div>
                          <div className="text-sm font-semibold text-foreground">{selectedTask.title}</div>
                          <div className="mt-1 font-mono text-[11px] text-muted-foreground">{selectedTask.id}</div>
                        </div>
                        <MetaCard label="Stage" value={selectedTask.stage || "source_review"} />
                        <MetaCard label="Target" value={selectedTask.field_path || selectedTask.operation_name || detail.source.name} />
                        <MetaCard label="Status" value={selectedTask.status} />
                        <MetaCard label="Draft Status" value={selectedTask.draft_status || "not_started"} />
                        <MetaCard label="Recommended Action" value={selectedTask.recommended_action || "-"} />
                        {selectedTask.stage === "semantic_model_drafting" ? (
                          <div className="space-y-3 rounded-2xl border border-border/70 bg-background/80 p-3">
                            <div className="text-xs font-semibold uppercase tracking-[0.08em] text-muted-foreground">Semantic Evidence Clusters</div>
                            {semanticEvidenceClustersFromDraft(selectedTask.draft_payload).slice(0, 5).map((cluster, index) => {
                              const candidate = cluster.top_registry_candidate;
                              const candidateName =
                                candidate && typeof candidate === "object" ? String((candidate as Record<string, unknown>).semantic_type_name || "") : "";
                              const candidateScore =
                                candidate && typeof candidate === "object" ? Number((candidate as Record<string, unknown>).score || 0) : 0;
                              return (
                                <div key={String(cluster.field_id || index)} className="rounded-xl border border-border/60 bg-muted/20 p-3">
                                  <div className="text-[12px] font-semibold text-foreground">
                                    {String(cluster.raw_name || cluster.field_path || "Field")}
                                  </div>
                                  <div className="mt-1 text-[11px] text-muted-foreground">
                                    {String(cluster.cluster_summary || "-")}
                                  </div>
                                  <div className="mt-2 flex flex-wrap gap-2 text-[11px]">
                                    <Badge variant={String(cluster.status || "") === "matched_existing" ? "success" : "warning"}>
                                      {String(cluster.status || "unknown")}
                                    </Badge>
                                    {candidateName ? (
                                      <Badge variant="default">
                                        {candidateName} · {candidateScore.toFixed(2)}
                                      </Badge>
                                    ) : null}
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        ) : null}
                        <MetaCard label="Payload" value={JSON.stringify(selectedTask.payload || {})} />
                        <MetaCard label="Draft" value={JSON.stringify(selectedTask.draft_payload || {})} />
                        <div className="flex flex-wrap gap-2">
                          <Button type="button" variant="outline" size="sm" onClick={() => void handleGenerateTaskDraft(selectedTask.id)}>
                            <Sparkles className="h-3.5 w-3.5" />
                            Generate Draft
                          </Button>
                          <Button type="button" variant="default" size="sm" onClick={() => void handleCompleteTask(selectedTask.id)} disabled={selectedTask.status === "completed"}>
                            <CheckCircle2 className="h-3.5 w-3.5" />
                            Complete
                          </Button>
                        </div>
                      </div>
                    ) : (
                      <EmptyPanel message="No task selected." />
                    )}
                  </InspectorPanel>
                </div>
              ) : null}

            </>
          ) : null}

          {createOpen && detail ? (
            <MappingForm
              title="Create Mapping"
              description="Map one source field to semantic type and transform contract."
              form={form}
              sources={[detail.source]}
              operations={detail.operations}
              operationFields={detail.fields.filter((item) => !detail.mappings.some((mapping) => mapping.field_id === item.id || (mapping.operation_id === item.operation_id && mapping.field_path === String(item.field_path || item.raw_name))))}
              semanticTypes={semanticTypes}
              canonicalEntities={canonicalEntities}
              canonicalAttributes={canonicalAttributes}
              sourceFieldLocked
              onChange={(next) => {
                const semanticChanged = next.semanticTypeId !== form.semanticTypeId;
                setForm(next);
                if (semanticChanged && next.semanticTypeId) {
                  void generateMappingSuggestion(createOperationFieldId, next.semanticTypeId, false);
                }
              }}
              transformSuggestion={mappingSuggestion?.transform_suggestion as TransformSuggestion | null}
              transformSuggestionLoading={suggestionLoading}
              transformSuggestionError={suggestionError}
              semanticTypeSuggestions={mappingSuggestion?.semantic_type_suggestions || []}
              semanticSuggestionLoading={suggestionLoading}
              semanticSuggestionError={suggestionError}
              onGenerateMappingSuggestion={() => void generateMappingSuggestion(createOperationFieldId, form.semanticTypeId, !form.semanticTypeId)}
              onApplySemanticTypeSuggestion={applySemanticTypeSuggestion}
              onSubmit={submitCreateMapping}
              onCancel={() => {
                setCreateOpen(false);
                setCreateOperationFieldId("");
                setMappingSuggestion(null);
              }}
              submitLabel="Create Mapping"
              submitting={submitting}
              submitDisabled={submitting || !!createFormValidationError}
              formValidationMessage={createFormValidationError}
              onCreateSemanticType={handleCreateSemanticType}
              onCreateCanonicalEntity={handleCreateCanonicalEntity}
              onCreateCanonicalAttribute={handleCreateCanonicalAttribute}
            />
          ) : null}

          <ErrorModal open={!!actionError} message={actionError} onClose={() => setActionError("")} />
          <ActionToast
            open={!!actionMessage}
            message={actionMessage}
            actionLabel={actionLinkProposalIds.length ? `View ${actionLinkProposalIds.length} Proposals` : undefined}
            onAction={
              actionLinkProposalIds.length
                ? () => {
                    setActiveTab("review");
                    setFocusedStage("proposal_review");
                    setSelectedKey(actionLinkProposalIds[0] || "");
                  }
                : undefined
            }
            onClose={() => {
              setActionMessage("");
              setActionLinkProposalIds([]);
            }}
          />
        </div>
      }
    />
  );
}
