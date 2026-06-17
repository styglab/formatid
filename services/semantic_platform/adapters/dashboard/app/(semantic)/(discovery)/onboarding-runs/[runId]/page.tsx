"use client";

import type { FormEvent } from "react";
import Link from "next/link";
import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import { ArrowLeft, ExternalLink, RefreshCw, Sparkles, CheckCircle2, PlayCircle } from "lucide-react";
import {
  completeOnboardingTask,
  createCanonicalAttribute,
  createCanonicalEntity,
  createMapping,
  createSemanticType,
  generateOnboardingTaskDraft,
  getOnboardingRun,
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
import { EmptyPanel, ErrorPanel, InfoLine, LoadingPanel } from "@/components/semantic/common/state-panel";
import { MetaCard, MetricCard } from "@/components/semantic/common/meta-card";
import { useCanonicalModel } from "@/hooks/semantic/use-canonical-model";
import { useSemanticRegistry } from "@/hooks/semantic/use-proposals";
import { parseJsonObject, stringifyJson } from "@/lib/semantic/forms";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import type { OperationField, OnboardingRunDetail } from "@/types/semantic";

type RunTab = "evidence" | "operations" | "schemas" | "work_queue" | "bundle";

const TAB_LABELS: Record<RunTab, string> = {
  evidence: "Evidence",
  operations: "Operations / Access Paths",
  schemas: "Structures",
  work_queue: "Review Tasks",
  bundle: "Proposal Bundle",
};

export default function OnboardingRunDetailPage() {
  const params = useParams<{ runId: string }>();
  const runId = Array.isArray(params?.runId) ? params.runId[0] : params?.runId || "";
  const [detail, setDetail] = useState<OnboardingRunDetail | null>(null);
  const [activeTab, setActiveTab] = useState<RunTab>("evidence");
  const [selectedKey, setSelectedKey] = useState("");
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
  const selectedField = detail?.fields.find((item) => item.id === selectedKey) || detail?.fields[0] || null;
  const selectedTask = detail?.work_queue_tasks.find((item) => item.id === selectedKey) || detail?.work_queue_tasks[0] || null;
  const selectedProposal = detail?.proposals.find((item) => item.id === selectedKey) || detail?.proposals[0] || null;
  const evidence = detail?.evidence_snapshots[0] || null;
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
      setActiveTab("bundle");
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
      setActionMessage("Task completed.");
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
      setActionMessage("Onboarding run resumed.");
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to resume onboarding run.");
    }
  }

  return (
    <SectionPlaceholder
      title={detail?.run.source_name || "Onboarding Run"}
      description="Evidence, discovered operations, schema fields, work queue tasks, and generated proposal bundle for a single source onboarding run."
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
        </div>
      }
      body={
        <div className="space-y-5">
          {error ? <ErrorPanel message={error} /> : null}
          {loading ? <LoadingPanel message="Loading onboarding run detail..." /> : null}
          {!loading && !error && !detail ? <EmptyPanel message="Onboarding run not found." /> : null}
          {!loading && !error && detail ? (
            <>
              <InfoLine>
                <Badge variant={detail.run.pending_proposal_count ? "warning" : "default"}>{detail.run.status}</Badge>
                <Badge variant="info">{detail.run.stage || "source_uploaded"}</Badge>
                <Badge variant="default">{detail.run.current_stage || "source_review"}</Badge>
                <Badge variant={detail.run.stage_status === "completed" ? "success" : "warning"}>{detail.run.stage_status || "pending"}</Badge>
                <Badge variant="default">{detail.source.source_type}</Badge>
                <Badge variant="warning">{detail.work_queue_tasks.length} tasks</Badge>
              </InfoLine>

              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
                <MetricCard label="Operations" value={String(detail.run.operation_count)} />
                <MetricCard label="Fields" value={String(detail.run.field_count)} />
                <MetricCard label="Mappings" value={String(detail.run.mapping_count)} />
                <MetricCard label="Proposals" value={String(detail.run.proposal_count)} />
                <MetricCard label="Pending" value={String(detail.run.pending_proposal_count)} />
              </div>

              <div className="rounded-xl border border-border/70 bg-muted/15 px-4 py-4">
                <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                  <div>
                    <div className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Current Stage</div>
                    <div className="mt-1 text-sm font-semibold text-foreground">{detail.run.current_stage || "source_review"}</div>
                    <div className="mt-1 text-sm text-muted-foreground">{detail.run.next_action || "Review current stage tasks."}</div>
                  </div>
                  <Button type="button" variant="outline" onClick={() => void handleResumeRun()}>
                    <PlayCircle className="h-4 w-4" />
                    Resume Worker
                  </Button>
                </div>
              </div>

              <div className="flex flex-wrap gap-2 border-b border-border/70 pb-2">
                {(Object.keys(TAB_LABELS) as RunTab[]).map((tab) => (
                  <Button
                    key={tab}
                    type="button"
                    size="sm"
                    variant={activeTab === tab ? "default" : "ghost"}
                    onClick={() => {
                      setActiveTab(tab);
                      setSelectedKey("");
                    }}
                  >
                    {TAB_LABELS[tab]}
                  </Button>
                ))}
              </div>

              {activeTab === "evidence" ? (
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
                        {detail.operations.map((item) => {
                          const fieldCount = detail.fields.filter((field) => field.operation_id === item.id).length;
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
                        {detail.fields.map((item) => {
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

              {activeTab === "bundle" ? (
                <div className="grid gap-5 xl:grid-cols-[minmax(0,1.1fr)_380px]">
                  <TablePanel>
                    <table className="min-w-full table-fixed text-left text-[12px]">
                      <thead className="sticky top-0 z-10 border-b border-border/70 bg-slate-50/70 text-[10px] uppercase tracking-[0.08em] text-muted-foreground backdrop-blur">
                        <tr>
                          <th className="w-[42%] px-3 py-2.5 font-medium">Proposal</th>
                          <th className="w-[18%] px-3 py-2.5 font-medium">Entity</th>
                          <th className="w-[16%] px-3 py-2.5 font-medium">Change</th>
                          <th className="w-[14%] px-3 py-2.5 font-medium">Status</th>
                          <th className="w-[10%] px-3 py-2.5 font-medium">View</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-200/80">
                        {detail.proposals.map((item) => (
                          <tr key={item.id} className={`cursor-pointer transition hover:bg-primary/[0.04] ${selectedProposal?.id === item.id ? "bg-primary/[0.08] shadow-[inset_2px_0_0_0_hsl(var(--primary))]" : ""}`} onClick={() => setSelectedKey(item.id)}>
                            <td className="px-3 py-2.5">
                              <div className="truncate text-[13px] font-semibold text-foreground">{item.title}</div>
                              <div className="mt-0.5 font-mono text-[10px] text-muted-foreground">{item.id}</div>
                            </td>
                            <td className="px-3 py-2.5 text-muted-foreground">{item.entity_type}</td>
                            <td className="px-3 py-2.5 text-muted-foreground">{item.change_type}</td>
                            <td className="px-3 py-2.5"><Badge variant={item.status === "pending_review" ? "warning" : "default"}>{item.status}</Badge></td>
                            <td className="px-3 py-2.5 text-muted-foreground">
                              <ExternalLink className="h-3.5 w-3.5" />
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </TablePanel>
                  <InspectorPanel>
                    <div className="space-y-4 p-4">
                      <div>
                        <div className="text-sm font-semibold text-foreground">{detail.proposal_bundle.source_name}</div>
                        <div className="mt-1 font-mono text-[11px] text-muted-foreground">{detail.proposal_bundle.id}</div>
                      </div>
                      <div className="grid grid-cols-3 gap-3">
                        <MetricCard label="Pending" value={String(detail.proposal_bundle.pending_count)} />
                        <MetricCard label="Approved" value={String(detail.proposal_bundle.approved_count)} />
                        <MetricCard label="Rejected" value={String(detail.proposal_bundle.rejected_count)} />
                      </div>
                      <div className="rounded-xl border border-border/70 p-4">
                        <div className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Entity Breakdown</div>
                        <div className="mt-3 flex flex-wrap gap-2">
                          {Object.entries(detail.proposal_bundle.entity_counts || {}).length ? (
                            Object.entries(detail.proposal_bundle.entity_counts || {}).map(([key, value]) => (
                              <Badge key={key} variant="default">{key} {value}</Badge>
                            ))
                          ) : (
                            <span className="text-sm text-muted-foreground">No linked proposals.</span>
                          )}
                        </div>
                      </div>
                      <div className="rounded-xl border border-border/70 bg-background/80">
                        <ProposalInspector
                          proposal={selectedProposal}
                          submitting={reviewing}
                          onApprove={(id) => void handleReview(id, "approve")}
                          onReject={(id) => void handleReview(id, "reject")}
                        />
                      </div>
                    </div>
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
                    setActiveTab("bundle");
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
