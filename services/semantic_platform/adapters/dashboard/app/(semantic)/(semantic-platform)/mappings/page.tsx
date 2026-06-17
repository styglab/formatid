"use client";

import type { FormEvent } from "react";
import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { Save, Trash2 } from "lucide-react";
import {
  createCanonicalAttribute,
  createCanonicalEntity,
  createSemanticType,
  deleteMapping,
  listExecutionOperations,
  listExecutionSources,
  updateMapping,
} from "@/api/semantic-admin";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { TablePanel } from "@/components/layout/table-panel";
import { DetailDrawer } from "@/components/layout/detail-drawer";
import { InspectorPanel } from "@/components/layout/inspector-panel";
import { MappingFormFields, mappingFormDefaults, type MappingFormState } from "@/components/semantic/forms/mapping-form";
import { FilterToolbar } from "@/components/semantic/filters/filter-toolbar";
import { ActionToast } from "@/components/semantic/common/action-toast";
import { ConfirmModal } from "@/components/semantic/common/confirm-modal";
import { EmptyPanel, ErrorPanel, InfoLine, LoadingPanel } from "@/components/semantic/common/state-panel";
import { ErrorModal } from "@/components/semantic/common/error-modal";
import { PaginationBar } from "@/components/semantic/common/pagination-bar";
import { MappingsTable, type MappingTableRow } from "@/components/semantic/tables/mappings-table";
import { useAllMappings, useMappingExists } from "@/hooks/semantic/use-mappings";
import { useCanonicalModel } from "@/hooks/semantic/use-canonical-model";
import { useOperationFields } from "@/hooks/semantic/use-operations";
import { useSemanticRegistry } from "@/hooks/semantic/use-proposals";
import { parseJsonObject, stringifyJson } from "@/lib/semantic/forms";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Combobox } from "@/components/ui/combobox";
import { InspectorSection, InspectorSurface } from "@/components/semantic/inspector/inspector-section";
import type { ExecutionOperation, ExecutionSource } from "@/types/semantic";

type BulkSelectionItem = {
  id: string;
  fieldLabel: string;
  fieldPath: string;
  sourceId: string;
  operationId: string;
  semanticTypeId: string;
  notes: string;
};

type MappedRow = {
  id: string;
  kind: "mapped";
  mapping: NonNullable<ReturnType<typeof useAllMappings>["data"]>[number];
  fieldLabel: string;
  fieldPath: string;
  sourceLabel: string;
  operationId: string;
  semanticTypeLabel: string;
  semanticTypeId: string;
  pendingProposalId: string;
  reviewState: string;
};

export default function MappingsPage() {
  const router = useRouter();
  const [selectedId, setSelectedId] = useState("");
  const [query, setQuery] = useState("");
  const [review, setReview] = useState("all");
  const [page, setPage] = useState(1);
  const [detailOpen, setDetailOpen] = useState(false);
  const pageSize = 10;
  const { data: allMappings, loading, error, reload } = useAllMappings();
  const { data: operationFields } = useOperationFields();
  const { semanticTypes, reload: reloadSemanticRegistry } = useSemanticRegistry();
  const { entities: canonicalEntities, attributes: canonicalAttributesRegistry, reload: reloadCanonicalModel } = useCanonicalModel();
  const [form, setForm] = useState<MappingFormState>(mappingFormDefaults);
  const [sources, setSources] = useState<ExecutionSource[]>([]);
  const [operations, setOperations] = useState<ExecutionOperation[]>([]);
  const [submitting, setSubmitting] = useState(false);
  const [actionError, setActionError] = useState("");
  const [actionMessage, setActionMessage] = useState("");
  const [actionLinkProposalIds, setActionLinkProposalIds] = useState<string[]>([]);
  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false);
  const [bulkSelections, setBulkSelections] = useState<Record<string, BulkSelectionItem>>({});
  const [bulkAssignOpen, setBulkAssignOpen] = useState(false);
  const [bulkSemanticTypeId, setBulkSemanticTypeId] = useState("");
  const semanticTypeNames = useMemo(
    () =>
      semanticTypes.reduce<Record<string, string>>((accumulator, item) => {
        accumulator[item.id] = (item.draft_snapshot || item).name;
        return accumulator;
      }, {}),
    [semanticTypes]
  );
  const operationSourceMap = useMemo(
    () =>
      operations.reduce<Record<string, string>>((accumulator, item) => {
        accumulator[item.id] = item.source_id || item.source_name || "";
        return accumulator;
      }, {}),
    [operations]
  );
  const canonicalAttributes = useMemo(
    () => canonicalAttributesRegistry,
    [canonicalAttributesRegistry]
  );
  const canonicalAttributeNames = useMemo(
    () =>
      canonicalAttributes.reduce<Record<string, string>>((accumulator, item) => {
        const display = item.draft_snapshot || item;
        accumulator[item.id] = display.name;
        return accumulator;
      }, {}),
    [canonicalAttributes]
  );
  const fieldLabels = useMemo(() =>
    operationFields.reduce<Record<string, string>>((accumulator, item) => {
      const displayName = (item.display_name || "").trim();
      const raw = String(item.raw_name || "").trim();
      const path = String(item.field_path || item.raw_name).trim();
      accumulator[`${item.operation_id}::${String(item.field_path || item.raw_name)}`] = displayName || raw || path;
      return accumulator;
    }, {}),
    [operationFields]
  );
  const mappedFieldKeys = useMemo(
    () =>
      new Set(
        allMappings.map((item) => {
          const display = item.draft_snapshot || item;
          return `${display.operation_id}::${display.field_path}`;
        })
      ),
    [allMappings]
  );
  const unmappedOperationFields = useMemo(
    () =>
      operationFields.filter((item) => {
        const fieldKey = `${item.operation_id}::${String(item.field_path || item.raw_name)}`;
        return !mappedFieldKeys.has(fieldKey);
      }),
    [mappedFieldKeys, operationFields]
  );
  const mappedRows = useMemo<MappedRow[]>(() => {
    const mappedRows: MappedRow[] = allMappings.map((item) => {
      const display = item.draft_snapshot || item;
      const fieldKey = `${display.operation_id}::${display.field_path}`;
      return {
        id: item.id,
        kind: "mapped",
        mapping: item,
        fieldLabel: fieldLabels[fieldKey] || display.field_path,
        fieldPath: display.field_path,
        sourceLabel: display.source_id || operationSourceMap[display.operation_id] || "-",
        operationId: display.operation_id,
        semanticTypeLabel: semanticTypeNames[display.semantic_type_id] || display.semantic_type_id,
        semanticTypeId: display.semantic_type_id,
        pendingProposalId: item.pending_proposal_id || "",
        reviewState: item.pending_proposal_id ? "Pending Review" : item.draft_snapshot ? "Draft" : "Approved",
      };
    });
    return mappedRows;
  }, [allMappings, fieldLabels, operationSourceMap, semanticTypeNames]);
  const filteredRows = useMemo(() => {
    const normalizedQuery = query.trim().toLowerCase();
    return mappedRows.filter((row) => {
      if (review === "pending_review" && row.reviewState !== "Pending Review") return false;
      if (review === "approved" && row.reviewState !== "Approved") return false;
      if (!normalizedQuery) return true;
      return [
        row.fieldLabel,
        row.fieldPath,
        row.sourceLabel,
        row.operationId,
        row.semanticTypeLabel,
        row.semanticTypeId,
      ]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(normalizedQuery));
    });
  }, [mappedRows, query, review]);
  const pagedRows = useMemo(() => {
    const start = (page - 1) * pageSize;
    return filteredRows.slice(start, start + pageSize);
  }, [filteredRows, page, pageSize]);
  const selectedRow = useMemo(() => pagedRows.find((item) => item.id === selectedId) || filteredRows.find((item) => item.id === selectedId) || null, [filteredRows, pagedRows, selectedId]);
  const selectedMapping = selectedRow?.mapping || null;
  const selectedDisplay = selectedMapping ? selectedMapping.draft_snapshot || selectedMapping : null;

  const selectedOperationField = useMemo(
    () => operationFields.find((item) => item.id === form.operationFieldId) || null,
    [operationFields, form.operationFieldId]
  );
  const duplicateCheck = useMappingExists({
    operationId: form.operationId,
    fieldPath: form.fieldPath,
    excludeMappingId: selectedMapping?.id,
  });
  const selectedSnapshotForm = useMemo(() => (selectedMapping ? toFormState(selectedMapping) : null), [selectedMapping, operationFields]);
  const duplicateMapping = useMemo(() => allMappings.find((item) => item.id === duplicateCheck.mappingId) || null, [allMappings, duplicateCheck.mappingId]);
  const duplicateSemanticTypeLabel = useMemo(() => {
    if (!duplicateMapping) return "";
    const display = duplicateMapping.draft_snapshot || duplicateMapping;
    return semanticTypeNames[display.semantic_type_id] || display.semantic_type_id || "";
  }, [duplicateMapping, semanticTypeNames]);
  const duplicateJumpLabel = useMemo(() => {
    if (!duplicateMapping) return "";
    const display = duplicateMapping.draft_snapshot || duplicateMapping;
    return fieldLabels[`${display.operation_id}::${display.field_path}`] || display.field_path;
  }, [duplicateMapping, fieldLabels]);
  const semanticTypeOptions = useMemo(
    () =>
      semanticTypes
        .map((item) => {
          const display = item.draft_snapshot || item;
          return {
            value: item.id,
            label: display.name,
            description: display.description || display.datatype || "",
            meta: [display.namespace || "", ...(display.aliases || [])].filter(Boolean).join(" · "),
          };
        })
        .sort((left, right) => `${left.meta || ""} ${left.label}`.localeCompare(`${right.meta || ""} ${right.label}`)),
    [semanticTypes]
  );

  const formValidationError = useMemo(() => {
    if (!form.operationFieldId || !selectedOperationField) {
      return "Select one extracted source field.";
    }
    if (!form.semanticTypeId) {
      return "Select one semantic type.";
    }
    if (duplicateCheck.exists) {
      return duplicateSemanticTypeLabel
        ? `A mapping for the selected source field already exists: ${duplicateSemanticTypeLabel}.`
        : "A mapping for the selected source field already exists.";
    }
    if (duplicateCheck.error) {
      return duplicateCheck.error;
    }
    return "";
  }, [duplicateCheck.error, duplicateCheck.exists, duplicateSemanticTypeLabel, form.operationFieldId, form.semanticTypeId, selectedOperationField]);
  const detailDirty = useMemo(() => {
    if (!selectedSnapshotForm) return false;
    return JSON.stringify(form) !== JSON.stringify(selectedSnapshotForm);
  }, [form, selectedSnapshotForm]);
  const canSubmitForm = !submitting && !duplicateCheck.loading;
  const bulkSelectedIds = useMemo(() => Object.keys(bulkSelections), [bulkSelections]);
  const bulkSelectedMappings = useMemo(() => Object.values(bulkSelections), [bulkSelections]);

  useEffect(() => {
    if (!actionMessage) {
      return;
    }
    const timeoutId = window.setTimeout(() => setActionMessage(""), 3200);
    return () => window.clearTimeout(timeoutId);
  }, [actionMessage]);

  useEffect(() => {
    if (!filteredRows.length) {
      setSelectedId("");
      return;
    }
    if (!selectedId || !filteredRows.some((item) => item.id === selectedId)) {
      setSelectedId(filteredRows[0].id);
    }
  }, [filteredRows, selectedId]);

  useEffect(() => {
    if (!bulkAssignOpen) return;

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setBulkAssignOpen(false);
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [bulkAssignOpen]);

  async function loadCrudDependencies() {
    const [nextSources, nextOperations] = await Promise.all([listExecutionSources(), listExecutionOperations()]);
    setSources(nextSources);
    setOperations(nextOperations);
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

  function toFormState(mapping = selectedMapping) {
    const display = mapping?.draft_snapshot || mapping;
    const matchedOperationField = operationFields.find(
      (item) =>
        item.operation_id === (display?.operation_id || "") &&
        String(item.field_path || item.raw_name) === (display?.field_path || "")
    );
    return {
      operationFieldId: matchedOperationField?.id || "",
      sourceId: display?.source_id || "",
      operationId: display?.operation_id || "",
      fieldPath: display?.field_path || "",
      semanticTypeId: display?.semantic_type_id || "",
      canonicalAttributeId: display?.canonical_attribute_id || "",
      mappingType: display?.mapping_type || "exact",
      mappingKind: display?.mapping_kind || "direct",
      namespace: display?.namespace || "public",
      lifecycle: display?.lifecycle || "draft",
      version: display?.version || "1.0.0",
      confidence: display?.confidence == null ? "" : String(display.confidence),
      notes: display?.notes || "",
      transformSpec: stringifyJson(display?.transform_spec || {}),
      enumMapping: stringifyJson(display?.enum_mapping || {})
    };
  }

  async function openDetail(mappingId?: string) {
    const target = allMappings.find((item) => item.id === (mappingId || selectedId));
    if (!target) return;
    setSelectedId(target.id);
    setForm(toFormState(target));
    setDetailOpen(true);
    setActionError("");
    setActionMessage("");
    await loadCrudDependencies();
  }

  async function jumpToDuplicateMapping() {
    if (!duplicateMapping) return;
    closeForm();
    setSelectedId(duplicateMapping.id);
  }

  function toggleBulkSelection(mappingId: string, checked: boolean) {
    const target = allMappings.find((item) => item.id === mappingId);
    if (!target) return;
    const display = target.draft_snapshot || target;
    setBulkSelections((current) => {
      if (!checked) {
        const next = { ...current };
        delete next[mappingId];
        return next;
      }
      return {
        ...current,
        [mappingId]: {
          id: target.id,
          fieldLabel: fieldLabels[`${display.operation_id}::${display.field_path}`] || display.field_path,
          fieldPath: display.field_path,
          sourceId: display.source_id || "",
          operationId: display.operation_id || "",
          semanticTypeId: display.semantic_type_id || "",
          notes: display.notes || "",
        },
      };
    });
  }

  function toggleAllVisibleSelections(checked: boolean) {
    setBulkSelections((current) => {
      const visibleMappedRows = pagedRows;
      if (checked) {
        const next = { ...current };
        visibleMappedRows.forEach((item) => {
          const display = item.mapping.draft_snapshot || item.mapping;
          next[item.mapping.id] = {
            id: item.mapping.id,
            fieldLabel: fieldLabels[`${display.operation_id}::${display.field_path}`] || display.field_path,
            fieldPath: display.field_path,
            sourceId: display.source_id || "",
            operationId: display.operation_id || "",
            semanticTypeId: display.semantic_type_id || "",
            notes: display.notes || "",
          };
        });
        return next;
      }
      const next = { ...current };
      visibleMappedRows.forEach((item) => {
        delete next[item.mapping.id];
      });
      return next;
    });
  }

  async function applyBulkSemanticType() {
    if (!bulkSemanticTypeId) {
      setActionError("Select one semantic type for bulk assignment.");
      return;
    }
    if (!bulkSelectedMappings.length) {
      setActionError("Select one or more mappings first.");
      return;
    }
    const targets = bulkSelectedMappings.filter((item) => item.semanticTypeId !== bulkSemanticTypeId);
    if (!targets.length) {
      setActionError("All selected mappings already use the chosen semantic type.");
      return;
    }

    setSubmitting(true);
    setActionError("");
    try {
      const results = await Promise.all(
        targets.map((item) =>
          updateMapping(item.id, {
            semantic_type_id: bulkSemanticTypeId,
            notes: item.notes || "",
          })
        )
      );
      const proposalIds = results
        .map((item) => (item as { proposal?: { id?: string } }).proposal?.id || "")
        .filter(Boolean);
      await reload();
      setBulkAssignOpen(false);
      setBulkSemanticTypeId("");
      setBulkSelections({});
      setActionLinkProposalIds(proposalIds);
      setActionMessage(
        targets.length === 1
          ? "1 mapping update proposal created."
          : `${targets.length} mapping update proposals created.`
      );
    } catch (requestError) {
      setActionError(
        requestError instanceof Error ? requestError.message : "Failed to create bulk mapping update proposals."
      );
    } finally {
      setSubmitting(false);
    }
  }

  function closeForm() {
    setDetailOpen(false);
  }

  async function submitForm(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (formValidationError) {
      setActionError(formValidationError);
      return;
    }
    setSubmitting(true);
    setActionError("");
    setActionMessage("");
    setActionLinkProposalIds([]);
    try {
      const updatePayload = {
        semantic_type_id: form.semanticTypeId,
        mapping_type: form.mappingType,
        mapping_kind: form.mappingKind,
        transform_spec: parseJsonObject(form.transformSpec, "transform_spec"),
        enum_mapping: parseJsonObject(form.enumMapping, "enum_mapping"),
        notes: form.notes.trim(),
      };
      if (selectedMapping) {
        const result = await updateMapping(selectedMapping.id, updatePayload);
        const proposalId = (result as { proposal?: { id?: string } }).proposal?.id || "";
        setActionLinkProposalIds(proposalId ? [proposalId] : []);
      }
      await reload();
      setActionMessage("Mapping update proposal created.");
      setDetailOpen(false);
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to save mapping.");
    } finally {
      setSubmitting(false);
    }
  }

  async function removeSelected() {
    if (!selectedMapping) {
      return;
    }
    setSubmitting(true);
    setActionError("");
    setActionMessage("");
    setActionLinkProposalIds([]);
    try {
      await deleteMapping(selectedMapping.id);
      await reload();
      setSelectedId("");
      setDetailOpen(false);
      setActionMessage("Mapping delete proposal created.");
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to delete mapping.");
    } finally {
      setSubmitting(false);
      setDeleteConfirmOpen(false);
    }
  }

  return (
    <SectionPlaceholder
      title="Mappings"
      description="Bind `source + operation + path` context to semantic types and canonical attributes with proposal review."
      body={
        <div className="space-y-4">
          <InfoLine>
            <Badge variant="info">{allMappings.length} mappings</Badge>
            <Badge variant="warning">{unmappedOperationFields.length} unmapped fields</Badge>
            {review !== "all" ? <Badge variant="warning">{review.replace(/_/g, " ")}</Badge> : null}
          </InfoLine>

          <FilterToolbar
            query={query}
            onQueryChange={(next) => {
              setQuery(next);
              setPage(1);
            }}
            queryPlaceholder="Search mappings by field, operation, or semantic type"
            status={review}
            statusLabel="Review"
            statusOptions={[
              { value: "all", label: "All" },
              { value: "pending_review", label: "Pending Review" },
              { value: "approved", label: "Approved" },
            ]}
            onStatusChange={(next) => {
              setReview(next);
              setPage(1);
            }}
            extra={
              bulkSelectedIds.length ? (
                <>
                  <Button type="button" variant="outline" size="sm" onClick={() => setBulkSelections({})}>
                    Clear
                  </Button>
                  <Button type="button" size="sm" onClick={() => setBulkAssignOpen(true)}>
                    Edit Semantic Type
                  </Button>
                </>
              ) : null
            }
          />

          {error ? <ErrorPanel message={error} /> : null}
          <ErrorModal open={!!actionError} message={actionError} onClose={() => setActionError("")} />
          <ConfirmModal
            open={deleteConfirmOpen}
            title="Delete Mapping"
            message="This creates a delete proposal for the selected mapping."
            confirmLabel="Create Delete Proposal"
            confirming={submitting}
            onCancel={() => setDeleteConfirmOpen(false)}
            onConfirm={() => void removeSelected()}
          />
          {loading ? <LoadingPanel message="Loading mappings..." /> : null}

          {!loading && !error ? (
            <div className="grid gap-5 xl:grid-cols-[minmax(0,1.18fr)_360px]">
              <TablePanel footer={<PaginationBar page={page} pageSize={pageSize} total={filteredRows.length} onPageChange={setPage} />}>
                <MappingsTable
                  items={pagedRows as MappingTableRow[]}
                  selectedId={selectedRow?.id || ""}
                  selectedIds={bulkSelectedIds}
                  onSelect={setSelectedId}
                  onToggleSelection={toggleBulkSelection}
                  onToggleAllVisible={toggleAllVisibleSelections}
                  onOpenProposal={(proposalId) => {
                    router.push(`/proposals?query=${encodeURIComponent(proposalId)}`);
                  }}
                />
              </TablePanel>
              <InspectorPanel>
                {selectedMapping && selectedDisplay ? (
                  <div className="space-y-4 p-4">
                    <div className="space-y-3 border-b border-border/60 pb-4">
                      <div className="text-sm font-semibold text-foreground">
                        {fieldLabels[`${selectedDisplay.operation_id}::${selectedDisplay.field_path}`] || selectedDisplay.field_path}
                      </div>
                      <div className="space-y-1 text-xs text-muted-foreground">
                        <div>{[selectedDisplay.source_id, selectedDisplay.operation_id].filter(Boolean).join(" · ")}</div>
                        <div className="font-mono text-[11px] text-muted-foreground/80">{selectedDisplay.field_path}</div>
                      </div>
                      <div className="flex items-center gap-2">
                        <Button type="button" onClick={() => void openDetail()}>
                          <Save className="h-4 w-4" />
                          Edit
                        </Button>
                        <Button type="button" variant="outline" onClick={() => setDeleteConfirmOpen(true)} disabled={submitting}>
                          <Trash2 className="h-4 w-4" />
                          Delete
                        </Button>
                      </div>
                    </div>

                    <InspectorSection title="Semantic Type">
                      <InspectorSurface muted>
                        <div className="text-sm font-medium text-foreground">
                          {semanticTypeNames[selectedDisplay.semantic_type_id] || selectedDisplay.semantic_type_id}
                        </div>
                        <div className="mt-1 truncate font-mono text-[11px] text-muted-foreground/80">
                          {selectedDisplay.semantic_type_id}
                        </div>
                      </InspectorSurface>
                    </InspectorSection>

                    {selectedDisplay.notes ? (
                      <InspectorSection title="Notes">
                        <InspectorSurface muted>
                          <p className="text-sm leading-6 text-muted-foreground">{selectedDisplay.notes}</p>
                        </InspectorSurface>
                      </InspectorSection>
                    ) : null}

                    <InspectorSection title="Review">
                      <InspectorSurface muted>
                        <dl className="grid gap-3 text-sm">
                          <div>
                            <dt className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">State</dt>
                            <dd className="mt-1">
                              <Badge
                                variant={
                                  selectedMapping.pending_proposal_id
                                    ? "warning"
                                    : selectedMapping.draft_snapshot
                                      ? "info"
                                      : "success"
                                }
                              >
                                {selectedMapping.pending_proposal_id ? "Pending Review" : selectedMapping.draft_snapshot ? "Draft" : "Approved"}
                              </Badge>
                            </dd>
                          </div>
                          {selectedMapping.pending_proposal_id ? (
                            <div>
                              <dt className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Proposal</dt>
                              <dd className="mt-1">
                                <button
                                  type="button"
                                  className="font-mono text-[11px] text-amber-700 underline-offset-2 hover:underline"
                                  onClick={() => router.push(`/proposals?query=${encodeURIComponent(selectedMapping.pending_proposal_id || "")}`)}
                                >
                                  {selectedMapping.pending_proposal_id}
                                </button>
                              </dd>
                            </div>
                          ) : null}
                          {selectedDisplay.updated_at ? (
                            <div>
                              <dt className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Updated</dt>
                              <dd className="mt-1 text-foreground">{new Date(selectedDisplay.updated_at).toLocaleString()}</dd>
                            </div>
                          ) : null}
                        </dl>
                      </InspectorSurface>
                    </InspectorSection>
                  </div>
                ) : (
                  <div className="p-4 text-sm text-muted-foreground">Select one mapping to inspect it here.</div>
                )}
              </InspectorPanel>
            </div>
          ) : null}

          {!loading && !error && !filteredRows.length ? <EmptyPanel message="No fields match the current filters." /> : null}
          <DetailDrawer
            open={detailOpen && !!selectedMapping}
            title={
              selectedDisplay
                ? fieldLabels[`${selectedDisplay.operation_id}::${selectedDisplay.field_path}`] || selectedDisplay.field_path
                : "Mapping Detail"
            }
            subtitle={
              selectedDisplay
                ? [selectedDisplay.source_id, selectedDisplay.operation_id].filter(Boolean).join(" · ") || undefined
                : undefined
            }
            onClose={() => setDetailOpen(false)}
          >
            {selectedMapping ? (
              <form id="mapping-detail-form" className="space-y-4" onSubmit={submitForm}>
                <div className="space-y-4">
                  <div className="flex items-center justify-between gap-3 border-b border-border/60 pb-3">
                    <div className="text-xs text-muted-foreground">
                      {detailDirty ? "Unsaved changes will create an update proposal." : "No unsaved changes."}
                    </div>
                    <div className="flex items-center gap-2">
                    <Button type="button" variant="outline" onClick={() => setDeleteConfirmOpen(true)} disabled={!selectedMapping || submitting}>
                      <Trash2 className="h-4 w-4" />
                      Delete
                    </Button>
                    <Button type="submit" form="mapping-detail-form" disabled={!selectedMapping || !canSubmitForm || !detailDirty}>
                      <Save className="h-4 w-4" />
                      {detailDirty ? "Save Proposal" : "No Changes"}
                    </Button>
                    </div>
                  </div>
                  <InspectorSection title="Mapping Definition">
                    <MappingFormFields
                      form={form}
                      sources={sources}
                      operations={operations}
                      operationFields={operationFields}
                      semanticTypes={semanticTypes}
                      canonicalEntities={canonicalEntities}
                      canonicalAttributes={canonicalAttributes}
                      sourceFieldLocked
                      onChange={setForm}
                      onCreateSemanticType={handleCreateSemanticType}
                      onCreateCanonicalEntity={handleCreateCanonicalEntity}
                      onCreateCanonicalAttribute={handleCreateCanonicalAttribute}
                      sourceFieldValidationMessage={duplicateCheck.exists ? (duplicateSemanticTypeLabel ? `Already mapped to ${duplicateSemanticTypeLabel}.` : "This source field already has a mapping.") : duplicateCheck.error || ""}
                      afterNotes={
                        <>
                          {(selectedMapping.pending_proposal_id ||
                            selectedDisplay?.semantic_type_id ||
                            selectedDisplay?.canonical_attribute_id ||
                            selectedDisplay?.source_id ||
                            selectedDisplay?.operation_id) ? (
                            <InspectorSection title="Context">
                              <InspectorSurface muted>
                                <dl className="grid gap-3 text-sm sm:grid-cols-2">
                                  {selectedMapping.pending_proposal_id ? (
                                    <div>
                                      <dt className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Proposal</dt>
                                      <dd className="mt-1 text-foreground">{selectedMapping.pending_proposal_id}</dd>
                                    </div>
                                  ) : null}
                                  {selectedDisplay?.semantic_type_id ? (
                                    <div>
                                      <dt className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Semantic Type</dt>
                                      <dd className="mt-1 text-foreground">{semanticTypeNames[selectedDisplay.semantic_type_id] || selectedDisplay.semantic_type_id}</dd>
                                    </div>
                                  ) : null}
                                  {selectedDisplay?.canonical_attribute_id ? (
                                    <div>
                                      <dt className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Canonical</dt>
                                      <dd className="mt-1 text-foreground">
                                        {canonicalAttributeNames[selectedDisplay.canonical_attribute_id] || selectedDisplay.canonical_attribute_id}
                                      </dd>
                                    </div>
                                  ) : null}
                                  {selectedDisplay?.source_id ? (
                                    <div>
                                      <dt className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Source</dt>
                                      <dd className="mt-1 text-foreground">{selectedDisplay.source_id}</dd>
                                    </div>
                                  ) : null}
                                  {selectedDisplay?.operation_id ? (
                                    <div>
                                      <dt className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Operation</dt>
                                      <dd className="mt-1 text-foreground">{selectedDisplay.operation_id}</dd>
                                    </div>
                                  ) : null}
                                </dl>
                              </InspectorSurface>
                            </InspectorSection>
                          ) : null}
                          <InspectorSection title="Payload Notes">
                            <InspectorSurface muted>
                              <p className="text-sm leading-6 text-muted-foreground">
                                Saving from this drawer creates an update proposal. Runtime context changes only after governance approval.
                              </p>
                            </InspectorSurface>
                          </InspectorSection>
                        </>
                      }
                    />
                  </InspectorSection>
                </div>
              </form>
            ) : null}
          </DetailDrawer>
          {bulkAssignOpen ? (
            <div className="fixed inset-0 z-[90]">
              <button
                type="button"
                aria-label="Close bulk assign dialog"
                className="absolute inset-0 bg-black/20"
                onClick={() => setBulkAssignOpen(false)}
              />
              <div className="fixed left-1/2 top-1/2 w-[min(92vw,560px)] -translate-x-1/2 -translate-y-1/2 rounded-2xl border border-border/80 bg-background p-5 shadow-2xl">
                <div className="mb-4 flex items-start justify-between gap-4 border-b border-border/70 pb-3">
                  <div>
                    <h4 className="text-sm font-semibold text-foreground">Edit Semantic Type</h4>
                    <p className="mt-1 text-xs text-muted-foreground">
                      Apply one semantic type to the selected mappings. This creates update proposals.
                    </p>
                  </div>
                  <Button type="button" variant="ghost" size="sm" onClick={() => setBulkAssignOpen(false)}>
                    Close
                  </Button>
                </div>
                <div className="space-y-4">
                  <div className="rounded-lg border border-border/70 bg-muted/20 px-3 py-2 text-sm text-muted-foreground">
                    {bulkSelectedIds.length} mappings selected
                  </div>
                  <div>
                    <div className="mb-2 text-xs font-medium uppercase tracking-[0.12em] text-muted-foreground">
                      Semantic Type
                    </div>
                    <Combobox
                      value={bulkSemanticTypeId}
                      options={semanticTypeOptions}
                      onValueChange={setBulkSemanticTypeId}
                      placeholder="Search semantic type"
                      searchPlaceholder="Search semantic types"
                      emptyLabel="No semantic types"
                    />
                  </div>
                  <div className="max-h-56 overflow-auto rounded-lg border border-border/70 bg-muted/10">
                    <div className="border-b border-border/70 px-3 py-2 text-[11px] uppercase tracking-[0.12em] text-muted-foreground">
                      Selected Mappings
                    </div>
                    <div className="divide-y divide-border/60">
                      {bulkSelectedMappings.map((item) => (
                        <div key={item.id} className="px-3 py-2.5">
                          <div className="truncate text-sm font-medium text-foreground">{item.fieldLabel}</div>
                          <div className="mt-0.5 truncate text-[11px] text-muted-foreground">
                            {[item.sourceId, item.operationId].filter(Boolean).join(" · ")}
                          </div>
                          <div className="mt-1 flex flex-wrap items-center gap-2 text-[11px] text-muted-foreground/90">
                            <span className="font-mono">{item.fieldPath}</span>
                            <span>·</span>
                            <span>{semanticTypeNames[item.semanticTypeId] || item.semanticTypeId}</span>
                            {bulkSemanticTypeId ? (
                              <>
                                <span>→</span>
                                <span className="font-medium text-foreground">
                                  {semanticTypeNames[bulkSemanticTypeId] || bulkSemanticTypeId}
                                </span>
                              </>
                            ) : null}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div className="flex justify-end gap-2 border-t border-border/70 pt-3">
                    <Button type="button" variant="outline" onClick={() => setBulkAssignOpen(false)}>
                      Cancel
                    </Button>
                    <Button type="button" onClick={() => void applyBulkSemanticType()} disabled={submitting || !bulkSemanticTypeId}>
                      Update Selected
                    </Button>
                  </div>
                </div>
              </div>
            </div>
          ) : null}
          <ActionToast
            open={!!actionMessage}
            message={actionMessage}
            actionLabel={actionLinkProposalIds.length ? `View ${actionLinkProposalIds.length} Proposals` : undefined}
            onAction={
              actionLinkProposalIds.length
                ? () => router.push(`/proposals?query=${encodeURIComponent(actionLinkProposalIds[0])}`)
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
