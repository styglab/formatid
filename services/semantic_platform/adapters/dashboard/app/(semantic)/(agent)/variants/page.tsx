"use client";

import type { FormEvent } from "react";
import { useMemo, useState } from "react";
import { Pencil, Plus, RefreshCw, Trash2 } from "lucide-react";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { TablePanel } from "@/components/layout/table-panel";
import { InspectorPanel } from "@/components/layout/inspector-panel";
import { FilterToolbar } from "@/components/semantic/filters/filter-toolbar";
import { VariantInspector } from "@/components/semantic/inspector/variant-inspector";
import { FormField, FormGrid, FormShell, FormTextarea } from "@/components/semantic/forms/form-shell";
import { EmptyPanel, ErrorPanel, InfoLine, LoadingPanel } from "@/components/semantic/common/state-panel";
import { PaginationBar } from "@/components/semantic/common/pagination-bar";
import { VariantsTable } from "@/components/semantic/tables/variants-table";
import { createOperationVariant, deleteOperationVariant, listExecutionOperations, updateOperationVariant } from "@/api/semantic-admin";
import { useCapabilities } from "@/hooks/semantic/use-capabilities";
import { useOperationVariants, useOperations } from "@/hooks/semantic/use-operations";
import { parseJsonObject, stringifyJson } from "@/lib/semantic/forms";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { FormSelect } from "@/components/semantic/forms/form-shell";
import type { ExecutionOperation } from "@/types/semantic";

export default function VariantsPage() {
  const [selectedId, setSelectedId] = useState("");
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");
  const [page, setPage] = useState(1);
  const pageSize = 12;
  const { data, loading, error, reload } = useOperationVariants({ query, status, page, pageSize });
  const { data: operations } = useOperations();
  const { data: capabilities } = useCapabilities();
  const [formMode, setFormMode] = useState<"create" | "edit" | null>(null);
  const [operationOptions, setOperationOptions] = useState<ExecutionOperation[]>([]);
  const [submitting, setSubmitting] = useState(false);
  const [actionError, setActionError] = useState("");
  const [actionMessage, setActionMessage] = useState("");
  const [form, setForm] = useState({
    operationId: "",
    variantKey: "",
    name: "",
    description: "",
    version: "1.0.0",
    lifecycle: "draft",
    status: "draft",
    fixedSemanticArguments: "",
    fixedRawArguments: "",
    metadata: ""
  });

  const filteredVariants = useMemo(
    () => data.items,
    [data.items]
  );

  const selectedVariant = useMemo(
    () => filteredVariants.find((item) => item.id === selectedId) || filteredVariants[0] || null,
    [filteredVariants, selectedId]
  );

  async function loadOperations() {
    const nextOperations = await listExecutionOperations();
    setOperationOptions(nextOperations);
    return nextOperations;
  }

  async function openCreate() {
    const nextOperations = await loadOperations();
    setForm({
      operationId: nextOperations[0]?.id || operations.items[0]?.id || "",
      variantKey: "",
      name: "",
      description: "",
      version: "1.0.0",
      lifecycle: "draft",
      status: "draft",
      fixedSemanticArguments: "",
      fixedRawArguments: "",
      metadata: ""
    });
    setFormMode("create");
    setActionError("");
    setActionMessage("");
  }

  async function openEdit() {
    if (!selectedVariant) return;
    await loadOperations();
    const display = selectedVariant.draft_snapshot || selectedVariant;
    setForm({
      operationId: display.operation_id,
      variantKey: display.variant_key,
      name: display.name,
      description: display.description || "",
      version: display.version || "1.0.0",
      lifecycle: display.lifecycle || "draft",
      status: display.status || "draft",
      fixedSemanticArguments: stringifyJson(display.fixed_semantic_arguments || {}),
      fixedRawArguments: stringifyJson(display.fixed_raw_arguments || {}),
      metadata: stringifyJson(display.metadata || {})
    });
    setFormMode("edit");
    setActionError("");
    setActionMessage("");
  }

  function closeForm() {
    setFormMode(null);
  }

  async function submitForm(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setActionError("");
    setActionMessage("");
    try {
      const payload = {
        operation_id: form.operationId,
        variant_key: form.variantKey.trim(),
        name: form.name.trim(),
        description: form.description.trim(),
        version: form.version.trim() || "1.0.0",
        lifecycle: form.lifecycle.trim() || "draft",
        status: form.status.trim() || "draft",
        fixed_semantic_arguments: parseJsonObject(form.fixedSemanticArguments, "fixed_semantic_arguments"),
        fixed_raw_arguments: parseJsonObject(form.fixedRawArguments, "fixed_raw_arguments"),
        metadata: parseJsonObject(form.metadata, "metadata")
      };
      if (formMode === "create") {
        await createOperationVariant(payload);
      } else if (selectedVariant) {
        await updateOperationVariant(selectedVariant.id, payload);
      }
      await reload();
      setActionMessage(formMode === "create" ? "Variant proposal created." : "Variant update proposal created.");
      closeForm();
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to save variant.");
    } finally {
      setSubmitting(false);
    }
  }

  async function removeSelected() {
    if (!selectedVariant || !window.confirm("This creates a delete proposal for the selected variant.")) return;
    setSubmitting(true);
    setActionError("");
    setActionMessage("");
    try {
      await deleteOperationVariant(selectedVariant.id);
      await reload();
      setSelectedId("");
      setActionMessage("Variant delete proposal created.");
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to delete variant.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <SectionPlaceholder
      title="Variants"
      description="Review operation meaning splits, fixed semantic controls, and operation-local raw controls before planner use."
      actions={
        <div className="flex items-center gap-2">
          <Button type="button" onClick={() => void openCreate()}>
            <Plus className="h-4 w-4" />
            New
          </Button>
          <Button type="button" variant="outline" onClick={() => void openEdit()} disabled={!selectedVariant}>
            <Pencil className="h-4 w-4" />
            Edit
          </Button>
          <Button type="button" variant="outline" onClick={() => void removeSelected()} disabled={!selectedVariant || submitting}>
            <Trash2 className="h-4 w-4" />
            Delete
          </Button>
          <Button type="button" variant="outline" onClick={() => void reload()} disabled={loading}>
            <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
        </div>
      }
      body={
        <div className="space-y-4">
          <InfoLine>
            <Badge variant="info">{filteredVariants.length} variants</Badge>
            <Badge variant="default">{selectedVariant ? "1 selected" : "0 selected"}</Badge>
          </InfoLine>

          <FilterToolbar
            query={query}
            onQueryChange={(next) => {
              setQuery(next);
              setPage(1);
            }}
            queryPlaceholder="Search variants by name, key, operation, or source"
            status={status}
            onStatusChange={(next) => {
              setStatus(next);
              setPage(1);
            }}
          />

          {actionMessage ? <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/5 px-4 py-3 text-sm text-emerald-700">{actionMessage}</div> : null}
          {actionError ? <ErrorPanel message={actionError} /> : null}
          {error ? <ErrorPanel message={error} /> : null}
          {formMode ? (
            <FormShell
              title={formMode === "create" ? "Create Variant" : "Edit Variant"}
              description="Model an operation-local meaning split as a reviewed variant with fixed semantic and raw controls."
              onSubmit={submitForm}
              onCancel={closeForm}
              submitLabel={formMode === "create" ? "Create Variant" : "Save Changes"}
              submitting={submitting}
            >
              <FormGrid>
                <FormField label="Operation">
                  <FormSelect value={form.operationId} onChange={(event) => setForm({ ...form, operationId: event.target.value })}>
                    <option value="">select operation</option>
                    {operationOptions.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}
                  </FormSelect>
                </FormField>
                <FormField label="Variant Key">
                  <Input value={form.variantKey} onChange={(event) => setForm({ ...form, variantKey: event.target.value })} required />
                </FormField>
                <FormField label="Name">
                  <Input value={form.name} onChange={(event) => setForm({ ...form, name: event.target.value })} required />
                </FormField>
                <FormField label="Status">
                  <Input value={form.status} onChange={(event) => setForm({ ...form, status: event.target.value })} />
                </FormField>
              </FormGrid>
              <FormField label="Description">
                <FormTextarea value={form.description} onChange={(event) => setForm({ ...form, description: event.target.value })} />
              </FormField>
              <FormGrid>
                <FormField label="Fixed Semantic Arguments">
                  <FormTextarea value={form.fixedSemanticArguments} onChange={(event) => setForm({ ...form, fixedSemanticArguments: event.target.value })} placeholder='{"inquiry_basis":"contract_date"}' />
                </FormField>
                <FormField label="Fixed Raw Arguments">
                  <FormTextarea value={form.fixedRawArguments} onChange={(event) => setForm({ ...form, fixedRawArguments: event.target.value })} placeholder='{"inqryDiv":"1"}' />
                </FormField>
              </FormGrid>
              <FormField label="Metadata">
                <FormTextarea
                  value={form.metadata}
                  onChange={(event) => setForm({ ...form, metadata: event.target.value })}
                  placeholder='{"capability_key":"search_contracts","capability_id":"optional-capability-id"}'
                />
              </FormField>
            </FormShell>
          ) : null}
          {loading ? <LoadingPanel message="Loading operation variants..." /> : null}

          {!loading && !error ? (
            <div className="grid gap-5 xl:grid-cols-[minmax(0,1.05fr)_minmax(420px,0.95fr)]">
              <TablePanel footer={<PaginationBar page={page} pageSize={pageSize} total={data.total} onPageChange={setPage} />}>
                <VariantsTable items={filteredVariants} selectedId={selectedVariant?.id || ""} onSelect={setSelectedId} capabilities={capabilities.items} />
              </TablePanel>
              <InspectorPanel>
                <VariantInspector variant={selectedVariant} operations={operations.items} capabilities={capabilities.items} />
              </InspectorPanel>
            </div>
          ) : null}

          {!loading && !error && !filteredVariants.length ? <EmptyPanel message="No operation variants match the current filters." /> : null}
        </div>
      }
    />
  );
}
