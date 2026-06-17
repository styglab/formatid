"use client";

import type { FormEvent } from "react";
import { useMemo, useState } from "react";
import { Pencil, Plus, RefreshCw, Trash2 } from "lucide-react";
import { createCapability, deleteCapability, updateCapability } from "@/api/semantic-admin";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { TablePanel } from "@/components/layout/table-panel";
import { InspectorPanel } from "@/components/layout/inspector-panel";
import { CapabilityForm, capabilityFormDefaults, type CapabilityFormState } from "@/components/semantic/forms/capability-form";
import { FilterToolbar } from "@/components/semantic/filters/filter-toolbar";
import { CapabilityInspector } from "@/components/semantic/inspector/capability-inspector";
import { EmptyPanel, ErrorPanel, InfoLine, LoadingPanel } from "@/components/semantic/common/state-panel";
import { PaginationBar } from "@/components/semantic/common/pagination-bar";
import { CapabilitiesTable } from "@/components/semantic/tables/capabilities-table";
import { useCapabilities } from "@/hooks/semantic/use-capabilities";
import { useSemanticRegistry } from "@/hooks/semantic/use-proposals";
import { commaList, parseJsonObject, stringifyJson } from "@/lib/semantic/forms";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

export default function CapabilitiesPage() {
  const [selectedId, setSelectedId] = useState("");
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");
  const [page, setPage] = useState(1);
  const pageSize = 12;
  const { data, loading, error, reload } = useCapabilities({ query, status, page, pageSize });
  const { semanticTypes } = useSemanticRegistry();
  const [formMode, setFormMode] = useState<"create" | "edit" | null>(null);
  const [form, setForm] = useState<CapabilityFormState>(capabilityFormDefaults);
  const [submitting, setSubmitting] = useState(false);
  const [actionError, setActionError] = useState("");
  const [actionMessage, setActionMessage] = useState("");
  const semanticTypeNames = useMemo(
    () =>
      semanticTypes.reduce<Record<string, string>>((accumulator, item) => {
        accumulator[item.id] = (item.draft_snapshot || item).name;
        return accumulator;
      }, {}),
    [semanticTypes]
  );
  const filteredCapabilities = useMemo(
    () => data.items,
    [data.items]
  );
  const selectedCapability = useMemo(
    () => filteredCapabilities.find((item) => item.id === selectedId) || filteredCapabilities[0] || null,
    [filteredCapabilities, selectedId]
  );

  function toFormState() {
    const display = selectedCapability?.draft_snapshot || selectedCapability;
    return {
      capabilityKey: display?.capability_key || "",
      namespace: display?.namespace || "public",
      name: display?.name || "",
      description: display?.description || "",
      version: display?.version || "1.0.0",
      lifecycle: display?.lifecycle || "draft",
      status: display?.status || "draft",
      inputSemanticTypes: (display?.input_semantic_types || []).join(", "),
      outputSemanticTypes: (display?.output_semantic_types || []).join(", "),
      intentSpec: stringifyJson(display?.intent_spec || {})
    };
  }

  function openCreate() {
    setForm(capabilityFormDefaults);
    setFormMode("create");
    setActionError("");
    setActionMessage("");
  }

  function openEdit() {
    if (!selectedCapability) return;
    setForm(toFormState());
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
        capability_key: form.capabilityKey.trim(),
        namespace: form.namespace.trim() || "public",
        name: form.name.trim(),
        description: form.description.trim(),
        version: form.version.trim() || "1.0.0",
        lifecycle: form.lifecycle.trim() || "draft",
        status: form.status.trim() || "draft",
        input_semantic_types: commaList(form.inputSemanticTypes),
        output_semantic_types: commaList(form.outputSemanticTypes),
        intent_spec: parseJsonObject(form.intentSpec, "intent_spec"),
        metadata: {}
      };
      if (formMode === "create") {
        await createCapability(payload);
      } else if (selectedCapability) {
        await updateCapability(selectedCapability.id, payload);
      }
      await reload();
      setActionMessage(formMode === "create" ? "Capability proposal created." : "Capability update proposal created.");
      closeForm();
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to save capability.");
    } finally {
      setSubmitting(false);
    }
  }

  async function removeSelected() {
    if (!selectedCapability || !window.confirm("This creates a delete proposal for the selected capability.")) {
      return;
    }
    setSubmitting(true);
    setActionError("");
    setActionMessage("");
    try {
      await deleteCapability(selectedCapability.id);
      await reload();
      setSelectedId("");
      setActionMessage("Capability delete proposal created.");
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to delete capability.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <SectionPlaceholder
      title="Capabilities"
      description="Define planner-facing semantic intents after source context, mappings, and execution evidence are in place."
      actions={
        <div className="flex items-center gap-2">
          <Button type="button" onClick={openCreate}>
            <Plus className="h-4 w-4" />
            New
          </Button>
          <Button type="button" variant="outline" onClick={openEdit} disabled={!selectedCapability}>
            <Pencil className="h-4 w-4" />
            Edit
          </Button>
          <Button type="button" variant="outline" onClick={() => void removeSelected()} disabled={!selectedCapability || submitting}>
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
            <Badge variant="info">{filteredCapabilities.length} capabilities</Badge>
            <Badge variant="default">{selectedCapability ? "1 selected" : "0 selected"}</Badge>
          </InfoLine>

          <FilterToolbar
            query={query}
            onQueryChange={(next) => {
              setQuery(next);
              setPage(1);
            }}
            queryPlaceholder="Search capabilities by name, key, or namespace"
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
            <CapabilityForm
              title={formMode === "create" ? "Create Capability" : "Edit Capability"}
              description={formMode === "create" ? "Create a new planner-facing capability definition directly from the agent layer." : "Update the selected capability and create an update proposal."}
              form={form}
              onChange={setForm}
              onSubmit={submitForm}
              onCancel={closeForm}
              submitLabel={formMode === "create" ? "Create Capability" : "Save Changes"}
              submitting={submitting}
            />
          ) : null}

          {loading ? <LoadingPanel message="Loading capabilities..." /> : null}

          {!loading && !error ? (
            <div className="grid gap-5 xl:grid-cols-[minmax(0,1.1fr)_minmax(380px,0.9fr)]">
              <TablePanel footer={<PaginationBar page={page} pageSize={pageSize} total={data.total} onPageChange={setPage} />}>
                <CapabilitiesTable items={filteredCapabilities} selectedId={selectedCapability?.id || ""} onSelect={setSelectedId} />
              </TablePanel>
              <InspectorPanel>
                <CapabilityInspector capability={selectedCapability} semanticTypeNames={semanticTypeNames} />
              </InspectorPanel>
            </div>
          ) : null}

          {!loading && !error && !filteredCapabilities.length ? <EmptyPanel message="No capabilities match the current filters." /> : null}
        </div>
      }
    />
  );
}
