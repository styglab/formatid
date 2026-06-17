"use client";

import type { FormEvent } from "react";
import { useMemo, useState } from "react";
import { Pencil, Plus, RefreshCw, Trash2 } from "lucide-react";
import { createSemanticType, deleteSemanticType, updateSemanticType } from "@/api/semantic-admin";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { WorkbenchSplit } from "@/components/layout/workbench-split";
import { SemanticTypeForm, semanticTypeFormDefaults, type SemanticTypeFormState } from "@/components/semantic/forms/semantic-type-form";
import { MetaCard } from "@/components/semantic/common/meta-card";
import { EmptyPanel, ErrorPanel, InfoLine, LoadingPanel } from "@/components/semantic/common/state-panel";
import { FilterToolbar } from "@/components/semantic/filters/filter-toolbar";
import { SemanticTypeInspector } from "@/components/semantic/inspector/semantic-type-inspector";
import { SemanticTypesTable } from "@/components/semantic/tables/semantic-types-table";
import { useSemanticRegistry } from "@/hooks/semantic/use-proposals";
import { commaList } from "@/lib/semantic/forms";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

export default function SemanticTypesPage() {
  const { semanticTypes, relationships, loading, error, reload } = useSemanticRegistry();
  const [selectedId, setSelectedId] = useState("");
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");
  const [formMode, setFormMode] = useState<"create" | "edit" | null>(null);
  const [form, setForm] = useState<SemanticTypeFormState>(semanticTypeFormDefaults);
  const [submitting, setSubmitting] = useState(false);
  const [actionError, setActionError] = useState("");
  const [actionMessage, setActionMessage] = useState("");

  const filtered = useMemo(
    () =>
      semanticTypes.filter((item) => {
        const display = item.draft_snapshot || item;
        const lowered = query.toLowerCase();
        const matchesQuery =
          !query ||
          display.name.toLowerCase().includes(lowered) ||
          (display.description || "").toLowerCase().includes(lowered) ||
          String(display.parent_entity_name || "").toLowerCase().includes(lowered);
        const matchesStatus = status === "all" || (display.status || "") === status;
        return matchesQuery && matchesStatus;
      }),
    [semanticTypes, query, status]
  );
  const selected = useMemo(
    () => filtered.find((item) => item.id === selectedId) || filtered[0] || null,
    [filtered, selectedId]
  );
  const linkedRelationships = useMemo(
    () => relationships.filter((item) => item.source_id === selected?.id || item.target_id === selected?.id),
    [relationships, selected]
  );
  const entityOptions = useMemo(
    () => semanticTypes.filter((item) => (item.draft_snapshot || item).entity_kind === "entity"),
    [semanticTypes]
  );

  function toFormState() {
    const display = selected?.draft_snapshot || selected;
    return {
      name: display?.name || "",
      description: display?.description || "",
      datatype: display?.datatype || "string",
      entityKind: display?.entity_kind || "entity",
      parentEntityId: display?.parent_entity_id || "",
      aliases: (display?.aliases || []).join(", "),
      owners: (display?.owners || []).join(", "),
      status: display?.status || "draft"
    };
  }

  function openCreate() {
    setForm(semanticTypeFormDefaults);
    setFormMode("create");
    setActionError("");
    setActionMessage("");
  }

  function openEdit() {
    if (!selected) return;
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
        name: form.name.trim(),
        description: form.description.trim(),
        datatype: form.datatype.trim() || "string",
        entity_kind: form.entityKind,
        parent_entity_id: form.entityKind === "attribute" ? form.parentEntityId : "",
        semantic_role: "",
        aliases: commaList(form.aliases),
        owners: commaList(form.owners),
        status: form.status
      };
      if (formMode === "create") {
        await createSemanticType(payload);
      } else if (selected) {
        await updateSemanticType(selected.id, payload);
      }
      await reload();
      setActionMessage(formMode === "create" ? "Semantic type proposal created." : "Semantic type update proposal created.");
      closeForm();
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to save semantic type.");
    } finally {
      setSubmitting(false);
    }
  }

  async function removeSelected() {
    if (!selected || !window.confirm("This creates a delete proposal for the selected semantic type.")) {
      return;
    }
    setSubmitting(true);
    setActionError("");
    setActionMessage("");
    try {
      await deleteSemanticType(selected.id);
      await reload();
      setSelectedId("");
      setActionMessage("Semantic type delete proposal created.");
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to delete semantic type.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <SectionPlaceholder
      title="Semantic Types"
      description="Manage versioned meaning units, aliases, and parent links before canonical modeling and mapping review."
      actions={
        <div className="flex items-center gap-2">
          <Button type="button" onClick={openCreate}>
            <Plus className="h-4 w-4" />
            New
          </Button>
          <Button type="button" variant="outline" onClick={openEdit} disabled={!selected}>
            <Pencil className="h-4 w-4" />
            Edit
          </Button>
          <Button type="button" variant="outline" onClick={() => void removeSelected()} disabled={!selected || submitting}>
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
            <Badge variant="info">{filtered.length} semantic types</Badge>
            <Badge variant="default">{selected ? "1 selected" : "0 selected"}</Badge>
          </InfoLine>

          <FilterToolbar
            query={query}
            onQueryChange={setQuery}
            queryPlaceholder="Search semantic types by name, description, or parent entity"
            status={status}
            onStatusChange={setStatus}
          />

          {actionMessage ? <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/5 px-4 py-3 text-sm text-emerald-700">{actionMessage}</div> : null}
          {actionError ? <ErrorPanel message={actionError} /> : null}
          {error ? <ErrorPanel message={error} /> : null}
          {formMode ? (
            <SemanticTypeForm
              title={formMode === "create" ? "Create Semantic Type" : "Edit Semantic Type"}
              description={formMode === "create" ? "Create a new semantic meaning unit or canonical entity/attribute anchor." : "Update the selected semantic type and create an update proposal."}
              form={form}
              entities={entityOptions}
              onChange={setForm}
              onSubmit={submitForm}
              onCancel={closeForm}
              submitLabel={formMode === "create" ? "Create Semantic Type" : "Save Changes"}
              submitting={submitting}
            />
          ) : null}
          {loading ? <LoadingPanel message="Loading semantic types..." /> : null}

          {!loading && !error ? (
            <WorkbenchSplit
              className="xl:grid-cols-[minmax(0,1.05fr)_minmax(420px,0.95fr)]"
              list={
                <div className="space-y-4 p-4">
                  <div className="grid gap-3 sm:grid-cols-3">
                    <MetaCard label="Entities" value={String(filtered.filter((item) => (item.draft_snapshot || item).entity_kind === "entity").length)} />
                    <MetaCard label="Attributes" value={String(filtered.filter((item) => (item.draft_snapshot || item).entity_kind === "attribute").length)} />
                    <MetaCard label="Draft Linked" value={String(filtered.filter((item) => item.draft_snapshot).length)} />
                  </div>
                  <SemanticTypesTable items={filtered} selectedId={selected?.id || ""} onSelect={setSelectedId} />
                </div>
              }
              detail={<SemanticTypeInspector semanticType={selected} linkedRelationships={linkedRelationships} />}
            />
          ) : null}

          {!loading && !error && !filtered.length ? <EmptyPanel message="No semantic types match the current filters." /> : null}
        </div>
      }
    />
  );
}
