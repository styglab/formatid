"use client";

import { useEffect, useMemo, useState } from "react";
import { Pencil, Plus, RefreshCw, Trash2 } from "lucide-react";
import {
  createCanonicalAttribute,
  createCanonicalEntity,
  createCanonicalRelation,
  deleteCanonicalAttribute,
  deleteCanonicalEntity,
  deleteCanonicalRelation,
  updateCanonicalAttribute,
  updateCanonicalEntity,
  updateCanonicalRelation,
} from "@/api/semantic-admin";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { WorkbenchSplit } from "@/components/layout/workbench-split";
import { FormField, FormGrid, FormSelect, FormShell, FormTextarea } from "@/components/semantic/forms/form-shell";
import { EmptyPanel, ErrorPanel, InfoLine, LoadingPanel } from "@/components/semantic/common/state-panel";
import { FilterToolbar } from "@/components/semantic/filters/filter-toolbar";
import { CanonicalEntityInspector } from "@/components/semantic/inspector/canonical-entity-inspector";
import { CanonicalEntitiesTable } from "@/components/semantic/tables/canonical-entities-table";
import { useMappings } from "@/hooks/semantic/use-mappings";
import { useCanonicalModel } from "@/hooks/semantic/use-canonical-model";
import { useSemanticRegistry } from "@/hooks/semantic/use-proposals";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

export default function CanonicalModelPage() {
  const { semanticTypes } = useSemanticRegistry();
  const { entities: canonicalEntities, attributes, relations, loading, error, reload } = useCanonicalModel();
  const { data: mappings } = useMappings({ page: 1, pageSize: 500 });
  const [selectedId, setSelectedId] = useState("");
  const [selectedAttributeId, setSelectedAttributeId] = useState("");
  const [selectedRelationId, setSelectedRelationId] = useState("");
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");
  const [formMode, setFormMode] = useState<"entity-create" | "entity-edit" | "attribute-create" | "attribute-edit" | "relation-create" | "relation-edit" | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [actionError, setActionError] = useState("");
  const [actionMessage, setActionMessage] = useState("");
  const [entityForm, setEntityForm] = useState({ name: "", semanticTypeId: "", description: "", status: "draft" });
  const [attributeForm, setAttributeForm] = useState({ entityId: "", semanticTypeId: "", name: "", datatype: "string", description: "", identityRole: "", status: "draft" });
  const [relationForm, setRelationForm] = useState({ sourceEntityId: "", targetEntityId: "", relationType: "related_to", forwardLabel: "", reverseLabel: "", status: "draft" });

  const entities = useMemo(
    () =>
      canonicalEntities.filter((item) => {
        const display = item.draft_snapshot || item;
        const lowered = query.toLowerCase();
        const matchesQuery =
          !query ||
          display.name.toLowerCase().includes(lowered) ||
          (display.description || "").toLowerCase().includes(lowered);
        const matchesStatus = status === "all" || (display.status || "") === status;
        return matchesQuery && matchesStatus;
      }),
    [canonicalEntities, query, status]
  );

  const attributeCounts = useMemo(
    () =>
      attributes.reduce<Record<string, number>>((accumulator, item) => {
        const parentId = (item.draft_snapshot || item).entity_id;
        if (parentId) {
          accumulator[parentId] = (accumulator[parentId] || 0) + 1;
        }
        return accumulator;
      }, {}),
    [attributes]
  );
  const selected = useMemo(
    () => entities.find((item) => item.id === selectedId) || entities[0] || null,
    [entities, selectedId]
  );
  const entityAttributes = useMemo(
    () => attributes.filter((item) => (item.draft_snapshot || item).entity_id === selected?.id),
    [attributes, selected]
  );
  const entityRelationships = useMemo(
    () => relations.filter((item) => item.source_entity_id === selected?.id || item.target_entity_id === selected?.id),
    [relations, selected]
  );
  const selectedAttribute = useMemo(
    () => entityAttributes.find((item) => item.id === selectedAttributeId) || entityAttributes[0] || null,
    [entityAttributes, selectedAttributeId]
  );
  const selectedRelation = useMemo(
    () => entityRelationships.find((item) => item.id === selectedRelationId) || entityRelationships[0] || null,
    [entityRelationships, selectedRelationId]
  );
  const mappedAttributes = useMemo(() => {
    const attributeIds = new Set(entityAttributes.map((item) => item.id));
    return mappings.items.filter((item) => item.canonical_attribute_id && attributeIds.has(item.canonical_attribute_id)).length;
  }, [entityAttributes, mappings.items]);

  useEffect(() => {
    setSelectedAttributeId("");
    setSelectedRelationId("");
  }, [selected?.id]);

  function openEntityCreate() {
    setEntityForm({ name: "", semanticTypeId: "", description: "", status: "draft" });
    setFormMode("entity-create");
  }

  function openEntityEdit() {
    if (!selected) return;
    const display = selected.draft_snapshot || selected;
    setEntityForm({
      name: display.name,
      semanticTypeId: display.semantic_type_id || "",
      description: display.description || "",
      status: display.status || "draft"
    });
    setFormMode("entity-edit");
  }

  function openAttributeCreate() {
    if (!selected) return;
    setAttributeForm({ entityId: selected.id, semanticTypeId: "", name: "", datatype: "string", description: "", identityRole: "", status: "draft" });
    setFormMode("attribute-create");
  }

  function openAttributeEdit() {
    const attribute = selectedAttribute;
    if (!attribute) return;
    const display = attribute.draft_snapshot || attribute;
    setAttributeForm({
      entityId: display.entity_id,
      semanticTypeId: display.semantic_type_id || "",
      name: display.name,
      datatype: display.datatype || "string",
      description: display.description || "",
      identityRole: display.identity_role || "",
      status: display.status || "draft"
    });
    setFormMode("attribute-edit");
  }

  function openRelationCreate() {
    if (!selected) return;
    setRelationForm({ sourceEntityId: selected.id, targetEntityId: "", relationType: "related_to", forwardLabel: "", reverseLabel: "", status: "draft" });
    setFormMode("relation-create");
  }

  function openRelationEdit() {
    if (!selectedRelation) return;
    const display = selectedRelation.draft_snapshot || selectedRelation;
    setRelationForm({
      sourceEntityId: display.source_entity_id,
      targetEntityId: display.target_entity_id,
      relationType: display.relation_type,
      forwardLabel: display.forward_label || "",
      reverseLabel: display.reverse_label || "",
      status: display.status || "draft"
    });
    setFormMode("relation-edit");
  }

  function closeForm() {
    setFormMode(null);
    setActionError("");
  }

  async function submitEntity(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    try {
      const payload = {
        name: entityForm.name.trim(),
        semantic_type_id: entityForm.semanticTypeId || null,
        description: entityForm.description.trim(),
        status: entityForm.status
      };
      if (formMode === "entity-create") {
        await createCanonicalEntity(payload);
      } else if (selected) {
        await updateCanonicalEntity(selected.id, payload);
      }
      await reload();
      setActionMessage("Canonical entity proposal created.");
      closeForm();
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to save canonical entity.");
    } finally {
      setSubmitting(false);
    }
  }

  async function submitAttribute(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    try {
      const payload = {
        entity_id: attributeForm.entityId,
        semantic_type_id: attributeForm.semanticTypeId || null,
        name: attributeForm.name.trim(),
        datatype: attributeForm.datatype,
        description: attributeForm.description.trim(),
        identity_role: attributeForm.identityRole.trim(),
        status: attributeForm.status
      };
      if (formMode === "attribute-create") {
        await createCanonicalAttribute(payload);
      } else {
        if (selectedAttribute) {
          await updateCanonicalAttribute(selectedAttribute.id, payload);
        }
      }
      await reload();
      setActionMessage("Canonical attribute proposal created.");
      closeForm();
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to save canonical attribute.");
    } finally {
      setSubmitting(false);
    }
  }

  async function submitRelation(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    try {
      const payload = {
        source_entity_id: relationForm.sourceEntityId,
        target_entity_id: relationForm.targetEntityId,
        relation_type: relationForm.relationType,
        forward_label: relationForm.forwardLabel.trim(),
        reverse_label: relationForm.reverseLabel.trim(),
        status: relationForm.status
      };
      if (formMode === "relation-create") {
        await createCanonicalRelation(payload);
      } else {
        if (selectedRelation) {
          await updateCanonicalRelation(selectedRelation.id, payload);
        }
      }
      await reload();
      setActionMessage("Canonical relation proposal created.");
      closeForm();
    } catch (requestError) {
      setActionError(requestError instanceof Error ? requestError.message : "Failed to save canonical relation.");
    } finally {
      setSubmitting(false);
    }
  }

  async function removeEntity() {
    if (!selected || !window.confirm("This creates a delete proposal for the selected canonical entity.")) return;
    await deleteCanonicalEntity(selected.id);
    await reload();
    setSelectedId("");
    setActionMessage("Canonical entity delete proposal created.");
  }

  async function removeAttribute() {
    if (!selectedAttribute || !window.confirm("This creates a delete proposal for the selected canonical attribute.")) return;
    await deleteCanonicalAttribute(selectedAttribute.id);
    await reload();
    setSelectedAttributeId("");
    setActionMessage("Canonical attribute delete proposal created.");
  }

  async function removeRelation() {
    if (!selectedRelation || !window.confirm("This creates a delete proposal for the selected canonical relation.")) return;
    await deleteCanonicalRelation(selectedRelation.id);
    await reload();
    setSelectedRelationId("");
    setActionMessage("Canonical relation delete proposal created.");
  }

  return (
    <SectionPlaceholder
      title="Canonical Model"
      description="Inspect canonical entities and attribute links used by mappings and planner-facing capability outputs."
      actions={
        <div className="flex items-center gap-2">
          <Button type="button" onClick={openEntityCreate}>
            <Plus className="h-4 w-4" />
            Entity
          </Button>
          <Button type="button" variant="outline" onClick={openAttributeCreate} disabled={!selected}>
            <Plus className="h-4 w-4" />
            Attribute
          </Button>
          <Button type="button" variant="outline" onClick={openRelationCreate} disabled={!selected}>
            <Plus className="h-4 w-4" />
            Relation
          </Button>
          <Button type="button" variant="outline" onClick={openEntityEdit} disabled={!selected}>
            <Pencil className="h-4 w-4" />
            Edit Entity
          </Button>
          <Button type="button" variant="outline" onClick={() => void removeEntity()} disabled={!selected}>
            <Trash2 className="h-4 w-4" />
            Delete Entity
          </Button>
          <Button type="button" variant="outline" onClick={openAttributeEdit} disabled={!selectedAttribute}>
            <Pencil className="h-4 w-4" />
            Edit Attribute
          </Button>
          <Button type="button" variant="outline" onClick={() => void removeAttribute()} disabled={!selectedAttribute}>
            <Trash2 className="h-4 w-4" />
            Delete Attribute
          </Button>
          <Button type="button" variant="outline" onClick={openRelationEdit} disabled={!selectedRelation}>
            <Pencil className="h-4 w-4" />
            Edit Relation
          </Button>
          <Button type="button" variant="outline" onClick={() => void removeRelation()} disabled={!selectedRelation}>
            <Trash2 className="h-4 w-4" />
            Delete Relation
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
            <Badge variant="info">{entities.length} entities</Badge>
            <Badge variant="default">{attributes.length} attributes</Badge>
            <Badge variant="default">{relations.length} relations</Badge>
            <span>This route now reads the canonical model registry directly instead of projecting from semantic types.</span>
          </InfoLine>

          <FilterToolbar
            query={query}
            onQueryChange={setQuery}
            queryPlaceholder="Search canonical entities by name or description"
            status={status}
            onStatusChange={setStatus}
          />

          {actionMessage ? <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/5 px-4 py-3 text-sm text-emerald-700">{actionMessage}</div> : null}
          {error ? <ErrorPanel message={error} /> : null}
          {actionError ? <ErrorPanel message={actionError} /> : null}

          {formMode?.startsWith("entity") ? (
            <FormShell
              title={formMode === "entity-create" ? "Create Canonical Entity" : "Edit Canonical Entity"}
              description="Canonical entities are the business object anchors that mappings and relations point to."
              onSubmit={submitEntity}
              onCancel={closeForm}
              submitLabel={formMode === "entity-create" ? "Create Entity" : "Save Changes"}
              submitting={submitting}
            >
              <FormGrid>
                <FormField label="Name">
                  <Input value={entityForm.name} onChange={(event) => setEntityForm({ ...entityForm, name: event.target.value })} required />
                </FormField>
                <FormField label="Linked Semantic Type">
                  <FormSelect value={entityForm.semanticTypeId} onChange={(event) => setEntityForm({ ...entityForm, semanticTypeId: event.target.value })}>
                    <option value="">optional</option>
                    {semanticTypes.filter((item) => (item.draft_snapshot || item).entity_kind === "entity").map((item) => (
                      <option key={item.id} value={item.id}>{(item.draft_snapshot || item).name}</option>
                    ))}
                  </FormSelect>
                </FormField>
              </FormGrid>
              <FormField label="Description">
                <FormTextarea value={entityForm.description} onChange={(event) => setEntityForm({ ...entityForm, description: event.target.value })} />
              </FormField>
            </FormShell>
          ) : null}

          {formMode?.startsWith("attribute") ? (
            <FormShell
              title={formMode === "attribute-create" ? "Create Canonical Attribute" : "Edit Canonical Attribute"}
              description="Canonical attributes belong to a canonical entity and become the target for source-context mappings."
              onSubmit={submitAttribute}
              onCancel={closeForm}
              submitLabel={formMode === "attribute-create" ? "Create Attribute" : "Save Changes"}
              submitting={submitting}
            >
              <FormGrid>
                <FormField label="Entity">
                  <FormSelect value={attributeForm.entityId} onChange={(event) => setAttributeForm({ ...attributeForm, entityId: event.target.value })}>
                    {entities.map((item) => <option key={item.id} value={item.id}>{(item.draft_snapshot || item).name}</option>)}
                  </FormSelect>
                </FormField>
                <FormField label="Linked Semantic Type">
                  <FormSelect value={attributeForm.semanticTypeId} onChange={(event) => setAttributeForm({ ...attributeForm, semanticTypeId: event.target.value })}>
                    <option value="">optional</option>
                    {semanticTypes.filter((item) => (item.draft_snapshot || item).entity_kind === "attribute").map((item) => (
                      <option key={item.id} value={item.id}>{(item.draft_snapshot || item).name}</option>
                    ))}
                  </FormSelect>
                </FormField>
                <FormField label="Name">
                  <Input value={attributeForm.name} onChange={(event) => setAttributeForm({ ...attributeForm, name: event.target.value })} required />
                </FormField>
                <FormField label="Datatype">
                  <Input value={attributeForm.datatype} onChange={(event) => setAttributeForm({ ...attributeForm, datatype: event.target.value })} />
                </FormField>
              </FormGrid>
              <FormField label="Description">
                <FormTextarea value={attributeForm.description} onChange={(event) => setAttributeForm({ ...attributeForm, description: event.target.value })} />
              </FormField>
            </FormShell>
          ) : null}

          {formMode?.startsWith("relation") ? (
            <FormShell
              title={formMode === "relation-create" ? "Create Canonical Relation" : "Edit Canonical Relation"}
              description="Canonical relations model entity-to-entity structure independent of source-specific semantics."
              onSubmit={submitRelation}
              onCancel={closeForm}
              submitLabel={formMode === "relation-create" ? "Create Relation" : "Save Changes"}
              submitting={submitting}
            >
              <FormGrid>
                <FormField label="Source Entity">
                  <FormSelect value={relationForm.sourceEntityId} onChange={(event) => setRelationForm({ ...relationForm, sourceEntityId: event.target.value })}>
                    {entities.map((item) => <option key={item.id} value={item.id}>{(item.draft_snapshot || item).name}</option>)}
                  </FormSelect>
                </FormField>
                <FormField label="Target Entity">
                  <FormSelect value={relationForm.targetEntityId} onChange={(event) => setRelationForm({ ...relationForm, targetEntityId: event.target.value })}>
                    <option value="">select entity</option>
                    {entities.map((item) => <option key={item.id} value={item.id}>{(item.draft_snapshot || item).name}</option>)}
                  </FormSelect>
                </FormField>
                <FormField label="Relation Type">
                  <FormSelect value={relationForm.relationType} onChange={(event) => setRelationForm({ ...relationForm, relationType: event.target.value })}>
                    <option value="related_to">related_to</option>
                    <option value="contains">contains</option>
                    <option value="belongs_to">belongs_to</option>
                    <option value="references">references</option>
                    <option value="issued_by">issued_by</option>
                    <option value="awarded_to">awarded_to</option>
                  </FormSelect>
                </FormField>
                <FormField label="Forward Label">
                  <Input value={relationForm.forwardLabel} onChange={(event) => setRelationForm({ ...relationForm, forwardLabel: event.target.value })} />
                </FormField>
                <FormField label="Reverse Label">
                  <Input value={relationForm.reverseLabel} onChange={(event) => setRelationForm({ ...relationForm, reverseLabel: event.target.value })} />
                </FormField>
              </FormGrid>
            </FormShell>
          ) : null}

          {loading ? <LoadingPanel message="Loading canonical model..." /> : null}

          {!loading && !error ? (
            <WorkbenchSplit
              className="xl:grid-cols-[minmax(0,1.05fr)_minmax(420px,0.95fr)]"
              list={<CanonicalEntitiesTable items={entities} selectedId={selected?.id || ""} onSelect={setSelectedId} attributeCounts={attributeCounts} />}
              detail={
                <CanonicalEntityInspector
                  entity={selected}
                  attributes={entityAttributes}
                  relationships={entityRelationships}
                  mappedAttributes={mappedAttributes}
                  selectedAttributeId={selectedAttribute?.id || ""}
                  onSelectAttribute={setSelectedAttributeId}
                  selectedRelationId={selectedRelation?.id || ""}
                  onSelectRelation={setSelectedRelationId}
                />
              }
            />
          ) : null}

          {!loading && !error && !entities.length ? <EmptyPanel message="No canonical entities match the current filters." /> : null}
        </div>
      }
    />
  );
}
