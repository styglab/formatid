"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Background,
  Controls,
  MarkerType,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  type Edge,
  type Node,
} from "@xyflow/react";
import {
  Boxes,
  Braces,
  CheckCircle2,
  Database,
  GitBranch,
  ListTree,
  Network,
  RefreshCw,
  Search,
  Tags,
} from "lucide-react";
import {
  getCanonicalModelLinkml,
  listCanonicalClassSlotUsages,
  listCanonicalClasses,
  listCanonicalEnums,
  listCanonicalRelations,
  listCanonicalSlots,
  listCanonicalTypes,
} from "@/api/context-admin";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type {
  CanonicalClass,
  CanonicalClassSlotUsage,
  CanonicalEnum,
  CanonicalRelation,
  CanonicalSlot,
  CanonicalType,
} from "@/types/context";

type CanonicalTab = "classes" | "relations" | "graph" | "linkml";
type ClassKind = "business_entity" | "reference_value" | "reference_scheme" | "value_object" | "context_object" | "abstract";

type CanonicalSnapshot = {
  types: CanonicalType[];
  enums: CanonicalEnum[];
  slots: CanonicalSlot[];
  classes: CanonicalClass[];
  classSlotUsages: CanonicalClassSlotUsage[];
  relations: CanonicalRelation[];
  linkml: Record<string, unknown>;
};

const emptySnapshot: CanonicalSnapshot = {
  types: [],
  enums: [],
  slots: [],
  classes: [],
  classSlotUsages: [],
  relations: [],
  linkml: {},
};

export default function CanonicalModelPage() {
  const [snapshot, setSnapshot] = useState<CanonicalSnapshot>(emptySnapshot);
  const [selectedClassId, setSelectedClassId] = useState("");
  const [query, setQuery] = useState("");
  const [activeTab, setActiveTab] = useState<CanonicalTab>("classes");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const reload = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const [types, enums, slots, classes, classSlotUsages, relations, linkml] = await Promise.all([
        listCanonicalTypes(),
        listCanonicalEnums(),
        listCanonicalSlots(),
        listCanonicalClasses(),
        listCanonicalClassSlotUsages(),
        listCanonicalRelations(),
        getCanonicalModelLinkml(),
      ]);
      setSnapshot({ types, enums, slots, classes, classSlotUsages, relations, linkml });
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load Canonical Model.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void reload();
  }, [reload]);

  const slotById = useMemo(() => new Map(snapshot.slots.map((slot) => [slot.id, slot])), [snapshot.slots]);
  const classById = useMemo(() => new Map(snapshot.classes.map((item) => [item.id, item])), [snapshot.classes]);

  const usagesByClassId = useMemo(() => {
    const grouped = new Map<string, CanonicalClassSlotUsage[]>();
    for (const usage of snapshot.classSlotUsages) {
      const current = grouped.get(usage.class_id) || [];
      current.push(usage);
      grouped.set(usage.class_id, current);
    }
    return grouped;
  }, [snapshot.classSlotUsages]);

  const relationsByClassId = useMemo(() => {
    const grouped = new Map<string, CanonicalRelation[]>();
    for (const relation of snapshot.relations) {
      const source = grouped.get(relation.source_class_id) || [];
      source.push(relation);
      grouped.set(relation.source_class_id, source);
      const target = grouped.get(relation.target_class_id) || [];
      target.push(relation);
      grouped.set(relation.target_class_id, target);
    }
    return grouped;
  }, [snapshot.relations]);

  const inheritanceEdges = useMemo(() => {
    const byName = new Map(snapshot.classes.map((item) => [item.name, item]));
    return snapshot.classes
      .map((canonicalClass) => {
        const parentName = metadataString(canonicalClass.metadata, "is_a");
        const parent = parentName ? byName.get(parentName) : undefined;
        return parent ? { source: canonicalClass, target: parent } : null;
      })
      .filter(Boolean) as Array<{ source: CanonicalClass; target: CanonicalClass }>;
  }, [snapshot.classes]);

  const normalizedQuery = query.trim().toLowerCase();
  const filteredClasses = useMemo(() => {
    const classes = [...snapshot.classes].sort((left, right) => left.name.localeCompare(right.name));
    if (!normalizedQuery) return classes;
    return classes.filter((canonicalClass) => {
      const classText = `${canonicalClass.name} ${canonicalClass.description || ""} ${metadataString(canonicalClass.metadata, "is_a")} ${classKindLabel(classKind(canonicalClass))}`.toLowerCase();
      const usageText = (usagesByClassId.get(canonicalClass.id) || [])
        .map((usage) => {
          const slot = usage.canonical_slot_id ? slotById.get(usage.canonical_slot_id) : null;
          return `${usage.name} ${usage.description || ""} ${slot?.name || ""} ${slot?.aliases?.join(" ") || ""}`;
        })
        .join(" ")
        .toLowerCase();
      const relationText = (relationsByClassId.get(canonicalClass.id) || [])
        .map((relation) => `${relation.relation_type} ${relation.source_class_name || ""} ${relation.target_class_name || ""}`)
        .join(" ")
        .toLowerCase();
      return classText.includes(normalizedQuery) || usageText.includes(normalizedQuery) || relationText.includes(normalizedQuery);
    });
  }, [normalizedQuery, relationsByClassId, slotById, snapshot.classes, usagesByClassId]);

  const selectedClass =
    snapshot.classes.find((canonicalClass) => canonicalClass.id === selectedClassId) ||
    filteredClasses[0] ||
    null;
  const selectedUsages = selectedClass ? usagesByClassId.get(selectedClass.id) || [] : [];
  const selectedRelations = selectedClass ? relationsByClassId.get(selectedClass.id) || [] : [];
  const approvedClasses = snapshot.classes.filter((item) => item.status === "approved" || item.status === "published").length;
  const reviewedSlots = snapshot.slots.filter((item) => item.status === "approved" || item.status === "published").length;
  const kindCounts = useMemo(() => countByClassKind(snapshot.classes), [snapshot.classes]);

  const graph = useMemo(
    () => buildGraph({
      classes: filteredClasses,
      usagesByClassId,
      relations: snapshot.relations,
      inheritanceEdges,
      selectedClassId: selectedClass?.id || "",
    }),
    [filteredClasses, inheritanceEdges, selectedClass?.id, snapshot.relations, usagesByClassId],
  );

  const tabs: Array<{ key: CanonicalTab; label: string; icon: typeof Boxes }> = [
    { key: "classes", label: "Classes", icon: Boxes },
    { key: "relations", label: "Relations", icon: GitBranch },
    { key: "graph", label: "Graph", icon: Network },
    { key: "linkml", label: "LinkML", icon: Braces },
  ];

  return (
    <div className="mx-auto flex max-w-[1480px] flex-col gap-5">
      <section className="flex flex-col gap-4 border-b border-border pb-5 lg:flex-row lg:items-end lg:justify-between">
        <div className="min-w-0">
          <div className="flex items-center gap-2 text-xs uppercase tracking-[0.12em] text-muted-foreground">
            <Boxes className="h-3.5 w-3.5" />
            Representation Model
          </div>
          <h1 className="mt-2 text-2xl font-semibold text-foreground">Representation browser</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Browse object types, property types, representation templates, validation schemas, and the LinkML export.
          </p>
        </div>
        <Button type="button" variant="outline" onClick={() => void reload()} disabled={loading}>
          <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </section>

      {error ? <Notice message={error} /> : null}

      <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
        <Metric icon={Database} label="Object Types" value={snapshot.classes.length} detail={`${approvedClasses} active`} />
        <Metric icon={ListTree} label="Representations" value={snapshot.classSlotUsages.length} detail="concept-to-structure templates" />
        <Metric icon={GitBranch} label="Link Types" value={snapshot.relations.length} detail="object-to-object edges" />
        <Metric icon={Tags} label="Business / Ref" value={`${kindCounts.business_entity} / ${kindCounts.reference_value + kindCounts.reference_scheme}`} detail="primary and governed values" />
        <Metric icon={Braces} label="Types / Enums" value={`${snapshot.types.length} / ${snapshot.enums.length}`} detail="validation ranges" />
      </section>

      <section className="flex flex-col gap-3 border-b border-border pb-3 lg:flex-row lg:items-center lg:justify-between">
        <div className="flex flex-wrap gap-2">
          {tabs.map((tab) => {
            const Icon = tab.icon;
            const active = activeTab === tab.key;
            return (
              <button
                key={tab.key}
                type="button"
                onClick={() => setActiveTab(tab.key)}
                className={`inline-flex h-9 items-center gap-2 rounded-md border px-3 text-sm transition ${
                  active ? "border-primary/45 bg-primary/[0.08] text-foreground" : "border-border bg-card text-muted-foreground hover:bg-muted/30"
                }`}
              >
                <Icon className="h-4 w-4" />
                {tab.label}
              </button>
            );
          })}
        </div>
        <label className="flex h-9 min-w-0 items-center gap-2 rounded-md border border-border bg-background px-3 lg:w-[340px]">
          <Search className="h-4 w-4 text-muted-foreground" />
          <input
            className="min-w-0 flex-1 bg-transparent text-sm outline-none placeholder:text-muted-foreground"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search classes, fields, relations"
          />
        </label>
      </section>

      {activeTab === "classes" ? (
        <ClassesTab
          classes={filteredClasses}
          selectedClass={selectedClass}
          selectedUsages={selectedUsages}
          slotById={slotById}
          loading={loading}
          onSelectClass={setSelectedClassId}
        />
      ) : null}

      {activeTab === "relations" ? (
        <RelationsTab
          classes={snapshot.classes}
          relations={snapshot.relations}
          inheritanceEdges={inheritanceEdges}
          query={normalizedQuery}
          classById={classById}
        />
      ) : null}

      {activeTab === "graph" ? (
        <GraphTab
          graph={graph}
          selectedClass={selectedClass}
          selectedUsages={selectedUsages}
          selectedRelations={selectedRelations}
          slotById={slotById}
          onSelectClass={setSelectedClassId}
        />
      ) : null}

      {activeTab === "linkml" ? <LinkmlTab linkml={snapshot.linkml} /> : null}
    </div>
  );
}

function ClassesTab({
  classes,
  selectedClass,
  selectedUsages,
  slotById,
  loading,
  onSelectClass,
}: {
  classes: CanonicalClass[];
  selectedClass: CanonicalClass | null;
  selectedUsages: CanonicalClassSlotUsage[];
  slotById: Map<string, CanonicalSlot>;
  loading: boolean;
  onSelectClass: (classId: string) => void;
}) {
  return (
    <>
      <section className="grid gap-5 xl:grid-cols-[380px_minmax(0,1fr)]">
        <Card>
          <CardHeader>
            <div>
              <CardTitle>Model Objects</CardTitle>
              <CardDescription>Grouped by business, reference, value, and context roles.</CardDescription>
            </div>
            <Boxes className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <ClassList classes={classes} selectedClass={selectedClass} onSelectClass={onSelectClass} empty={loading ? "Loading classes..." : "No classes match this search."} />
          </CardContent>
        </Card>

        <ClassDetail selectedClass={selectedClass} selectedUsages={selectedUsages} slotById={slotById} loading={loading} />
      </section>
    </>
  );
}

function ClassList({
  classes,
  selectedClass,
  onSelectClass,
  empty,
}: {
  classes: CanonicalClass[];
  selectedClass: CanonicalClass | null;
  onSelectClass: (classId: string) => void;
  empty: string;
}) {
  const sections = classKindSections
    .map((section) => ({
      ...section,
      classes: classes.filter((canonicalClass) => classKindGroup(classKind(canonicalClass)) === section.key),
    }))
    .filter((section) => section.classes.length);

  return (
    <div className="max-h-[42rem] space-y-2 overflow-auto pr-1">
      {sections.length ? (
        sections.map((section) => (
          <div key={section.key} className="space-y-2">
            <div className="flex items-center justify-between px-1 pt-2">
              <div className="text-xs font-medium uppercase tracking-[0.12em] text-muted-foreground">{section.label}</div>
              <div className="text-xs text-muted-foreground">{section.classes.length}</div>
            </div>
            {section.classes.map((canonicalClass) => {
              const selected = canonicalClass.id === selectedClass?.id;
              const kind = classKind(canonicalClass);
              return (
                <button
                  key={canonicalClass.id}
                  type="button"
                  onClick={() => onSelectClass(canonicalClass.id)}
                  className={`w-full rounded-lg border px-3 py-2.5 text-left transition ${
                    selected ? "border-primary/35 bg-primary/[0.08]" : "border-border hover:bg-muted/25"
                  }`}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <div className="flex min-w-0 items-center gap-2">
                        <div className="truncate text-sm font-medium text-foreground">{canonicalClass.name}</div>
                        <KindBadge kind={kind} />
                      </div>
                      <div className="mt-1 truncate text-xs text-muted-foreground">{metadataString(canonicalClass.metadata, "is_a") || "root class"}</div>
                    </div>
                    <StatusBadge status={canonicalClass.status} />
                  </div>
                </button>
              );
            })}
          </div>
        ))
      ) : (
        <EmptyState message={empty} />
      )}
    </div>
  );
}

function ClassDetail({
  selectedClass,
  selectedUsages,
  slotById,
  loading,
}: {
  selectedClass: CanonicalClass | null;
  selectedUsages: CanonicalClassSlotUsage[];
  slotById: Map<string, CanonicalSlot>;
  loading: boolean;
}) {
  const viewFields = selectedClass ? canonicalViewFields(selectedClass) : [];
  return (
    <Card>
      <CardHeader>
        <div className="min-w-0">
          <CardTitle className="truncate">{selectedClass?.name || "Class Detail"}</CardTitle>
          <CardDescription>{selectedClass ? `${classKindLabel(classKind(selectedClass))}${metadataString(selectedClass.metadata, "is_a") ? ` · is_a ${metadataString(selectedClass.metadata, "is_a")}` : ""}` : "Select a class to inspect its canonical fields."}</CardDescription>
        </div>
        <div className="flex items-center gap-2">
          {selectedClass ? <KindBadge kind={classKind(selectedClass)} /> : null}
          <StatusBadge status={selectedClass?.status} />
        </div>
      </CardHeader>
      <CardContent>
        {selectedClass ? (
          <div className="space-y-4">
            {viewFields.length ? (
              <div className="rounded-lg border border-border bg-muted/20 p-3">
                <div className="mb-2 flex items-center justify-between gap-3">
                  <div className="text-xs font-medium uppercase tracking-[0.12em] text-muted-foreground">View Fields</div>
                  <Badge variant="info">Convenience</Badge>
                </div>
                <div className="grid gap-2 md:grid-cols-2">
                  {viewFields.map((field) => (
                    <div key={field.field_key} className="rounded-md border border-border bg-card px-3 py-2">
                      <div className="truncate text-sm font-medium text-foreground">{field.display_name || field.field_key}</div>
                      <div className="mt-1 truncate text-xs text-muted-foreground">{field.field_key}</div>
                      <div className="mt-2 line-clamp-2 text-xs leading-5 text-muted-foreground">{field.description || field.path || "Derived from normalized model paths."}</div>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}
            <div className="overflow-hidden rounded-lg border border-border">
              <div className="grid grid-cols-[minmax(160px,1.1fr)_minmax(140px,0.9fr)_120px_minmax(140px,0.8fr)] border-b border-border bg-muted/35 px-3 py-2 text-xs font-medium text-muted-foreground">
                <div>Field</div>
                <div>Range</div>
                <div>Role</div>
                <div>Reusable Slot</div>
              </div>
              <div className="max-h-[39rem] overflow-auto">
                {selectedUsages.length ? (
                  selectedUsages.map((usage) => {
                    const slot = usage.canonical_slot_id ? slotById.get(usage.canonical_slot_id) : null;
                    return <FieldRow key={usage.id} usage={usage} slot={slot} />;
                  })
                ) : (
                  <EmptyState message="No class fields have been attached yet." />
                )}
              </div>
            </div>
          </div>
        ) : (
          <EmptyState message={loading ? "Loading Canonical Model..." : "No canonical classes yet."} />
        )}
      </CardContent>
    </Card>
  );
}

function FieldRow({ usage, slot }: { usage: CanonicalClassSlotUsage; slot: CanonicalSlot | null | undefined }) {
  const inherited = usage.annotations?.context_platform_inherited === true;
  const declaredOn = typeof usage.annotations?.context_platform_declared_on === "string" ? usage.annotations.context_platform_declared_on : "";
  return (
    <div className="grid grid-cols-[minmax(160px,1.1fr)_minmax(140px,0.9fr)_120px_minmax(140px,0.8fr)] gap-3 border-b border-border/70 px-3 py-3 last:border-b-0">
      <div className="min-w-0">
        <div className="truncate text-sm font-medium text-foreground">{usage.name || slot?.name}</div>
        <div className="mt-1 line-clamp-2 text-xs leading-5 text-muted-foreground">{usage.description || slot?.description || "No description"}</div>
      </div>
      <div className="min-w-0 text-sm text-foreground">{rangeLabel(usage, slot)}</div>
      <div className="space-y-1">
        {usage.identity_role ? <Badge variant="info">{usage.identity_role}</Badge> : <span className="text-sm text-muted-foreground">field</span>}
        <div>
          <Badge variant={inherited ? "warning" : "default"}>{inherited ? `Inherited${declaredOn ? ` from ${declaredOn}` : ""}` : "Direct"}</Badge>
        </div>
      </div>
      <div className="min-w-0">
        <div className="truncate text-sm text-foreground">{slot?.name || "local usage"}</div>
        {slot?.aliases?.length ? <div className="mt-1 truncate text-xs text-muted-foreground">{slot.aliases.join(", ")}</div> : null}
      </div>
    </div>
  );
}

function RelationsTab({
  classes,
  relations,
  inheritanceEdges,
  query,
  classById,
}: {
  classes: CanonicalClass[];
  relations: CanonicalRelation[];
  inheritanceEdges: Array<{ source: CanonicalClass; target: CanonicalClass }>;
  query: string;
  classById: Map<string, CanonicalClass>;
}) {
  const rows = [
    ...inheritanceEdges.map((edge) => ({
      id: `is-a-${edge.source.id}-${edge.target.id}`,
      kind: "is_a",
      source: edge.source.name,
      target: edge.target.name,
      label: "is_a",
      status: edge.source.status,
    })),
    ...relations.map((relation) => ({
      id: relation.id,
      kind: "relation",
      source: relation.source_class_name || classById.get(relation.source_class_id)?.name || relation.source_class_id,
      target: relation.target_class_name || classById.get(relation.target_class_id)?.name || relation.target_class_id,
      label: relation.relation_type,
      status: relation.status,
    })),
  ].filter((row) => {
    if (!query) return true;
    return `${row.kind} ${row.source} ${row.label} ${row.target}`.toLowerCase().includes(query);
  });

  return (
    <Card>
      <CardHeader>
        <div>
          <CardTitle>Relations</CardTitle>
          <CardDescription>Inheritance and class-to-class canonical relations.</CardDescription>
        </div>
        <GitBranch className="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        <div className="overflow-hidden rounded-lg border border-border">
          <div className="grid grid-cols-[130px_minmax(150px,1fr)_180px_minmax(150px,1fr)_120px] border-b border-border bg-muted/35 px-3 py-2 text-xs font-medium text-muted-foreground">
            <div>Kind</div>
            <div>Source</div>
            <div>Edge</div>
            <div>Target</div>
            <div>Status</div>
          </div>
          <div className="max-h-[42rem] overflow-auto">
            {rows.length ? (
              rows.map((row) => (
                <div key={row.id} className="grid grid-cols-[130px_minmax(150px,1fr)_180px_minmax(150px,1fr)_120px] gap-3 border-b border-border/70 px-3 py-3 last:border-b-0">
                  <div><Badge variant={row.kind === "is_a" ? "warning" : "info"}>{row.kind}</Badge></div>
                  <div className="truncate text-sm font-medium text-foreground">{row.source}</div>
                  <div className="truncate text-sm text-muted-foreground">{row.label}</div>
                  <div className="truncate text-sm font-medium text-foreground">{row.target}</div>
                  <StatusBadge status={row.status} />
                </div>
              ))
            ) : (
              <EmptyState message="No relations match this search." />
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function GraphTab({
  graph,
  selectedClass,
  selectedUsages,
  selectedRelations,
  slotById,
  onSelectClass,
}: {
  graph: { nodes: Node[]; edges: Edge[] };
  selectedClass: CanonicalClass | null;
  selectedUsages: CanonicalClassSlotUsage[];
  selectedRelations: CanonicalRelation[];
  slotById: Map<string, CanonicalSlot>;
  onSelectClass: (classId: string) => void;
}) {
  return (
    <section className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_360px]">
      <div className="h-[680px] overflow-hidden rounded-lg border border-border bg-card">
        <ReactFlowProvider>
          <ReactFlow
            nodes={graph.nodes}
            edges={graph.edges}
            fitView
            fitViewOptions={{ padding: 0.18 }}
            minZoom={0.25}
            maxZoom={1.6}
            nodesDraggable
            nodesConnectable={false}
            elementsSelectable
            onNodeClick={(_, node) => onSelectClass(node.id)}
          >
            <Background gap={24} size={1} />
            <Controls showInteractive={false} />
            <MiniMap pannable zoomable nodeStrokeWidth={3} />
          </ReactFlow>
        </ReactFlowProvider>
      </div>

      <Card>
        <CardHeader>
          <div className="min-w-0">
            <CardTitle className="truncate">{selectedClass?.name || "Graph Inspector"}</CardTitle>
            <CardDescription>{selectedClass ? metadataString(selectedClass.metadata, "is_a") || "Root class" : "Select a node in the graph."}</CardDescription>
          </div>
          <Network className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent className="space-y-4">
          {selectedClass ? (
            <>
              <div className="space-y-2">
                <div className="text-xs font-medium uppercase tracking-[0.12em] text-muted-foreground">Fields</div>
                <div className="max-h-64 space-y-2 overflow-auto pr-1">
                  {selectedUsages.slice(0, 12).map((usage) => {
                    const slot = usage.canonical_slot_id ? slotById.get(usage.canonical_slot_id) : null;
                    return (
                      <div key={usage.id} className="rounded-md border border-border px-3 py-2">
                        <div className="truncate text-sm font-medium text-foreground">{usage.name}</div>
                        <div className="mt-1 text-xs text-muted-foreground">{rangeLabel(usage, slot)}</div>
                      </div>
                    );
                  })}
                  {!selectedUsages.length ? <EmptyState message="No fields." /> : null}
                </div>
              </div>
              <div className="space-y-2">
                <div className="text-xs font-medium uppercase tracking-[0.12em] text-muted-foreground">Relations</div>
                <div className="space-y-2">
                  {selectedRelations.map((relation) => (
                    <div key={relation.id} className="rounded-md border border-border px-3 py-2 text-sm">
                      <div className="font-medium text-foreground">{relation.relation_type}</div>
                      <div className="mt-1 text-xs text-muted-foreground">
                        {relation.source_class_name || relation.source_class_id} {"->"} {relation.target_class_name || relation.target_class_id}
                      </div>
                    </div>
                  ))}
                  {!selectedRelations.length ? <EmptyState message="No direct relations." /> : null}
                </div>
              </div>
            </>
          ) : (
            <EmptyState message="No class selected." />
          )}
        </CardContent>
      </Card>
    </section>
  );
}

function LinkmlTab({ linkml }: { linkml: Record<string, unknown> }) {
  return (
    <Card>
      <CardHeader>
        <div>
          <CardTitle>LinkML Export</CardTitle>
          <CardDescription>Current representation model export from PostgreSQL.</CardDescription>
        </div>
        <Braces className="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        <pre className="max-h-[680px] overflow-auto rounded-lg border border-border bg-muted/35 p-4 text-xs leading-5 text-foreground">
          {JSON.stringify(linkml, null, 2)}
        </pre>
      </CardContent>
    </Card>
  );
}

function buildGraph({
  classes,
  usagesByClassId,
  relations,
  inheritanceEdges,
  selectedClassId,
}: {
  classes: CanonicalClass[];
  usagesByClassId: Map<string, CanonicalClassSlotUsage[]>;
  relations: CanonicalRelation[];
  inheritanceEdges: Array<{ source: CanonicalClass; target: CanonicalClass }>;
  selectedClassId: string;
}): { nodes: Node[]; edges: Edge[] } {
  const classById = new Map(classes.map((item) => [item.id, item]));
  const classByName = new Map(classes.map((item) => [item.name, item]));
  const depthById = new Map<string, number>();
  const childrenByParent = new Map<string, CanonicalClass[]>();

  for (const canonicalClass of classes) {
    const parentName = metadataString(canonicalClass.metadata, "is_a");
    const parent = parentName ? classByName.get(parentName) : undefined;
    if (parent) {
      const children = childrenByParent.get(parent.id) || [];
      children.push(canonicalClass);
      childrenByParent.set(parent.id, children);
    }
  }

  function depthFor(item: CanonicalClass, seen = new Set<string>()): number {
    if (depthById.has(item.id)) return depthById.get(item.id) || 0;
    if (seen.has(item.id)) return 0;
    seen.add(item.id);
    const parentName = metadataString(item.metadata, "is_a");
    const parent = parentName ? classByName.get(parentName) : undefined;
    const depth = parent ? depthFor(parent, seen) + 1 : 0;
    depthById.set(item.id, depth);
    return depth;
  }

  const rowsByDepth = new Map<number, CanonicalClass[]>();
  for (const canonicalClass of classes) {
    const depth = depthFor(canonicalClass);
    const row = rowsByDepth.get(depth) || [];
    row.push(canonicalClass);
    rowsByDepth.set(depth, row);
  }

  const nodes: Node[] = [];
  for (const [depth, row] of rowsByDepth.entries()) {
    row.sort((left, right) => left.name.localeCompare(right.name));
    row.forEach((canonicalClass, index) => {
      const isAbstract = metadataBool(canonicalClass.metadata, "abstract");
      const kind = classKind(canonicalClass);
      const usageCount = usagesByClassId.get(canonicalClass.id)?.length || 0;
      const selected = canonicalClass.id === selectedClassId;
      nodes.push({
        id: canonicalClass.id,
        type: "default",
        position: { x: depth * 300, y: index * 145 },
        data: {
          label: (
            <div className="min-w-[190px] max-w-[220px]">
              <div className="flex items-center justify-between gap-2">
                <div className="truncate text-sm font-semibold">{canonicalClass.name}</div>
                {isAbstract ? <span className="text-[10px] uppercase text-muted-foreground">abstract</span> : null}
              </div>
              <div className="mt-1 truncate text-xs text-muted-foreground">{classKindLabel(kind)} · {usageCount} fields</div>
              {canonicalClass.description ? <div className="mt-2 line-clamp-2 text-[11px] leading-4 text-muted-foreground">{canonicalClass.description}</div> : null}
            </div>
          ),
        },
        style: {
          border: selected ? "2px solid hsl(var(--primary))" : isAbstract || kind === "value_object" || kind === "context_object" ? "1px dashed hsl(var(--border))" : "1px solid hsl(var(--border))",
          borderRadius: 8,
          background: selected ? "hsl(var(--accent))" : "hsl(var(--card))",
          color: "hsl(var(--foreground))",
          padding: 0,
          width: 230,
        },
      });
    });
  }

  const edges: Edge[] = [
    ...inheritanceEdges
      .filter((edge) => classById.has(edge.source.id) && classById.has(edge.target.id))
      .map((edge) => ({
        id: `is-a-${edge.source.id}-${edge.target.id}`,
        source: edge.source.id,
        target: edge.target.id,
        label: "is_a",
        type: "smoothstep",
        animated: false,
        markerEnd: { type: MarkerType.ArrowClosed },
        style: { stroke: "hsl(var(--muted-foreground))", strokeDasharray: "5 5" },
        labelStyle: { fill: "hsl(var(--muted-foreground))", fontSize: 11 },
      })),
    ...relations
      .filter((relation) => classById.has(relation.source_class_id) && classById.has(relation.target_class_id))
      .map((relation) => ({
        id: relation.id,
        source: relation.source_class_id,
        target: relation.target_class_id,
        label: relation.relation_type,
        type: "smoothstep",
        markerEnd: { type: MarkerType.ArrowClosed },
        style: { stroke: "hsl(var(--primary))", strokeWidth: 1.6 },
        labelStyle: { fill: "hsl(var(--foreground))", fontSize: 11 },
      })),
  ];

  return { nodes, edges };
}

function rangeLabel(usage: CanonicalClassSlotUsage, slot: CanonicalSlot | null | undefined) {
  return usage.datatype || slot?.datatype || slot?.range_ref || slot?.range_kind || "unspecified";
}

function metadataString(metadata: Record<string, unknown> | undefined, key: string) {
  const value = metadata?.[key];
  return typeof value === "string" ? value : "";
}

function metadataBool(metadata: Record<string, unknown> | undefined, key: string) {
  return metadata?.[key] === true || metadata?.[key] === "true";
}

function metadataObject(metadata: Record<string, unknown> | undefined, key: string) {
  const value = metadata?.[key];
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function classKind(canonicalClass: CanonicalClass): ClassKind {
  const annotations = metadataObject(canonicalClass.metadata, "annotations");
  const value = annotations.context_platform_class_kind;
  if (
    value === "reference_value" ||
    value === "reference_scheme" ||
    value === "value_object" ||
    value === "context_object" ||
    value === "business_entity"
  ) {
    return value;
  }
  if (metadataBool(canonicalClass.metadata, "abstract")) return "abstract";
  return "business_entity";
}

function classKindGroup(kind: ClassKind) {
  if (kind === "reference_value" || kind === "reference_scheme") return "reference";
  if (kind === "value_object" || kind === "context_object") return "value_context";
  if (kind === "abstract") return "abstract";
  return "business";
}

const classKindSections = [
  { key: "business", label: "Business entities" },
  { key: "reference", label: "Reference values and schemes" },
  { key: "value_context", label: "Value and context objects" },
  { key: "abstract", label: "Abstract bases" },
] as const;

function classKindLabel(kind: ClassKind) {
  const labels: Record<ClassKind, string> = {
    business_entity: "Business entity",
    reference_value: "Reference value",
    reference_scheme: "Reference scheme",
    value_object: "Value object",
    context_object: "Context object",
    abstract: "Abstract",
  };
  return labels[kind];
}

function countByClassKind(classes: CanonicalClass[]) {
  return classes.reduce<Record<ClassKind, number>>(
    (counts, canonicalClass) => {
      const kind = classKind(canonicalClass);
      counts[kind] += 1;
      return counts;
    },
    {
      business_entity: 0,
      reference_value: 0,
      reference_scheme: 0,
      value_object: 0,
      context_object: 0,
      abstract: 0,
    },
  );
}

function KindBadge({ kind }: { kind: ClassKind }) {
  const variant = kind === "business_entity" ? "success" : kind === "reference_value" || kind === "reference_scheme" ? "info" : kind === "abstract" ? "warning" : "default";
  return <Badge variant={variant}>{classKindLabel(kind)}</Badge>;
}

function canonicalViewFields(canonicalClass: CanonicalClass) {
  const annotations = metadataObject(canonicalClass.metadata, "annotations");
  const value = annotations.context_platform_view_fields;
  if (!Array.isArray(value)) return [];
  return value
    .filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object" && !Array.isArray(item))
    .map((item) => ({
      field_key: typeof item.field_key === "string" ? item.field_key : "",
      display_name: typeof item.display_name === "string" ? item.display_name : "",
      path: typeof item.path === "string" ? item.path : "",
      description: typeof item.description === "string" ? item.description : "",
    }))
    .filter((item) => item.field_key);
}

function Metric({
  icon: Icon,
  label,
  value,
  detail,
}: {
  icon: typeof Database;
  label: string;
  value: string | number;
  detail: string;
}) {
  return (
    <div className="rounded-lg border border-border bg-card px-4 py-3">
      <div className="flex items-center justify-between gap-3">
        <div className="text-xs text-muted-foreground">{label}</div>
        <Icon className="h-4 w-4 text-muted-foreground" />
      </div>
      <div className="mt-2 text-2xl font-semibold text-foreground">{value}</div>
      <div className="mt-1 text-xs text-muted-foreground">{detail}</div>
    </div>
  );
}

function StatusBadge({ status }: { status?: string }) {
  const value = status || "draft";
  const variant =
    value === "complete" || value === "completed" || value === "approved" || value === "published"
      ? "success"
      : value === "failed" || value === "rejected"
        ? "danger"
        : value === "running" || value === "submitted" || value === "proposed"
          ? "info"
          : "warning";
  return <Badge variant={variant}>{value}</Badge>;
}

function EmptyState({ message }: { message: string }) {
  return (
    <div className="rounded-lg border border-dashed border-border px-4 py-8 text-center text-sm text-muted-foreground">
      {message}
    </div>
  );
}

function Notice({ message }: { message: string }) {
  return (
    <div className="rounded-lg border border-red-500/25 bg-red-500/10 px-4 py-3 text-sm text-red-700 dark:text-red-300">
      {message}
    </div>
  );
}
