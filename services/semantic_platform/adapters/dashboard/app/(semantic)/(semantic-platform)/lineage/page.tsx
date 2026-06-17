"use client";

import { useMemo, useState } from "react";
import { RefreshCw } from "lucide-react";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { MetaCard } from "@/components/semantic/common/meta-card";
import { EmptyPanel, ErrorPanel, InfoLine, LoadingPanel } from "@/components/semantic/common/state-panel";
import { FilterToolbar } from "@/components/semantic/filters/filter-toolbar";
import { useCapabilities } from "@/hooks/semantic/use-capabilities";
import { useMappings } from "@/hooks/semantic/use-mappings";
import { useOperations } from "@/hooks/semantic/use-operations";
import { useSemanticRegistry } from "@/hooks/semantic/use-proposals";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function LineagePage() {
  const registry = useSemanticRegistry();
  const mappings = useMappings({ page: 1, pageSize: 500 });
  const capabilities = useCapabilities({ page: 1, pageSize: 500 });
  const operations = useOperations({ page: 1, pageSize: 500 });
  const loading = registry.loading || mappings.loading || capabilities.loading || operations.loading;
  const error = registry.error || mappings.error || capabilities.error || operations.error;
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");
  const [selectedId, setSelectedId] = useState("");

  const nodes = useMemo(
    () =>
      registry.semanticTypes.filter((item) => {
        const display = item.draft_snapshot || item;
        const lowered = query.toLowerCase();
        const matchesQuery =
          !query ||
          display.name.toLowerCase().includes(lowered) ||
          (display.description || "").toLowerCase().includes(lowered);
        const matchesStatus = status === "all" || (display.status || "") === status;
        return matchesQuery && matchesStatus;
      }),
    [registry.semanticTypes, query, status]
  );
  const selected = useMemo(() => nodes.find((item) => item.id === selectedId) || nodes[0] || null, [nodes, selectedId]);
  const linkedRelationships = useMemo(
    () => registry.relationships.filter((item) => item.source_id === selected?.id || item.target_id === selected?.id),
    [registry.relationships, selected]
  );
  const linkedMappings = useMemo(
    () =>
      mappings.data.items.filter(
        (item) => item.semantic_type_id === selected?.id || item.canonical_attribute_id === selected?.id
      ),
    [mappings.data.items, selected]
  );
  const linkedCapabilities = useMemo(
    () =>
      capabilities.data.items.filter((item) => {
        const display = item.draft_snapshot || item;
        return display.input_semantic_types?.includes(selected?.id || "") || display.output_semantic_types?.includes(selected?.id || "");
      }),
    [capabilities.data.items, selected]
  );
  const linkedOperations = useMemo(() => {
    const operationIds = new Set(linkedMappings.map((item) => item.operation_id));
    return operations.data.items.filter((item) => operationIds.has(item.id));
  }, [linkedMappings, operations.data.items]);

  return (
    <SectionPlaceholder
      title="Lineage"
      description="Trace how semantic types connect to relationships, field mappings, capabilities, and execution operations."
      actions={
        <Button
          type="button"
          variant="outline"
          onClick={() => {
            void registry.reload();
            void mappings.reload();
            void capabilities.reload();
            void operations.reload();
          }}
          disabled={loading}
        >
          <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      }
      body={
        <div className="space-y-4">
          <InfoLine>
            <Badge variant="info">{nodes.length} nodes</Badge>
            <span>This is a first lineage explorer that ties semantic registry, mappings, capabilities, and operations together.</span>
          </InfoLine>

          <FilterToolbar
            query={query}
            onQueryChange={setQuery}
            queryPlaceholder="Search lineage nodes by semantic type name or description"
            status={status}
            onStatusChange={setStatus}
          />

          {error ? <ErrorPanel message={error} /> : null}
          {loading ? <LoadingPanel message="Loading lineage..." /> : null}

          {!loading && !error ? (
            <div className="grid gap-4 xl:grid-cols-[minmax(0,0.85fr)_minmax(0,1.15fr)]">
              <Card className="border-border/70">
                <CardHeader>
                  <CardTitle>Lineage Nodes</CardTitle>
                </CardHeader>
                <CardContent className="space-y-2">
                  {nodes.length ? (
                    nodes.map((item) => {
                      const display = item.draft_snapshot || item;
                      const active = item.id === selected?.id;
                      return (
                        <button
                          key={item.id}
                          type="button"
                          className={`w-full rounded-xl border px-3 py-3 text-left transition ${active ? "border-primary/20 bg-primary/10" : "border-border/70 hover:bg-muted/20"}`}
                          onClick={() => setSelectedId(item.id)}
                        >
                          <div className="font-medium text-foreground">{display.name}</div>
                          <div className="mt-1 text-xs text-muted-foreground">{display.entity_kind || "-"} · {display.datatype || "-"}</div>
                        </button>
                      );
                    })
                  ) : (
                    <EmptyPanel message="No lineage nodes match the current filters." />
                  )}
                </CardContent>
              </Card>

              <Card className="border-border/70">
                <CardHeader>
                  <CardTitle>{(selected?.draft_snapshot || selected)?.name || "Lineage Inspector"}</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  {selected ? (
                    <>
                      <div className="grid gap-3 sm:grid-cols-2">
                        <MetaCard label="Relationships" value={String(linkedRelationships.length)} />
                        <MetaCard label="Mappings" value={String(linkedMappings.length)} />
                        <MetaCard label="Capabilities" value={String(linkedCapabilities.length)} />
                        <MetaCard label="Operations" value={String(linkedOperations.length)} />
                      </div>

                      <LineageSection
                        title="Relationships"
                        items={linkedRelationships.map((item) => `${item.source_name} ${item.relation_type} ${item.target_name}`)}
                      />
                      <LineageSection
                        title="Mappings"
                        items={linkedMappings.map((item) => `${item.field_path} -> ${item.operation_id}`)}
                      />
                      <LineageSection
                        title="Capabilities"
                        items={linkedCapabilities.map((item) => `${(item.draft_snapshot || item).name} (${(item.draft_snapshot || item).capability_key})`)}
                      />
                      <LineageSection
                        title="Operations"
                        items={linkedOperations.map((item) => `${item.name} (${item.operation_key})`)}
                      />
                    </>
                  ) : (
                    <EmptyPanel message="Select a semantic type to inspect lineage." />
                  )}
                </CardContent>
              </Card>
            </div>
          ) : null}
        </div>
      }
    />
  );
}

function LineageSection({ title, items }: { title: string; items: string[] }) {
  return (
    <section className="space-y-2">
      <div className="text-xs uppercase tracking-[0.12em] text-muted-foreground">{title}</div>
      {items.length ? (
        <div className="space-y-2">
          {items.map((item) => (
            <div key={`${title}-${item}`} className="rounded-lg border border-border/70 px-3 py-2 text-sm text-foreground">
              {item}
            </div>
          ))}
        </div>
      ) : (
        <div className="rounded-xl border border-dashed border-border/70 bg-muted/10 p-4 text-sm text-muted-foreground">
          No linked {title.toLowerCase()}.
        </div>
      )}
    </section>
  );
}
