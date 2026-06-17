import type { FieldMapping } from "@/types/semantic";
import { MetaCard } from "@/components/semantic/common/meta-card";
import { InspectorEmpty, InspectorShell } from "@/components/semantic/inspector/inspector-shell";
import { InspectorJson, InspectorSection, InspectorSurface } from "@/components/semantic/inspector/inspector-section";
import { semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

export function MappingInspector({
  mapping,
  semanticTypeNames,
  mode = "preview",
  onOpenDetail
}: {
  mapping: FieldMapping | null;
  semanticTypeNames?: Record<string, string>;
  mode?: "preview" | "detail";
  onOpenDetail?: () => void;
}) {
  if (!mapping) {
    return <InspectorEmpty title="Mapping Inspector" message="Select a mapping to inspect semantic context, canonical link, and governance state." />;
  }

  const display = mapping.draft_snapshot || mapping;
  const canonicalValue = display.canonical_attribute_id ? semanticTypeNames?.[display.canonical_attribute_id] || display.canonical_attribute_id : "-";

  return (
    <InspectorShell
      title={display.field_path}
      subtitle={`${display.operation_id} · ${display.semantic_type_id}`}
      actions={
        <div className="flex flex-wrap gap-2">
          <Badge variant={semanticStatusBadgeVariant(display.status)}>{display.status || "unknown"}</Badge>
          {mapping.pending_proposal_id ? <Badge variant="warning">proposal {mapping.pending_proposal_id}</Badge> : null}
          {mode === "preview" && onOpenDetail ? (
            <Button size="sm" variant="outline" onClick={onOpenDetail}>
              Open detail
            </Button>
          ) : null}
        </div>
      }
    >
      <div className="space-y-4">
        <InspectorSection title="Summary">
          <div className="grid gap-2 sm:grid-cols-2">
            <MetaCard label="Source" value={display.source_id || "-"} />
            <MetaCard label="Operation" value={display.operation_id} />
            <MetaCard label="Semantic Type" value={semanticTypeNames?.[display.semantic_type_id] || display.semantic_type_id} />
            <MetaCard label="Canonical Attribute" value={canonicalValue} />
            <MetaCard label="Mapping Type" value={display.mapping_type || "exact"} />
            <MetaCard label="Mapping Kind" value={display.mapping_kind || "direct"} />
          </div>
        </InspectorSection>
        <InspectorSection title={mode === "preview" ? "Preview" : "Evidence"}>
          <InspectorSurface muted>
            <div className="text-sm text-muted-foreground">{display.notes || "No notes yet."}</div>
          </InspectorSurface>
          {mode === "detail" && display.evidence?.length ? (
            <InspectorSurface>
              <InspectorJson value={display.evidence} />
            </InspectorSurface>
          ) : null}
        </InspectorSection>
        {mode === "detail" ? (
          <InspectorSection title="Transforms">
            <div className="grid gap-4">
              <InspectorSurface>
                <div className="mb-2 text-xs uppercase tracking-[0.12em] text-muted-foreground">Transform Spec</div>
                <InspectorJson value={display.transform_spec || {}} />
              </InspectorSurface>
              <InspectorSurface>
                <div className="mb-2 text-xs uppercase tracking-[0.12em] text-muted-foreground">Enum Mapping</div>
                <InspectorJson value={display.enum_mapping || {}} />
              </InspectorSurface>
            </div>
          </InspectorSection>
        ) : null}
      </div>
    </InspectorShell>
  );
}
