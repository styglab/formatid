import type { CanonicalAttribute, CanonicalEntity, CanonicalRelation } from "@/types/semantic";
import { MetaCard } from "@/components/semantic/common/meta-card";
import { InspectorSection, InspectorSurface } from "@/components/semantic/inspector/inspector-section";
import { semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export function CanonicalEntityInspector({
  entity,
  attributes,
  relationships,
  mappedAttributes,
  selectedAttributeId,
  onSelectAttribute,
  selectedRelationId,
  onSelectRelation
}: {
  entity: CanonicalEntity | null;
  attributes: CanonicalAttribute[];
  relationships: CanonicalRelation[];
  mappedAttributes: number;
  selectedAttributeId: string;
  onSelectAttribute: (id: string) => void;
  selectedRelationId: string;
  onSelectRelation: (id: string) => void;
}) {
  if (!entity) {
    return (
      <Card className="border-border/70">
        <CardHeader>
          <CardTitle>Canonical Entity Inspector</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">Select an entity to inspect child attributes, linked relations, and mapping coverage.</CardContent>
      </Card>
    );
  }

  const display = entity.draft_snapshot || entity;

  return (
    <Card className="border-border/70">
      <CardHeader>
        <div>
          <CardTitle>{display.name}</CardTitle>
          <div className="mt-1 text-xs text-muted-foreground">{entity.id}</div>
        </div>
        <Badge variant={semanticStatusBadgeVariant(display.status)}>{display.status || "unknown"}</Badge>
      </CardHeader>
      <CardContent className="space-y-4">
        <InspectorSection title="Summary">
          <div className="grid gap-2 sm:grid-cols-2">
            <MetaCard label="Attributes" value={String(attributes.length)} />
            <MetaCard label="Mapped Attributes" value={String(mappedAttributes)} />
            <MetaCard label="Relationships" value={String(relationships.length)} />
            <MetaCard label="Status" value={display.status || "-"} />
          </div>
          <InspectorSurface muted>
            <div className="text-sm text-muted-foreground">{display.description || "No description yet."}</div>
          </InspectorSurface>
        </InspectorSection>
        <InspectorSection title="Attributes">
          <div className="space-y-2">
            {attributes.length ? (
              attributes.map((attribute) => (
                <button
                  key={attribute.id}
                  type="button"
                  onClick={() => onSelectAttribute(attribute.id)}
                  className={`block w-full rounded-lg border px-3 py-2 text-left text-sm transition hover:bg-muted/30 ${
                    selectedAttributeId === attribute.id ? "border-primary/40 bg-primary/5" : "border-border/70"
                  }`}
                >
                  <div className="font-medium text-foreground">{(attribute.draft_snapshot || attribute).name}</div>
                  <div className="mt-1 text-xs text-muted-foreground">{(attribute.draft_snapshot || attribute).datatype || "-"} · {attribute.id}</div>
                </button>
              ))
            ) : (
              <InspectorSurface muted>
                <div className="text-sm text-muted-foreground">No attributes linked to this entity.</div>
              </InspectorSurface>
            )}
          </div>
        </InspectorSection>
        <InspectorSection title="Relationships">
          <div className="space-y-2">
            {relationships.length ? (
              relationships.map((relationship) => (
                <button
                  key={relationship.id}
                  type="button"
                  onClick={() => onSelectRelation(relationship.id)}
                  className={`block w-full rounded-lg border px-3 py-2 text-left text-sm transition hover:bg-muted/30 ${
                    selectedRelationId === relationship.id ? "border-primary/40 bg-primary/5" : "border-border/70"
                  }`}
                >
                  <div className="font-medium text-foreground">
                    {(relationship.draft_snapshot || relationship).source_entity_name} <span className="text-muted-foreground">{(relationship.draft_snapshot || relationship).relation_type}</span> {(relationship.draft_snapshot || relationship).target_entity_name}
                  </div>
                  <div className="mt-1 text-xs text-muted-foreground">{relationship.id}</div>
                </button>
              ))
            ) : (
              <InspectorSurface muted>
                <div className="text-sm text-muted-foreground">No canonical relations linked to this entity.</div>
              </InspectorSurface>
            )}
          </div>
        </InspectorSection>
      </CardContent>
    </Card>
  );
}
