import type { SemanticRelationship, SemanticType } from "@/types/semantic";
import { MetaCard } from "@/components/semantic/common/meta-card";
import { InspectorSection, InspectorSurface } from "@/components/semantic/inspector/inspector-section";
import { formatSemanticList, semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export function SemanticTypeInspector({
  semanticType,
  linkedRelationships
}: {
  semanticType: SemanticType | null;
  linkedRelationships: SemanticRelationship[];
}) {
  if (!semanticType) {
    return (
      <Card className="border-border/70">
        <CardHeader>
          <CardTitle>Semantic Type Inspector</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">Select a semantic type to inspect meaning, aliases, and linked relationships.</CardContent>
      </Card>
    );
  }

  const display = semanticType.draft_snapshot || semanticType;

  return (
    <Card className="border-border/70">
      <CardHeader>
        <div>
          <CardTitle>{display.name}</CardTitle>
          <div className="mt-1 text-xs text-muted-foreground">{display.entity_kind || "-"} · {display.datatype || "-"}</div>
        </div>
        <div className="flex flex-wrap gap-2">
          <Badge variant={semanticStatusBadgeVariant(display.status)}>{display.status || "unknown"}</Badge>
          {semanticType.pending_proposal_id ? <Badge variant="warning">proposal {semanticType.pending_proposal_id}</Badge> : null}
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <InspectorSection title="Summary">
          <div className="grid gap-2 sm:grid-cols-2">
            <MetaCard label="Semantic Type ID" value={semanticType.id} />
            <MetaCard label="Parent Entity" value={display.parent_entity_name || "-"} />
            <MetaCard label="Aliases" value={formatSemanticList(display.aliases)} />
            <MetaCard label="Owners" value={formatSemanticList(display.owners)} />
          </div>
          <InspectorSurface muted>
            <div className="text-sm text-muted-foreground">{display.description || "No description yet."}</div>
          </InspectorSurface>
        </InspectorSection>
        <InspectorSection title="Linked Relationships">
          <div className="space-y-2">
            {linkedRelationships.length ? (
              linkedRelationships.map((relationship) => (
                <div key={relationship.id} className="rounded-lg border border-border/70 px-3 py-2 text-sm">
                  <div className="font-medium text-foreground">
                    {relationship.source_name} <span className="text-muted-foreground">{relationship.relation_type}</span> {relationship.target_name}
                  </div>
                  <div className="mt-1 text-xs text-muted-foreground">{relationship.id}</div>
                </div>
              ))
            ) : (
              <InspectorSurface muted>
                <div className="text-sm text-muted-foreground">No linked relationships.</div>
              </InspectorSurface>
            )}
          </div>
        </InspectorSection>
      </CardContent>
    </Card>
  );
}
