import type { Capability } from "@/types/semantic";
import { MetaCard } from "@/components/semantic/common/meta-card";
import { InspectorEmpty, InspectorShell } from "@/components/semantic/inspector/inspector-shell";
import { InspectorJson, InspectorSection, InspectorSurface } from "@/components/semantic/inspector/inspector-section";
import { formatSemanticList, semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { Badge } from "@/components/ui/badge";

export function CapabilityInspector({
  capability,
  semanticTypeNames
}: {
  capability: Capability | null;
  semanticTypeNames?: Record<string, string>;
}) {
  if (!capability) {
    return <InspectorEmpty title="Capability Inspector" message="Select a capability to inspect semantic IO coverage and governance state." />;
  }

  const display = capability.draft_snapshot || capability;

  return (
    <InspectorShell
      title={display.name}
      subtitle={`${display.capability_key} · ${display.namespace || "public"}`}
      actions={
        <div className="flex flex-wrap gap-2">
          <Badge variant={semanticStatusBadgeVariant(display.status)}>{display.status || "unknown"}</Badge>
          {capability.pending_proposal_id ? <Badge variant="warning">proposal {capability.pending_proposal_id}</Badge> : null}
        </div>
      }
    >
      <div className="space-y-4">
        <InspectorSection title="Summary">
          <div className="grid gap-2 sm:grid-cols-2">
            <MetaCard
              label="Inputs"
              value={formatSemanticList((display.input_semantic_types || []).map((item) => semanticTypeNames?.[item] || item))}
            />
            <MetaCard
              label="Outputs"
              value={formatSemanticList((display.output_semantic_types || []).map((item) => semanticTypeNames?.[item] || item))}
            />
            <MetaCard label="Lifecycle" value={display.lifecycle || "-"} />
            <MetaCard label="Version" value={display.version || "-"} />
          </div>
          <InspectorSurface muted>
            <div className="text-sm text-muted-foreground">{display.description || "No description yet."}</div>
          </InspectorSurface>
        </InspectorSection>
        <InspectorSection title="Evidence">
          {display.evidence?.length ? (
            <InspectorSurface>
              <InspectorJson value={display.evidence} />
            </InspectorSurface>
          ) : (
            <InspectorSurface muted>
              <div className="text-sm text-muted-foreground">No evidence attached yet.</div>
            </InspectorSurface>
          )}
        </InspectorSection>
        <InspectorSection title="Planner Context">
          <div className="grid gap-4">
            <InspectorSurface>
              <div className="mb-2 text-xs uppercase tracking-[0.12em] text-muted-foreground">Intent Spec</div>
              <InspectorJson value={display.intent_spec || {}} />
            </InspectorSurface>
            <div className="grid gap-2 sm:grid-cols-2">
              <MetaCard label="Namespace" value={display.namespace || "-"} />
              <MetaCard label="Confidence" value={display.confidence == null ? "-" : String(display.confidence)} />
            </div>
          </div>
        </InspectorSection>
      </div>
    </InspectorShell>
  );
}
