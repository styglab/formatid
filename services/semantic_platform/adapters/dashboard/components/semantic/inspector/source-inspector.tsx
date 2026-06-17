import type { ExecutionSource } from "@/types/semantic";
import { MetaCard } from "@/components/semantic/common/meta-card";
import { InspectorEmpty, InspectorShell } from "@/components/semantic/inspector/inspector-shell";
import { InspectorJson, InspectorSection, InspectorSurface } from "@/components/semantic/inspector/inspector-section";
import { semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { Badge } from "@/components/ui/badge";

export function SourceInspector({ source }: { source: ExecutionSource | null }) {
  if (!source) {
    return <InspectorEmpty title="Source Inspector" message="Select a source to inspect its semantic onboarding context." />;
  }

  const display = source.draft_snapshot || source;
  const config = (display.config || {}) as Record<string, unknown>;

  return (
    <InspectorShell
      title={display.name}
      subtitle={`${display.provider || "no provider"} · ${display.source_type}`}
      actions={
        <div className="flex flex-wrap gap-2">
          <Badge variant={semanticStatusBadgeVariant(display.status)}>{display.status || "unknown"}</Badge>
          {source.pending_proposal_id ? <Badge variant="warning">proposal {source.pending_proposal_id}</Badge> : null}
        </div>
      }
    >
      <div className="space-y-4">
        <InspectorSection title="Summary">
          <div className="grid gap-2 sm:grid-cols-2">
            <MetaCard label="Source ID" value={source.id} />
            <MetaCard label="Input Mode" value={String(config.input_mode || "-")} />
            <MetaCard label="Reference URI" value={String(config.reference_uri || "-")} />
            <MetaCard label="Updated At" value={display.updated_at || "-"} />
          </div>
          <InspectorSurface muted>
            <div className="text-sm text-muted-foreground">{display.description || "No description yet."}</div>
          </InspectorSurface>
        </InspectorSection>
        <InspectorSection title="Evidence">
          <InspectorSurface muted>
            <div className="text-sm text-muted-foreground">{String(config.manual_notes || "No evidence notes attached yet.")}</div>
          </InspectorSurface>
        </InspectorSection>
        <InspectorSection title="Linked Context">
          <div className="grid gap-2 sm:grid-cols-2">
            <MetaCard label="Provider" value={display.provider || "-"} />
            <MetaCard label="Source Type" value={display.source_type} />
            <MetaCard label="Pending Proposal" value={source.pending_proposal_id || "-"} />
            <MetaCard label="Lifecycle Status" value={display.status || "-"} />
          </div>
        </InspectorSection>
        <InspectorSection title="Config">
          <InspectorSurface>
            <InspectorJson value={config} />
          </InspectorSurface>
        </InspectorSection>
      </div>
    </InspectorShell>
  );
}
