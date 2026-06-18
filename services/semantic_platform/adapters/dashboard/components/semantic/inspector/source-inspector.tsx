import Link from "next/link";
import type { ExecutionSource } from "@/types/semantic";
import { MetaCard } from "@/components/semantic/common/meta-card";
import { InspectorEmpty, InspectorShell } from "@/components/semantic/inspector/inspector-shell";
import { InspectorJson, InspectorSection, InspectorSurface } from "@/components/semantic/inspector/inspector-section";
import { semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

type SourceInspectorProps = {
  source: ExecutionSource | null;
  onStartWorkspace?: () => void;
  startingWorkspace?: boolean;
};

export function SourceInspector({ source, onStartWorkspace, startingWorkspace = false }: SourceInspectorProps) {
  if (!source) {
    return <InspectorEmpty title="Source Inspector" message="Select a source to inspect its semantic onboarding context." />;
  }

  const display = source.draft_snapshot || source;
  const config = (display.config || {}) as Record<string, unknown>;
  const latestWorkspaceLabel = source.latest_run_stage || source.latest_run_status || "No workspace yet";

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
        <InspectorSection title="Workspace Entry">
          <div className="grid gap-2 sm:grid-cols-2">
            <MetaCard label="Latest Workspace" value={latestWorkspaceLabel} />
            <MetaCard label="Pending Proposals" value={String(source.pending_proposal_count || 0)} />
            <MetaCard label="Assets" value={String(source.asset_count || 0)} />
            <MetaCard label="Fields" value={String(source.field_count || 0)} />
          </div>
          <div className="flex flex-wrap gap-2">
            <Button type="button" onClick={onStartWorkspace} disabled={!onStartWorkspace || startingWorkspace}>
              {startingWorkspace ? "Starting..." : "Start Workspace"}
            </Button>
            {source.latest_run_id ? (
              <Button type="button" variant="outline" asChild>
                <Link href={`/onboarding-runs/${source.latest_run_id}`}>Open Latest Workspace</Link>
              </Button>
            ) : null}
          </div>
        </InspectorSection>
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
            <MetaCard label="Latest Workspace Status" value={source.latest_run_status || "-"} />
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
