import type { Capability, ExecutionOperation, OperationVariant } from "@/types/semantic";
import { MetaCard } from "@/components/semantic/common/meta-card";
import { semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { InspectorEmpty, InspectorShell } from "@/components/semantic/inspector/inspector-shell";
import { InspectorJson, InspectorSection, InspectorSurface } from "@/components/semantic/inspector/inspector-section";
import { Badge } from "@/components/ui/badge";

export function VariantInspector({
  variant,
  operations,
  capabilities
}: {
  variant: OperationVariant | null;
  operations: ExecutionOperation[];
  capabilities: Capability[];
}) {
  if (!variant) {
    return <InspectorEmpty title="Variant Inspector" message="Select a variant to inspect fixed semantic controls, operation-local meaning split, and linked capability context." />;
  }

  const metadata = variant.metadata || {};
  const operation =
    operations.find((item) => item.id === variant.operation_id) ||
    null;
  const capabilityKey = String(metadata.capability_key || "");
  const capabilityId = String(metadata.capability_id || "");
  const capability =
    capabilities.find((item) => item.capability_key === capabilityKey) ||
    capabilities.find((item) => item.id === capabilityId) ||
    null;
  const semanticArgs = variant.fixed_semantic_arguments || {};
  const rawArgs = variant.fixed_raw_arguments || {};

  return (
    <InspectorShell
      title={variant.name}
      subtitle={`${variant.variant_key} · ${String(operation?.operation_key || metadata.operation_key || "-")}`}
      actions={
        <div className="flex flex-wrap items-center gap-2">
          {variant.pending_proposal_id ? <Badge variant="warning">Pending Proposal</Badge> : null}
          <Badge variant={semanticStatusBadgeVariant(variant.status)}>{variant.status || "unknown"}</Badge>
        </div>
      }
    >
      <div className="space-y-4">
        <div className="grid gap-2 sm:grid-cols-2">
          <MetaCard label="Variant ID" value={variant.id} />
          <MetaCard label="Operation ID" value={variant.operation_id} />
          <MetaCard label="Operation" value={operation?.name || String(metadata.operation_name || "-")} />
          <MetaCard label="Source" value={operation?.source_name || String(metadata.source_name || "-")} />
          <MetaCard label="Capability" value={capability?.name || capabilityKey || capabilityId || "-"} />
          <MetaCard label="Lifecycle" value={variant.lifecycle || "-"} />
        </div>

        <InspectorSection title="Summary">
          <InspectorSurface muted>
            <div className="space-y-3 text-sm text-muted-foreground">
              <p>{variant.description || "No description yet."}</p>
              <div className="flex flex-wrap gap-2">
                <Badge variant="info">{Object.keys(semanticArgs).length} semantic controls</Badge>
                <Badge variant="default">{Object.keys(rawArgs).length} raw controls</Badge>
                {capability ? <Badge variant="success">Capability linked</Badge> : <Badge variant="warning">Capability not linked</Badge>}
              </div>
            </div>
          </InspectorSurface>
        </InspectorSection>

        <InspectorSection title="Linked Context">
          <InspectorSurface>
            <div className="space-y-3 text-sm text-muted-foreground">
              <div>
                <div className="font-medium text-foreground">Operations</div>
                <div className="mt-1">
                  {operation
                    ? `${operation.name} · ${operation.http_method || "-"} ${operation.access_path_locator || operation.access_path_name || "-"}`
                    : "Operation details are not available in the current cache."}
                </div>
              </div>
              <div>
                <div className="font-medium text-foreground">Capability</div>
                <div className="mt-1">
                  {capability
                    ? `${capability.name} · ${capability.capability_key}`
                    : capabilityKey || capabilityId || "Link this variant to a capability through metadata."}
                </div>
              </div>
            </div>
          </InspectorSurface>
        </InspectorSection>

        <InspectorSection title="Fixed Semantic Arguments">
          <InspectorSurface>
            <InspectorJson value={semanticArgs} />
          </InspectorSurface>
        </InspectorSection>

        <InspectorSection title="Fixed Raw Arguments">
          <InspectorSurface>
            <InspectorJson value={rawArgs} />
          </InspectorSurface>
        </InspectorSection>

        <InspectorSection title="Metadata">
          <InspectorSurface>
            <InspectorJson value={metadata} />
          </InspectorSurface>
        </InspectorSection>
      </div>
    </InspectorShell>
  );
}
