import type { ExecutionOperation, OperationField } from "@/types/semantic";
import { MetaCard } from "@/components/semantic/common/meta-card";
import { InspectorEmpty, InspectorShell } from "@/components/semantic/inspector/inspector-shell";
import { InspectorSection, InspectorSurface } from "@/components/semantic/inspector/inspector-section";
import { semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { Badge } from "@/components/ui/badge";

export function OperationInspector({
  operation,
  fields
}: {
  operation: ExecutionOperation | null;
  fields: OperationField[];
}) {
  if (!operation) {
    return <InspectorEmpty title="Operation Inspector" message="Select an operation to inspect its access path and extracted field context." />;
  }

  return (
    <InspectorShell
      title={operation.name}
      subtitle={`${operation.operation_key} · ${operation.http_method || operation.access_type || "access path"}`}
      actions={<Badge variant={semanticStatusBadgeVariant(operation.status)}>{operation.status || "unknown"}</Badge>}
    >
      <div className="space-y-4">
        <InspectorSection title="Summary">
          <div className="grid gap-2 sm:grid-cols-2">
            <MetaCard label="Operation ID" value={operation.id} />
            <MetaCard label="Namespace" value={operation.namespace || "-"} />
            <MetaCard label="Source" value={operation.source_name || "-"} />
            <MetaCard label="Asset" value={operation.asset_name || "-"} />
            <MetaCard label="Access Path" value={operation.access_path_locator || operation.access_path_name || "-"} />
            <MetaCard label="Fields" value={String(fields.length)} />
          </div>
          <InspectorSurface muted>
            <div className="text-sm text-muted-foreground">{operation.description || "No description yet."}</div>
          </InspectorSurface>
        </InspectorSection>
        <InspectorSection title="Extracted Fields">
          <div className="max-h-64 space-y-2 overflow-auto">
            {fields.length ? (
              fields.map((field) => (
                <div key={field.id} className="rounded-lg border border-border/70 px-3 py-2 text-sm">
                  <div className="font-medium text-foreground">{field.raw_name}</div>
                  <div className="mt-1 text-xs text-muted-foreground">
                    {field.scope} · {field.field_path || "-"} · {field.data_type || "string"}
                  </div>
                </div>
              ))
            ) : (
              <div className="rounded-lg border border-dashed border-border px-3 py-4 text-sm text-muted-foreground">
                No fields extracted yet.
              </div>
            )}
          </div>
        </InspectorSection>
        <InspectorSection title="Linked Context">
          <div className="grid gap-2 sm:grid-cols-2">
            <MetaCard label="HTTP Method" value={operation.http_method || "-"} />
            <MetaCard label="Access Type" value={operation.access_type || "-"} />
            <MetaCard label="Version" value={operation.version || "-"} />
            <MetaCard label="Lifecycle" value={operation.lifecycle || "-"} />
          </div>
        </InspectorSection>
      </div>
    </InspectorShell>
  );
}
