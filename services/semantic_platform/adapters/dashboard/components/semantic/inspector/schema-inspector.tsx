import type { ExecutionOperation, OperationField } from "@/types/semantic";
import { MetaCard } from "@/components/semantic/common/meta-card";
import { InspectorEmpty, InspectorShell } from "@/components/semantic/inspector/inspector-shell";
import { semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { Badge } from "@/components/ui/badge";

export function SchemaInspector({
  operation,
  fields,
  mappedPaths,
  mappedCount,
  unmappedCount,
  controlCount,
  onCreateMapping
}: {
  operation: ExecutionOperation | null;
  fields: OperationField[];
  mappedPaths: Set<string>;
  mappedCount: number;
  unmappedCount: number;
  controlCount: number;
  onCreateMapping?: (field: OperationField) => void;
}) {
  if (!operation) {
    return <InspectorEmpty title="Schema Inspector" message="Select an operation to inspect extracted request and response field paths." />;
  }

  const grouped = {
    input: fields.filter((field) => field.scope === "input"),
    output: fields.filter((field) => field.scope === "output"),
    control: fields.filter((field) => field.scope === "control")
  };

  return (
    <InspectorShell
      title={operation.name}
      subtitle={`${operation.operation_key} · schema paths`}
      actions={<Badge variant={semanticStatusBadgeVariant(operation.status)}>{operation.status || "unknown"}</Badge>}
    >
      <div className="space-y-4">
        <div className="grid gap-2 sm:grid-cols-2">
          <MetaCard label="Operation ID" value={operation.id} />
          <MetaCard label="Access Path" value={operation.access_path_locator || operation.access_path_name || "-"} />
          <MetaCard label="Input Fields" value={String(grouped.input.length)} />
          <MetaCard label="Output Fields" value={String(grouped.output.length)} />
          <MetaCard label="Mapped Paths" value={String(mappedCount)} />
          <MetaCard label="Unmapped Paths" value={String(unmappedCount)} />
        </div>
        <div className="flex flex-wrap items-center gap-2 rounded-xl border border-border/70 bg-muted/15 px-4 py-3 text-xs text-muted-foreground">
          <Badge variant="success">{mappedCount} mapped</Badge>
          <Badge variant="warning">{unmappedCount} unmapped</Badge>
          <Badge variant="default">{controlCount} control</Badge>
          <span>Create mappings from unmapped fields first. Control fields usually become variant or semantic control mappings.</span>
        </div>

        <SchemaFieldSection title="Input Fields" items={grouped.input} mappedPaths={mappedPaths} onCreateMapping={onCreateMapping} />
        <SchemaFieldSection title="Output Fields" items={grouped.output} mappedPaths={mappedPaths} onCreateMapping={onCreateMapping} />
        <SchemaFieldSection title="Control Fields" items={grouped.control} mappedPaths={mappedPaths} onCreateMapping={onCreateMapping} />
      </div>
    </InspectorShell>
  );
}

function SchemaFieldSection({
  title,
  items,
  mappedPaths,
  onCreateMapping
}: {
  title: string;
  items: OperationField[];
  mappedPaths: Set<string>;
  onCreateMapping?: (field: OperationField) => void;
}) {
  return (
    <section className="space-y-2">
      <div className="text-xs uppercase tracking-[0.12em] text-muted-foreground">{title}</div>
      <div className="max-h-48 space-y-2 overflow-auto">
        {items.length ? (
          items.map((field) => {
            const fieldPath = String(field.field_path || field.raw_name);
            const mapped = mappedPaths.has(`${field.operation_id}::${fieldPath.trim().toLowerCase()}`);
            return (
            <div key={field.id} className="rounded-lg border border-border/70 px-3 py-2 text-sm">
              <div className="flex items-center justify-between gap-3">
                <div className="font-medium text-foreground">{field.raw_name}</div>
                <Badge variant={mapped ? "success" : "warning"}>{mapped ? "Mapped" : "Unmapped"}</Badge>
              </div>
              <div className="mt-1 text-xs text-muted-foreground">
                {fieldPath} · {field.data_type || "string"} · {field.is_required ? "required" : "optional"}
              </div>
              {onCreateMapping ? (
                <button
                  type="button"
                  className="mt-2 text-xs font-medium text-primary transition hover:opacity-80"
                  onClick={() => onCreateMapping(field)}
                >
                  {mapped ? "Add Another Mapping" : "Create Mapping"}
                </button>
              ) : null}
            </div>
          );
          })
        ) : (
          <div className="rounded-lg border border-dashed border-border px-3 py-4 text-sm text-muted-foreground">
            No fields in this scope.
          </div>
        )}
      </div>
    </section>
  );
}
