"use client";

import type { ExecutionOperation } from "@/types/semantic";
import { semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { Badge } from "@/components/ui/badge";

export function SchemaOperationsTable({
  items,
  selectedId,
  onSelect,
  fieldCounts
}: {
  items: ExecutionOperation[];
  selectedId: string;
  onSelect: (id: string) => void;
  fieldCounts: Record<string, number>;
}) {
  return (
      <table className="min-w-full text-left text-sm">
        <thead className="sticky top-0 z-10 border-b border-border/70 bg-background/95 text-xs uppercase tracking-[0.12em] text-muted-foreground backdrop-blur supports-[backdrop-filter]:bg-background/88">
          <tr>
            <th className="px-4 py-3 font-medium">Operation</th>
            <th className="px-4 py-3 font-medium">Source</th>
            <th className="px-4 py-3 font-medium">Fields</th>
            <th className="px-4 py-3 font-medium">Status</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/70">
          {items.map((operation) => {
            const selected = operation.id === selectedId;
            return (
              <tr
                key={operation.id}
                className={`cursor-pointer transition hover:bg-muted/20 ${selected ? "bg-primary/[0.07] shadow-[inset_3px_0_0_0_hsl(var(--primary))]" : "even:bg-muted/[0.08]"}`}
                onClick={() => onSelect(operation.id)}
              >
                <td className="px-4 py-3">
                  <div className="font-medium text-foreground">{operation.name}</div>
                  <div className="mt-1 text-xs text-muted-foreground">{operation.operation_key}</div>
                </td>
                <td className="px-4 py-3 text-muted-foreground">{operation.source_name || "-"}</td>
                <td className="px-4 py-3 text-muted-foreground">{fieldCounts[operation.id] || 0}</td>
                <td className="px-4 py-3">
                  <Badge variant={semanticStatusBadgeVariant(operation.status)}>{operation.status || "unknown"}</Badge>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
  );
}
