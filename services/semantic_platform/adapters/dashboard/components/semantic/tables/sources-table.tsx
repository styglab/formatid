"use client";

import type { ExecutionSource } from "@/types/semantic";
import { semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { Badge } from "@/components/ui/badge";

type SourcesTableProps = {
  items: ExecutionSource[];
  selectedId: string;
  onSelect: (id: string) => void;
};

export function SourcesTable({ items, selectedId, onSelect }: SourcesTableProps) {
  return (
      <table className="min-w-full text-left text-sm">
        <thead className="sticky top-0 z-10 border-b border-border/70 bg-background/95 text-xs uppercase tracking-[0.12em] text-muted-foreground backdrop-blur supports-[backdrop-filter]:bg-background/88">
          <tr>
            <th className="px-4 py-3 font-medium">Name</th>
            <th className="px-4 py-3 font-medium">Provider</th>
            <th className="px-4 py-3 font-medium">Type</th>
            <th className="px-4 py-3 font-medium">Status</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/70">
          {items.map((source) => {
            const display = source.draft_snapshot || source;
            const selected = source.id === selectedId;
            return (
              <tr
                key={source.id}
                className={`cursor-pointer transition hover:bg-muted/20 ${selected ? "bg-primary/[0.07] shadow-[inset_3px_0_0_0_hsl(var(--primary))]" : "even:bg-muted/[0.08]"}`}
                onClick={() => onSelect(source.id)}
              >
                <td className="px-4 py-3">
                  <div className="font-medium text-foreground">{display.name}</div>
                  <div className="mt-1 text-xs text-muted-foreground">{source.id}</div>
                </td>
                <td className="px-4 py-3 text-muted-foreground">{display.provider || "-"}</td>
                <td className="px-4 py-3 text-muted-foreground">{display.source_type}</td>
                <td className="px-4 py-3">
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant={semanticStatusBadgeVariant(display.status)}>{display.status || "unknown"}</Badge>
                    {source.draft_snapshot ? <Badge variant="warning">Draft</Badge> : null}
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
  );
}
