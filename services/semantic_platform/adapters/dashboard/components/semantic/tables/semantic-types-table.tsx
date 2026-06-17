"use client";

import type { SemanticType } from "@/types/semantic";
import { semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { Badge } from "@/components/ui/badge";

export function SemanticTypesTable({
  items,
  selectedId,
  onSelect
}: {
  items: SemanticType[];
  selectedId: string;
  onSelect: (id: string) => void;
}) {
  return (
    <div className="overflow-hidden rounded-xl border border-border/70">
      <table className="min-w-full divide-y divide-border/70 text-left text-sm">
        <thead className="bg-muted/30 text-xs uppercase tracking-[0.12em] text-muted-foreground">
          <tr>
            <th className="px-4 py-3 font-medium">Semantic Type</th>
            <th className="px-4 py-3 font-medium">Kind</th>
            <th className="px-4 py-3 font-medium">Datatype</th>
            <th className="px-4 py-3 font-medium">Status</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/70">
          {items.map((item) => {
            const display = item.draft_snapshot || item;
            const selected = item.id === selectedId;
            return (
              <tr
                key={item.id}
                className={`cursor-pointer transition hover:bg-muted/30 ${selected ? "bg-primary/5" : ""}`}
                onClick={() => onSelect(item.id)}
              >
                <td className="px-4 py-3">
                  <div className="font-medium text-foreground">{display.name}</div>
                  <div className="mt-1 text-xs text-muted-foreground">{item.id}</div>
                </td>
                <td className="px-4 py-3 text-muted-foreground">{display.entity_kind || "-"}</td>
                <td className="px-4 py-3 text-muted-foreground">{display.datatype || "-"}</td>
                <td className="px-4 py-3">
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant={semanticStatusBadgeVariant(display.status)}>{display.status || "unknown"}</Badge>
                    {item.draft_snapshot ? <Badge variant="warning">Draft</Badge> : null}
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
