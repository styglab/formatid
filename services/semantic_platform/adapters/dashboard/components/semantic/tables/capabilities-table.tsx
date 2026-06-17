"use client";

import type { Capability } from "@/types/semantic";
import { semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { Badge } from "@/components/ui/badge";

export function CapabilitiesTable({
  items,
  selectedId,
  onSelect
}: {
  items: Capability[];
  selectedId: string;
  onSelect: (id: string) => void;
}) {
  return (
      <table className="min-w-full text-left text-sm">
        <thead className="sticky top-0 z-10 border-b border-border/70 bg-background/95 text-xs uppercase tracking-[0.12em] text-muted-foreground backdrop-blur supports-[backdrop-filter]:bg-background/88">
          <tr>
            <th className="px-4 py-3 font-medium">Capability</th>
            <th className="px-4 py-3 font-medium">Namespace</th>
            <th className="px-4 py-3 font-medium">Inputs</th>
            <th className="px-4 py-3 font-medium">Status</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/70">
          {items.map((capability) => {
            const display = capability.draft_snapshot || capability;
            const selected = capability.id === selectedId;
            return (
              <tr
                key={capability.id}
                className={`cursor-pointer transition hover:bg-muted/20 ${selected ? "bg-primary/[0.07] shadow-[inset_3px_0_0_0_hsl(var(--primary))]" : "even:bg-muted/[0.08]"}`}
                onClick={() => onSelect(capability.id)}
              >
                <td className="px-4 py-3">
                  <div className="font-medium text-foreground">{display.name}</div>
                  <div className="mt-1 text-xs text-muted-foreground">{display.capability_key}</div>
                </td>
                <td className="px-4 py-3 text-muted-foreground">{display.namespace || "public"}</td>
                <td className="px-4 py-3 text-muted-foreground">{display.input_semantic_types?.length || 0}</td>
                <td className="px-4 py-3">
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant={semanticStatusBadgeVariant(display.status)}>{display.status || "unknown"}</Badge>
                    {capability.draft_snapshot ? <Badge variant="warning">Draft</Badge> : null}
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
  );
}
