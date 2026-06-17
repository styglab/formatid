"use client";

import type { Capability, OperationVariant } from "@/types/semantic";
import { semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { Badge } from "@/components/ui/badge";

export function VariantsTable({
  items,
  selectedId,
  onSelect,
  capabilities
}: {
  items: OperationVariant[];
  selectedId: string;
  onSelect: (id: string) => void;
  capabilities: Capability[];
}) {
  return (
      <table className="min-w-full text-left text-sm">
        <thead className="sticky top-0 z-10 border-b border-border/70 bg-background/95 text-xs uppercase tracking-[0.12em] text-muted-foreground backdrop-blur supports-[backdrop-filter]:bg-background/88">
          <tr>
            <th className="px-4 py-3 font-medium">Variant</th>
            <th className="px-4 py-3 font-medium">Operation</th>
            <th className="px-4 py-3 font-medium">Source</th>
            <th className="px-4 py-3 font-medium">Status</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/70">
          {items.map((variant) => {
            const selected = variant.id === selectedId;
            const metadata = variant.metadata || {};
            const capabilityKey = String(metadata.capability_key || "");
            const capabilityId = String(metadata.capability_id || "");
            const capability =
              capabilities.find((item) => item.capability_key === capabilityKey) ||
              capabilities.find((item) => item.id === capabilityId) ||
              null;
            return (
              <tr
                key={variant.id}
                className={`cursor-pointer transition hover:bg-muted/20 ${selected ? "bg-primary/[0.07] shadow-[inset_3px_0_0_0_hsl(var(--primary))]" : "even:bg-muted/[0.08]"}`}
                onClick={() => onSelect(variant.id)}
              >
                <td className="px-4 py-3">
                  <div className="font-medium text-foreground">{variant.name}</div>
                  <div className="mt-1 text-xs text-muted-foreground">{variant.variant_key}</div>
                  <div className="mt-1 text-xs text-muted-foreground">{capability?.name || capabilityKey || capabilityId || "unlinked capability"}</div>
                </td>
                <td className="px-4 py-3 text-muted-foreground">{String(metadata.operation_name || "-")}</td>
                <td className="px-4 py-3 text-muted-foreground">{String(metadata.source_name || "-")}</td>
                <td className="px-4 py-3">
                  <Badge variant={semanticStatusBadgeVariant(variant.status)}>{variant.status || "unknown"}</Badge>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
  );
}
