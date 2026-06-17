"use client";

import type { Proposal } from "@/types/governance";
import { summarizeProposal } from "@/lib/semantic/proposals";
import { formatSemanticDate, semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { Badge } from "@/components/ui/badge";

export function ProposalsTable({
  items,
  selectedId,
  onSelect
}: {
  items: Proposal[];
  selectedId: string;
  onSelect: (id: string) => void;
}) {
  return (
      <table className="min-w-full text-left text-sm">
        <thead className="sticky top-0 z-10 border-b border-border/70 bg-background/95 text-xs uppercase tracking-[0.12em] text-muted-foreground backdrop-blur supports-[backdrop-filter]:bg-background/88">
          <tr>
            <th className="px-4 py-3 font-medium">Title</th>
            <th className="px-4 py-3 font-medium">Entity</th>
            <th className="px-4 py-3 font-medium">Change</th>
            <th className="px-4 py-3 font-medium">Status</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/70">
          {items.map((proposal) => {
            const selected = proposal.id === selectedId;
            return (
              <tr
                key={proposal.id}
                className={`cursor-pointer transition hover:bg-muted/20 ${selected ? "bg-primary/[0.07] shadow-[inset_3px_0_0_0_hsl(var(--primary))]" : "even:bg-muted/[0.08]"}`}
                onClick={() => onSelect(proposal.id)}
              >
                <td className="px-4 py-3">
                  <div className="font-medium text-foreground">{proposal.title}</div>
                  <div className="mt-1 text-xs text-muted-foreground">{summarizeProposal(proposal)}</div>
                  <div className="mt-1 text-[11px] text-muted-foreground/80">
                    {proposal.id} · {formatSemanticDate(proposal.created_at)}
                  </div>
                </td>
                <td className="px-4 py-3 text-muted-foreground">{proposal.entity_type}</td>
                <td className="px-4 py-3 text-muted-foreground">{proposal.change_type}</td>
                <td className="px-4 py-3">
                  <Badge variant={semanticStatusBadgeVariant(proposal.status)}>{proposal.status}</Badge>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
  );
}
