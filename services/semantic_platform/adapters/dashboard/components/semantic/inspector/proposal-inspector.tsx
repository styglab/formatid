import type { Proposal } from "@/types/governance";
import { MetaCard } from "@/components/semantic/common/meta-card";
import { ProposalDiffViewer } from "@/components/semantic/governance/proposal-diff-viewer";
import { InspectorEmpty, InspectorShell } from "@/components/semantic/inspector/inspector-shell";
import { formatSemanticDate, semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

export function ProposalInspector({
  proposal,
  submitting,
  onApprove,
  onReject
}: {
  proposal: Proposal | null;
  submitting: string;
  onApprove: (id: string) => void;
  onReject: (id: string) => void;
}) {
  if (!proposal) {
    return <InspectorEmpty title="Proposal Inspector" message="Select a proposal to inspect payload, change summary, and review actions." />;
  }

  return (
    <InspectorShell
      title={proposal.title}
      subtitle={`${proposal.entity_type} · ${proposal.change_type} · ${proposal.entity_id || "-"}`}
      actions={<Badge variant={semanticStatusBadgeVariant(proposal.status)}>{proposal.status}</Badge>}
    >
      <div className="space-y-4">
        <div className="grid gap-2 sm:grid-cols-2">
          <MetaCard label="Proposal ID" value={proposal.id} />
          <MetaCard label="Created" value={formatSemanticDate(proposal.created_at)} />
          <MetaCard label="Entity ID" value={proposal.entity_id || "-"} />
          <MetaCard label="Review Status" value={proposal.status} />
          <MetaCard label="Reviewed At" value={formatSemanticDate(proposal.reviewed_at)} />
          <MetaCard label="Reviewed By" value={proposal.reviewed_by || "-"} />
        </div>
        <ProposalDiffViewer proposal={proposal} />
        <div className="flex flex-wrap items-center gap-2">
          <Button type="button" size="sm" onClick={() => onApprove(proposal.id)} disabled={submitting.length > 0}>
            {submitting === `${proposal.id}:approve` ? "Approving..." : "Approve"}
          </Button>
          <Button type="button" size="sm" variant="outline" onClick={() => onReject(proposal.id)} disabled={submitting.length > 0}>
            {submitting === `${proposal.id}:reject` ? "Rejecting..." : "Reject"}
          </Button>
        </div>
      </div>
    </InspectorShell>
  );
}
