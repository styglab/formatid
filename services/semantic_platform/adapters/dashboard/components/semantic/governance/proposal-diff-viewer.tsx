import type { Proposal } from "@/types/governance";
import { proposalChangedFields, proposalEvidence, proposalPayload, proposalSnapshots, summarizeProposal } from "@/lib/semantic/proposals";
import { Badge } from "@/components/ui/badge";

function SnapshotCard({
  title,
  snapshot
}: {
  title: string;
  snapshot: Record<string, unknown> | null;
}) {
  return (
    <div className="rounded-xl border border-border/70 bg-muted/10 p-4">
      <div className="mb-2 text-xs uppercase tracking-[0.12em] text-muted-foreground">{title}</div>
      {snapshot ? (
        <pre className="overflow-x-auto whitespace-pre-wrap text-xs leading-6 text-foreground">{JSON.stringify(snapshot, null, 2)}</pre>
      ) : (
        <div className="text-sm text-muted-foreground">No snapshot available.</div>
      )}
    </div>
  );
}

export function ProposalDiffViewer({ proposal }: { proposal: Proposal }) {
  const payload = proposalPayload(proposal);
  const { approved, draft } = proposalSnapshots(proposal);
  const changedFields = proposalChangedFields(proposal);
  const evidence = proposalEvidence(proposal);

  return (
    <div className="space-y-4">
      <div className="rounded-xl border border-border/70 bg-muted/15 p-4">
        <div className="text-xs uppercase tracking-[0.12em] text-muted-foreground">Change Summary</div>
        <div className="mt-2 text-sm font-medium text-foreground">{summarizeProposal(proposal)}</div>
        <div className="mt-3 flex flex-wrap gap-2">
          <Badge variant="info">{proposal.change_type}</Badge>
          <Badge variant="default">{proposal.entity_type}</Badge>
          <Badge variant={evidence.length ? "success" : "warning"}>{evidence.length} evidence</Badge>
          <Badge variant={changedFields.length ? "info" : "default"}>{changedFields.length} changed fields</Badge>
        </div>
      </div>

      {changedFields.length ? (
        <div className="space-y-2">
          <div className="text-xs uppercase tracking-[0.12em] text-muted-foreground">Changed Fields</div>
          <div className="flex flex-wrap gap-2">
            {changedFields.map((field) => (
              <Badge key={field} variant="default">
                {field}
              </Badge>
            ))}
          </div>
        </div>
      ) : null}

      {approved || draft ? (
        <div className="grid gap-4 lg:grid-cols-2">
          <SnapshotCard title="Approved Snapshot" snapshot={approved} />
          <SnapshotCard title="Draft Snapshot" snapshot={draft} />
        </div>
      ) : (
        <SnapshotCard title="Payload" snapshot={payload} />
      )}

      <div className="space-y-2">
        <div className="text-xs uppercase tracking-[0.12em] text-muted-foreground">Evidence</div>
        {evidence.length ? (
          <div className="space-y-2">
            {evidence.map((item, index) => (
              <div key={`${proposal.id}-evidence-${index}`} className="rounded-xl border border-border/70 bg-background p-3">
                <pre className="overflow-x-auto whitespace-pre-wrap text-xs leading-6 text-foreground">{JSON.stringify(item, null, 2)}</pre>
              </div>
            ))}
          </div>
        ) : (
          <div className="rounded-xl border border-dashed border-border/70 bg-muted/10 p-4 text-sm text-muted-foreground">
            No structured evidence is attached to this proposal yet.
          </div>
        )}
      </div>

      <div className="space-y-2">
        <div className="text-xs uppercase tracking-[0.12em] text-muted-foreground">Raw Payload</div>
        <div className="rounded-xl border border-border/70 bg-background p-4">
          <pre className="overflow-x-auto whitespace-pre-wrap text-xs leading-6 text-foreground">{JSON.stringify(payload, null, 2)}</pre>
        </div>
      </div>
    </div>
  );
}
