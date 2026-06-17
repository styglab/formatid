"use client";

import { useMemo, useState } from "react";
import { RefreshCw } from "lucide-react";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { MetaCard, MetricCard } from "@/components/semantic/common/meta-card";
import { EmptyPanel, ErrorPanel, InfoLine, LoadingPanel } from "@/components/semantic/common/state-panel";
import { FilterToolbar } from "@/components/semantic/filters/filter-toolbar";
import { useSemanticOverview } from "@/hooks/semantic/use-proposals";
import { formatSemanticDate, semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function AuditPage() {
  const { data, loading, error, reload } = useSemanticOverview();
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");

  const filtered = useMemo(() => {
    const items = data?.recent_proposals || [];
    return items.filter((proposal) => {
      const lowered = query.toLowerCase();
      const matchesQuery =
        !query ||
        proposal.title.toLowerCase().includes(lowered) ||
        proposal.entity_type.toLowerCase().includes(lowered) ||
        String(proposal.entity_id || "").toLowerCase().includes(lowered) ||
        String(proposal.reviewed_by || "").toLowerCase().includes(lowered);
      const matchesStatus = status === "all" || proposal.status === status;
      return matchesQuery && matchesStatus;
    });
  }, [data, query, status]);

  const metrics = useMemo(
    () => ({
      pending: data?.counts.pending_proposals ?? 0,
      reviewed: (data?.recent_proposals || []).filter((item) => item.reviewed_at).length,
      approved: (data?.recent_proposals || []).filter((item) => item.status === "approved").length,
      rejected: (data?.recent_proposals || []).filter((item) => item.status === "rejected").length,
    }),
    [data]
  );

  return (
    <SectionPlaceholder
      title="Audit"
      description="Inspect derived audit signals across proposal history, review timestamps, and approval state while a dedicated audit registry is still pending."
      actions={
        <Button type="button" variant="outline" onClick={() => void reload()} disabled={loading}>
          <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      }
      body={
        <div className="space-y-4">
          <InfoLine>
            <Badge variant="info">{filtered.length} audit rows</Badge>
            <span>This is a derived audit view built from governance summary data until first-class audit events are modeled in the backend.</span>
          </InfoLine>

          <FilterToolbar
            query={query}
            onQueryChange={setQuery}
            queryPlaceholder="Search audit activity by title, entity, or reviewer"
            status={status}
            onStatusChange={setStatus}
          />

          {error ? <ErrorPanel message={error} /> : null}
          {loading ? <LoadingPanel message="Loading audit history..." /> : null}

          {!loading && !error ? (
            <div className="grid gap-4 xl:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
              <Card className="border-border/70">
                <CardHeader>
                  <CardTitle>Audit Summary</CardTitle>
                </CardHeader>
                <CardContent className="grid gap-3 sm:grid-cols-2">
                  <MetricCard label="Pending Proposals" value={String(metrics.pending)} />
                  <MetricCard label="Reviewed Records" value={String(metrics.reviewed)} />
                  <MetricCard label="Approved Records" value={String(metrics.approved)} />
                  <MetricCard label="Rejected Records" value={String(metrics.rejected)} />
                </CardContent>
              </Card>

              <Card className="border-border/70">
                <CardHeader>
                  <CardTitle>Recent Audit Trail</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  {filtered.length ? (
                    filtered.map((proposal) => (
                      <div key={proposal.id} className="rounded-xl border border-border/70 px-4 py-3">
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div className="min-w-0">
                            <div className="truncate text-sm font-medium text-foreground">{proposal.title}</div>
                            <div className="mt-1 text-xs text-muted-foreground">
                              {proposal.entity_type} · {proposal.change_type} · {proposal.entity_id || "-"}
                            </div>
                          </div>
                          <Badge variant={semanticStatusBadgeVariant(proposal.status)}>{proposal.status}</Badge>
                        </div>
                        <div className="mt-3 grid gap-2 text-xs sm:grid-cols-2">
                          <MetaCard label="Created" value={formatSemanticDate(proposal.created_at)} />
                          <MetaCard label="Reviewed At" value={formatSemanticDate(proposal.reviewed_at)} />
                          <MetaCard label="Reviewed By" value={proposal.reviewed_by || "-"} />
                          <MetaCard label="Proposal ID" value={proposal.id} />
                        </div>
                      </div>
                    ))
                  ) : (
                    <EmptyPanel message="No audit rows match the current filters." />
                  )}
                </CardContent>
              </Card>
            </div>
          ) : null}
        </div>
      }
    />
  );
}
