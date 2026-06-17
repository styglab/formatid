"use client";

import { useMemo } from "react";
import { RefreshCw } from "lucide-react";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { MetaCard, MetricCard } from "@/components/semantic/common/meta-card";
import { ErrorPanel, InfoLine, LoadingPanel } from "@/components/semantic/common/state-panel";
import { useCapabilities } from "@/hooks/semantic/use-capabilities";
import { useMappings } from "@/hooks/semantic/use-mappings";
import { useSemanticOverview } from "@/hooks/semantic/use-proposals";
import { useSources } from "@/hooks/semantic/use-sources";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function PublishPage() {
  const overview = useSemanticOverview();
  const sources = useSources({ page: 1, pageSize: 200 });
  const mappings = useMappings({ page: 1, pageSize: 200 });
  const capabilities = useCapabilities({ page: 1, pageSize: 200 });
  const loading = overview.loading || sources.loading || mappings.loading || capabilities.loading;
  const error = overview.error || sources.error || mappings.error || capabilities.error;

  const readiness = useMemo(() => {
    const pending = overview.data?.counts.pending_proposals ?? 0;
    const approvedSources = sources.data.items.filter((item) => (item.draft_snapshot || item).status === "approved").length;
    const approvedMappings = mappings.data.items.filter((item) => (item.draft_snapshot || item).status === "approved").length;
    const approvedCapabilities = capabilities.data.items.filter((item) => (item.draft_snapshot || item).status === "approved").length;
    return {
      pending,
      approvedSources,
      approvedMappings,
      approvedCapabilities,
      publishable: pending === 0 && approvedSources > 0 && approvedMappings > 0 && approvedCapabilities > 0,
    };
  }, [overview.data, sources.data, mappings.data, capabilities.data]);

  return (
    <SectionPlaceholder
      title="Publish"
      description="Review current approved context, pending review load, and publication readiness before runtime promotion."
      actions={
        <Button
          type="button"
          variant="outline"
          onClick={() => {
            void overview.reload();
            void sources.reload();
            void mappings.reload();
            void capabilities.reload();
          }}
          disabled={loading}
        >
          <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      }
      body={
        <div className="space-y-4">
          <InfoLine>
            <Badge variant={readiness.publishable ? "success" : "warning"}>
              {readiness.publishable ? "ready to publish" : "review required"}
            </Badge>
            <span>This route summarizes the current approved control-plane context even though snapshot history is not yet a first-class registry.</span>
          </InfoLine>

          {error ? <ErrorPanel message={error} /> : null}
          {loading ? <LoadingPanel message="Loading publication summary..." /> : null}

          {!loading && !error ? (
            <div className="grid gap-4 xl:grid-cols-[minmax(0,0.95fr)_minmax(0,1.05fr)]">
              <Card className="border-border/70">
                <CardHeader>
                  <CardTitle>Publication Readiness</CardTitle>
                </CardHeader>
                <CardContent className="grid gap-3 sm:grid-cols-2">
                  <MetricCard label="Pending Proposals" value={String(readiness.pending)} />
                  <MetricCard label="Approved Sources" value={String(readiness.approvedSources)} />
                  <MetricCard label="Approved Mappings" value={String(readiness.approvedMappings)} />
                  <MetricCard label="Approved Capabilities" value={String(readiness.approvedCapabilities)} />
                </CardContent>
              </Card>

              <Card className="border-border/70">
                <CardHeader>
                  <CardTitle>Current Snapshot</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="grid gap-3 sm:grid-cols-2">
                    <MetaCard label="Semantic Types" value={String(overview.data?.counts.semantic_types ?? 0)} />
                    <MetaCard label="Relationships" value={String(overview.data?.counts.relationships ?? 0)} />
                    <MetaCard label="Execution Sources" value={String(overview.data?.counts.execution_sources ?? 0)} />
                    <MetaCard label="Recent Pending" value={String(overview.data?.recent_proposals.length ?? 0)} />
                  </div>
                  <div className="rounded-xl border border-border/70 bg-muted/15 p-4 text-sm text-muted-foreground">
                    Publish snapshots are still modeled as a derived summary. The next backend step is explicit snapshot/version persistence with diff and promote actions.
                  </div>
                </CardContent>
              </Card>
            </div>
          ) : null}
        </div>
      }
    />
  );
}
