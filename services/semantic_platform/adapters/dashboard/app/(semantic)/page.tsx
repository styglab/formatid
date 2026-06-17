"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { AlertTriangle, ArrowRight, ExternalLink, RefreshCw, Upload } from "lucide-react";
import {
  listExecutionSourcesPage,
  listMappings,
  listOnboardingRuns,
  listOperationFields,
} from "@/api/semantic-admin";
import { usePendingProposals, useSemanticOverview } from "@/hooks/semantic/use-proposals";
import { formatSemanticDate, semanticStatusBadgeVariant } from "@/lib/semantic/presenters";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { EmptyPanel, ErrorPanel, LoadingPanel } from "@/components/semantic/common/state-panel";
import { MetricCard } from "@/components/semantic/common/meta-card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { ExecutionSource, FieldMapping, OnboardingRun, OperationField, PaginatedResult } from "@/types/semantic";

type AttentionItem = {
  id: string;
  title: string;
  detail: string;
  href: string;
  variant: "warning" | "danger" | "info";
  badge: string;
};

export default function OverviewPage() {
  const overview = useSemanticOverview();
  const proposals = usePendingProposals({ page: 1, pageSize: 6 });
  const [sources, setSources] = useState<PaginatedResult<ExecutionSource>>({ items: [], total: 0, page: 1, page_size: 8 });
  const [runs, setRuns] = useState<OnboardingRun[]>([]);
  const [fields, setFields] = useState<OperationField[]>([]);
  const [mappings, setMappings] = useState<FieldMapping[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      const [nextSources, nextRuns, nextFields, nextMappings] = await Promise.all([
        listExecutionSourcesPage({ page: 1, pageSize: 8 }),
        listOnboardingRuns(),
        listOperationFields(),
        listMappings(),
      ]);
      setSources(nextSources);
      setRuns(nextRuns);
      setFields(nextFields);
      setMappings(nextMappings);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load onboarding overview.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, []);

  const loadingState = loading || overview.loading || proposals.loading;
  const combinedError = error || overview.error || proposals.error;

  const mappedFieldKeys = useMemo(
    () =>
      new Set(
        mappings.map((item) => `${item.operation_id}::${item.field_path.trim().toLowerCase()}`)
      ),
    [mappings]
  );

  const controlFieldCount = useMemo(
    () => fields.filter((field) => field.scope === "control").length,
    [fields]
  );

  const unmappedFieldCount = useMemo(
    () =>
      fields.filter((field) => {
        const fieldPath = String(field.field_path || field.raw_name).trim().toLowerCase();
        return !mappedFieldKeys.has(`${field.operation_id}::${fieldPath}`);
      }).length,
    [fields, mappedFieldKeys]
  );

  const publishReadySourceCount = useMemo(
    () =>
      sources.items.filter((item) => {
        const display = item.draft_snapshot || item;
        return display.status === "approved" || display.status === "published";
      }).length,
    [sources.items]
  );

  const reviewRunCount = useMemo(
    () => runs.filter((item) => item.pending_proposal_count > 0 || item.field_count > item.mapping_count).length,
    [runs]
  );

  const attentionItems = useMemo<AttentionItem[]>(() => {
    const items: AttentionItem[] = [];

    for (const run of runs) {
      if (run.pending_proposal_count > 0) {
        items.push({
          id: `${run.id}:pending`,
          title: run.source_name,
          detail: `${run.pending_proposal_count} pending proposals waiting in this onboarding run.`,
          href: `/onboarding-runs/${run.id}`,
          variant: "warning",
          badge: "Review queue"
        });
      } else if (run.field_count > run.mapping_count) {
        items.push({
          id: `${run.id}:coverage`,
          title: run.source_name,
          detail: `${Math.max(run.field_count - run.mapping_count, 0)} extracted fields still need mapping coverage.`,
          href: `/onboarding-runs/${run.id}`,
          variant: "info",
          badge: "Coverage gap"
        });
      } else if (run.suggestion_status && run.suggestion_status !== "complete") {
        items.push({
          id: `${run.id}:suggestion`,
          title: run.source_name,
          detail: `Suggestion pipeline is still in ${run.suggestion_status} state.`,
          href: `/onboarding-runs/${run.id}`,
          variant: "warning",
          badge: "Suggestion state"
        });
      }
    }

    return items.slice(0, 6);
  }, [runs]);

  const recentRuns = useMemo(
    () =>
      [...runs]
        .sort((left, right) => new Date(right.updated_at || right.created_at || 0).getTime() - new Date(left.updated_at || left.created_at || 0).getTime())
        .slice(0, 6),
    [runs]
  );

  return (
    <SectionPlaceholder
      title="Overview"
      description="Operator-first summary of source onboarding, review pressure, mapping coverage, and pending governance work."
      actions={
        <div className="flex flex-wrap items-center gap-2">
          <Button type="button" variant="default" asChild>
            <Link href="/sources">
              <Upload className="h-4 w-4" />
              Register Source
            </Link>
          </Button>
          <Button type="button" variant="outline" asChild>
            <Link href="/work-queue">
              Open Review Queue
            </Link>
          </Button>
          <Button
            type="button"
            variant="outline"
            onClick={() => {
              void reload();
              void overview.reload();
              void proposals.reload();
            }}
            disabled={loadingState}
          >
            <RefreshCw className={`h-4 w-4 ${loadingState ? "animate-spin" : ""}`} />
            Refresh
          </Button>
        </div>
      }
      body={
        <div className="space-y-5">
          {combinedError ? <ErrorPanel message={combinedError} /> : null}
          {loadingState ? <LoadingPanel message="Loading overview..." /> : null}

          {!loadingState && !combinedError ? (
            <>
              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
                <MetricCard label="Sources" value={String(overview.data?.counts.execution_sources ?? sources.total)} />
                <MetricCard label="Runs Needing Review" value={String(reviewRunCount)} />
                <MetricCard label="Pending Proposals" value={String(overview.data?.counts.pending_proposals ?? proposals.data.total)} />
                <MetricCard label="Extracted Fields" value={String(fields.length)} />
                <MetricCard label="Unmapped Fields" value={String(unmappedFieldCount)} />
                <MetricCard label="Control Fields" value={String(controlFieldCount)} />
              </div>

              <div className="grid gap-5 xl:grid-cols-[minmax(0,1.2fr)_minmax(360px,0.8fr)]">
                <Card className="border-border/70">
                  <CardHeader>
                    <div>
                      <CardTitle>Needs Attention</CardTitle>
                      <CardDescription>Review bottlenecks and coverage gaps that should be handled first.</CardDescription>
                    </div>
                    <Button type="button" variant="ghost" size="sm" asChild>
                      <Link href="/work-queue">
                        Open queue
                        <ArrowRight className="h-3.5 w-3.5" />
                      </Link>
                    </Button>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    {attentionItems.length ? (
                      attentionItems.map((item) => (
                        <Link
                          key={item.id}
                          href={item.href}
                          className="block rounded-xl border border-border/70 bg-muted/15 px-4 py-3 transition hover:bg-muted/25"
                        >
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0">
                              <div className="flex items-center gap-2">
                                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" />
                                <div className="truncate text-sm font-semibold text-foreground">{item.title}</div>
                              </div>
                              <div className="mt-1 text-sm text-muted-foreground">{item.detail}</div>
                            </div>
                            <Badge variant={item.variant}>{item.badge}</Badge>
                          </div>
                        </Link>
                      ))
                    ) : (
                      <EmptyPanel message="No immediate onboarding bottlenecks detected." />
                    )}
                  </CardContent>
                </Card>

                <Card className="border-border/70">
                  <CardHeader>
                    <div>
                      <CardTitle>Quick Actions</CardTitle>
                      <CardDescription>Jump into the most common operator tasks.</CardDescription>
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-2">
                    <QuickAction href="/sources" title="Register Source" detail="Add a new source row or upload a source document." />
                    <QuickAction href="/onboarding-runs" title="Inspect Runs" detail="Review recent onboarding execution runs and blockers." />
                    <QuickAction href="/source-operations" title="Review Operations" detail="Inspect discovered operation inventory before mapping." />
                    <QuickAction href="/schemas" title="Map Field Paths" detail="Resolve unmapped fields and control-path decisions." />
                    <QuickAction href="/proposals" title="Review Proposals" detail="Approve or reject pending semantic governance changes." />
                  </CardContent>
                </Card>
              </div>

              <div className="grid gap-5 xl:grid-cols-[minmax(0,1.05fr)_minmax(0,0.95fr)]">
                <Card className="border-border/70">
                  <CardHeader>
                    <div>
                      <CardTitle>Recent Runs</CardTitle>
                      <CardDescription>Latest onboarding activity across registered sources.</CardDescription>
                    </div>
                    <Button type="button" variant="ghost" size="sm" asChild>
                      <Link href="/onboarding-runs">
                        View all
                        <ExternalLink className="h-3.5 w-3.5" />
                      </Link>
                    </Button>
                  </CardHeader>
                  <CardContent>
                    {recentRuns.length ? (
                      <table className="min-w-full text-left text-sm">
                        <thead className="border-b border-border/70 text-xs uppercase tracking-[0.12em] text-muted-foreground">
                          <tr>
                            <th className="px-2 py-2 font-medium">Source</th>
                            <th className="px-2 py-2 font-medium">Coverage</th>
                            <th className="px-2 py-2 font-medium">Status</th>
                            <th className="px-2 py-2 font-medium">Updated</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-border/70">
                          {recentRuns.map((run) => (
                            <tr key={run.id} className="align-top even:bg-muted/[0.08]">
                              <td className="px-2 py-3">
                                <Link href={`/onboarding-runs/${run.id}`} className="font-medium text-foreground transition hover:text-primary">
                                  {run.source_name}
                                </Link>
                                <div className="mt-1 text-xs text-muted-foreground">{run.stage || "source_uploaded"}</div>
                              </td>
                              <td className="px-2 py-3 text-muted-foreground">
                                {run.mapping_count}/{run.field_count} mapped
                              </td>
                              <td className="px-2 py-3">
                                <div className="flex flex-wrap gap-1.5">
                                  <Badge variant={semanticStatusBadgeVariant(run.status)}>{run.status}</Badge>
                                  {run.pending_proposal_count ? <Badge variant="warning">{run.pending_proposal_count} pending</Badge> : null}
                                </div>
                              </td>
                              <td className="px-2 py-3 text-muted-foreground">{formatSemanticDate(run.updated_at || run.created_at)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    ) : (
                      <EmptyPanel message="No onboarding runs yet. Register a source to start." />
                    )}
                  </CardContent>
                </Card>

                <Card className="border-border/70">
                  <CardHeader>
                    <div>
                      <CardTitle>Pending Proposals</CardTitle>
                      <CardDescription>Latest governance changes waiting for review.</CardDescription>
                    </div>
                    <Button type="button" variant="ghost" size="sm" asChild>
                      <Link href="/proposals">
                        Review all
                        <ExternalLink className="h-3.5 w-3.5" />
                      </Link>
                    </Button>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    {proposals.data.items.length ? (
                      proposals.data.items.map((proposal) => (
                        <Link
                          key={proposal.id}
                          href={`/proposals?query=${encodeURIComponent(proposal.id)}`}
                          className="block rounded-xl border border-border/70 px-4 py-3 transition hover:bg-muted/20"
                        >
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0">
                              <div className="truncate text-sm font-semibold text-foreground">{proposal.title}</div>
                              <div className="mt-1 text-xs text-muted-foreground">
                                {proposal.entity_type} · {proposal.change_type} · {formatSemanticDate(proposal.created_at)}
                              </div>
                            </div>
                            <Badge variant={semanticStatusBadgeVariant(proposal.status)}>{proposal.status}</Badge>
                          </div>
                        </Link>
                      ))
                    ) : (
                      <EmptyPanel message="No pending proposals at the moment." />
                    )}
                  </CardContent>
                </Card>
              </div>

              <div className="grid gap-5 xl:grid-cols-[minmax(0,0.95fr)_minmax(0,1.05fr)]">
                <Card className="border-border/70">
                  <CardHeader>
                    <div>
                      <CardTitle>Pipeline Snapshot</CardTitle>
                      <CardDescription>High-level registry and onboarding activity indicators.</CardDescription>
                    </div>
                  </CardHeader>
                  <CardContent className="grid gap-3 sm:grid-cols-2">
                    <SnapshotLine label="Semantic Types" value={String(overview.data?.counts.semantic_types ?? 0)} />
                    <SnapshotLine label="Relationships" value={String(overview.data?.counts.relationships ?? 0)} />
                    <SnapshotLine label="Draft Types" value={String(overview.data?.counts.draft_semantic_types ?? 0)} />
                    <SnapshotLine label="Approved Types" value={String(overview.data?.counts.approved_semantic_types ?? 0)} />
                    <SnapshotLine label="Mapped Fields" value={String(fields.length - unmappedFieldCount)} />
                    <SnapshotLine label="Publish-ready Sources" value={String(publishReadySourceCount)} />
                  </CardContent>
                </Card>

                <Card className="border-border/70">
                  <CardHeader>
                    <div>
                      <CardTitle>Recent Sources</CardTitle>
                      <CardDescription>Newest source entries and their current lifecycle state.</CardDescription>
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    {sources.items.length ? (
                      sources.items.map((source) => {
                        const display = source.draft_snapshot || source;
                        return (
                          <div key={source.id} className="rounded-xl border border-border/70 px-4 py-3">
                            <div className="flex items-start justify-between gap-3">
                              <div className="min-w-0">
                                <Link href="/sources" className="truncate text-sm font-semibold text-foreground transition hover:text-primary">
                                  {display.name}
                                </Link>
                                <div className="mt-1 text-xs text-muted-foreground">
                                  {display.provider || "no provider"} · {display.source_type} · {formatSemanticDate(display.updated_at || display.created_at)}
                                </div>
                              </div>
                              <Badge variant={semanticStatusBadgeVariant(display.status)}>{display.status || "unknown"}</Badge>
                            </div>
                          </div>
                        );
                      })
                    ) : (
                      <EmptyPanel message="No sources registered yet." />
                    )}
                  </CardContent>
                </Card>
              </div>
            </>
          ) : null}
        </div>
      }
    />
  );
}

function QuickAction({ href, title, detail }: { href: string; title: string; detail: string }) {
  return (
    <Link
      href={href}
      className="flex items-center justify-between gap-3 rounded-xl border border-border/70 px-4 py-3 transition hover:bg-muted/20"
    >
      <div className="min-w-0">
        <div className="text-sm font-medium text-foreground">{title}</div>
        <div className="mt-1 text-xs text-muted-foreground">{detail}</div>
      </div>
      <ArrowRight className="h-4 w-4 shrink-0 text-muted-foreground" />
    </Link>
  );
}

function SnapshotLine({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl border border-border/70 bg-muted/10 px-4 py-3">
      <div className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">{label}</div>
      <div className="mt-2 text-lg font-semibold text-foreground">{value}</div>
    </div>
  );
}
