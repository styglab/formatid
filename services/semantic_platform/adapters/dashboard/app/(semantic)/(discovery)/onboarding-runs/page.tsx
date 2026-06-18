"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { RefreshCw } from "lucide-react";
import { listOnboardingRuns } from "@/api/semantic-admin";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { TablePanel } from "@/components/layout/table-panel";
import { InspectorPanel } from "@/components/layout/inspector-panel";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  WORKBENCH_PROGRESS_LABELS,
  WORKBENCH_PROGRESS_STEPS,
  workbenchStepForStage,
} from "@/lib/semantic/onboarding";
import { EmptyPanel, ErrorPanel, InfoLine, LoadingPanel } from "@/components/semantic/common/state-panel";
import { MetricCard } from "@/components/semantic/common/meta-card";
import type { OnboardingRun } from "@/types/semantic";

function stepperCircleClass(state: "completed" | "current" | "upcoming" | "blocked") {
  if (state === "completed") return "border-emerald-500 bg-emerald-500 text-white";
  if (state === "current") return "border-primary bg-primary text-primary-foreground";
  if (state === "blocked") return "border-amber-500 bg-amber-500 text-white";
  return "border-border bg-background text-muted-foreground";
}

function stepperLineClass(state: "completed" | "current" | "upcoming" | "blocked") {
  if (state === "completed") return "bg-emerald-500";
  if (state === "current") return "bg-primary/40";
  if (state === "blocked") return "bg-amber-500";
  return "bg-border";
}

function preparationVariant(status?: string) {
  if (status === "blocked") return "warning" as const;
  if (status === "completed") return "success" as const;
  if (status === "ready") return "default" as const;
  return "info" as const;
}

function preparationLabel(status?: string) {
  if (status === "blocked") return "Draft blocked";
  if (status === "completed") return "Publish ready";
  if (status === "ready") return "Ready for review";
  return "AI drafting";
}

export default function OnboardingRunsPage() {
  const [items, setItems] = useState<OnboardingRun[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      const next = await listOnboardingRuns();
      setItems(next);
      setSelectedId((current) => current && next.some((item) => item.id === current) ? current : next[0]?.id || "");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load onboarding runs.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, []);

  const selected = useMemo(() => items.find((item) => item.id === selectedId) || items[0] || null, [items, selectedId]);

  return (
    <SectionPlaceholder
      title="Onboarding Workspaces"
      description="Track each workspace by its current review phase, continue the next approval, and publish approved changes when ready."
      actions={
        <Button type="button" variant="outline" onClick={() => void reload()} disabled={loading}>
          <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      }
      body={
        <div className="space-y-4">
          <InfoLine>
            <Badge variant="info">{items.length} workspaces</Badge>
            <Badge variant="warning">{items.reduce((total, item) => total + item.pending_proposal_count, 0)} pending proposals</Badge>
          </InfoLine>
          {error ? <ErrorPanel message={error} /> : null}
          {loading ? <LoadingPanel message="Loading onboarding runs..." /> : null}
          {!loading && !error ? (
            <div className="grid gap-5 xl:grid-cols-[minmax(0,1.1fr)_380px]">
              <TablePanel>
                <table className="min-w-full table-fixed text-left text-[12px]">
                  <thead className="sticky top-0 z-10 border-b border-border/70 bg-slate-50/70 text-[10px] uppercase tracking-[0.08em] text-muted-foreground backdrop-blur">
                    <tr>
                      <th className="w-[28%] px-3 py-2.5 font-medium">Workspace</th>
                      <th className="w-[34%] px-3 py-2.5 font-medium">Workflow</th>
                      <th className="w-[18%] px-3 py-2.5 font-medium">Progress</th>
                      <th className="w-[10%] px-3 py-2.5 font-medium">Status</th>
                      <th className="w-[10%] px-3 py-2.5 font-medium">Next</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-200/80">
                    {items.map((item) => (
                      <tr key={item.id} className={`cursor-pointer transition hover:bg-primary/[0.04] ${selected?.id === item.id ? "bg-primary/[0.08] shadow-[inset_2px_0_0_0_hsl(var(--primary))]" : ""}`} onClick={() => setSelectedId(item.id)}>
                        <td className="px-3 py-2.5">
                          <div className="truncate text-[13px] font-semibold text-foreground">{item.source_name}</div>
                          <div className="mt-0.5 truncate font-mono text-[10px] text-muted-foreground">{item.id}</div>
                        </td>
                        <td className="px-3 py-2.5">
                          {item.preparation_status === "preparing" || item.preparation_status === "blocked" ? (
                            <div className="space-y-2">
                              <div className="flex items-center justify-between gap-2">
                                <div className="text-[11px] font-medium text-foreground">{preparationLabel(item.preparation_status)}</div>
                                <div className="text-[11px] text-muted-foreground">{item.worker_progress_percent ?? 0}%</div>
                              </div>
                              <div className="h-2 overflow-hidden rounded-full bg-muted">
                                <div
                                  className={`h-full rounded-full ${item.preparation_status === "blocked" ? "bg-amber-500" : "bg-primary"}`}
                                  style={{ width: `${item.worker_progress_percent ?? 0}%` }}
                                />
                              </div>
                              <div className="text-[11px] text-muted-foreground">
                                {item.worker_current_task || "Generating AI drafts for workspace tasks."}
                              </div>
                            </div>
                          ) : (
                            <>
                              <div className="flex items-center gap-1">
                                {WORKBENCH_PROGRESS_STEPS.map((step, index) => {
                                  const current = workbenchStepForStage(item.current_stage);
                                  const currentIndex = WORKBENCH_PROGRESS_STEPS.indexOf(current);
                                  const targetIndex = WORKBENCH_PROGRESS_STEPS.indexOf(step);
                                  const state =
                                    targetIndex < currentIndex
                                      ? "completed"
                                      : targetIndex === currentIndex
                                        ? "current"
                                        : "upcoming";
                                  return (
                                    <div key={step} className="flex min-w-0 flex-1 items-center gap-1">
                                      <div className={`flex h-5 w-5 shrink-0 items-center justify-center rounded-full border text-[10px] font-semibold ${stepperCircleClass(state)}`}>
                                        {state === "completed" ? "✓" : targetIndex + 1}
                                      </div>
                                      {index < WORKBENCH_PROGRESS_STEPS.length - 1 ? (
                                        <div className={`h-0.5 min-w-2 flex-1 rounded-full ${stepperLineClass(state)}`} />
                                      ) : null}
                                    </div>
                                  );
                                })}
                              </div>
                              <div className="mt-2 text-[11px] text-muted-foreground">
                                Current: {WORKBENCH_PROGRESS_LABELS[workbenchStepForStage(item.current_stage)]}
                              </div>
                            </>
                          )}
                        </td>
                        <td className="px-3 py-2.5 text-muted-foreground">
                          {item.preparation_status === "preparing" || item.preparation_status === "blocked" ? (
                            <div className="space-y-1 text-[11px]">
                              <div>{item.draft_ready_count || 0} ready</div>
                              <div>{item.draft_active_count || 0} drafting · {item.draft_queued_count || 0} queued</div>
                              {item.draft_failed_count ? <div className="text-amber-600">{item.draft_failed_count} failed</div> : null}
                            </div>
                          ) : (
                            <div className="space-y-1 text-[11px]">
                              <div>{item.operation_count} ops · {item.field_count} fields</div>
                              <div>{item.current_stage_completed_count || 0}/{item.current_stage_task_count || 0} tasks in focus</div>
                            </div>
                          )}
                        </td>
                        <td className="px-3 py-2.5">
                          <div><Badge variant={item.preparation_status ? preparationVariant(item.preparation_status) : item.pending_proposal_count ? "warning" : "default"}>{item.preparation_status ? preparationLabel(item.preparation_status) : item.stage_status || item.status}</Badge></div>
                          <div className="mt-1 text-[11px] text-muted-foreground">{item.pending_proposal_count} pending</div>
                        </td>
                        <td className="px-3 py-2.5">
                          <Button type="button" size="sm" variant="ghost" asChild>
                            <Link href={`/onboarding-runs/${item.id}`}>Continue</Link>
                          </Button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </TablePanel>
              <InspectorPanel>
                {selected ? (
                  <div className="space-y-4 p-4">
                    <div>
                      <div className="text-sm font-semibold text-foreground">{selected.source_name}</div>
                      <div className="mt-1 font-mono text-[11px] text-muted-foreground">{selected.evidence_snapshot_id}</div>
                    </div>
                    <div className="space-y-2">
                      {selected.preparation_status === "preparing" || selected.preparation_status === "blocked" ? (
                        <div className="space-y-2">
                          <div className="flex items-center justify-between gap-2 text-[11px]">
                            <span className="font-medium text-foreground">{preparationLabel(selected.preparation_status)}</span>
                            <span className="text-muted-foreground">{selected.worker_progress_percent ?? 0}%</span>
                          </div>
                          <div className="h-2 overflow-hidden rounded-full bg-muted">
                            <div
                              className={`h-full rounded-full ${selected.preparation_status === "blocked" ? "bg-amber-500" : "bg-primary"}`}
                              style={{ width: `${selected.worker_progress_percent ?? 0}%` }}
                            />
                          </div>
                          <div className="text-[11px] text-muted-foreground">{selected.worker_current_task || "Preparing AI drafts for review."}</div>
                        </div>
                      ) : (
                        <>
                          <div className="flex items-center gap-1">
                            {WORKBENCH_PROGRESS_STEPS.map((step, index) => {
                              const current = workbenchStepForStage(selected.current_stage);
                              const currentIndex = WORKBENCH_PROGRESS_STEPS.indexOf(current);
                              const targetIndex = WORKBENCH_PROGRESS_STEPS.indexOf(step);
                              const state =
                                targetIndex < currentIndex
                                  ? "completed"
                                  : targetIndex === currentIndex
                                    ? "current"
                                    : "upcoming";
                              return (
                                <div key={step} className="flex min-w-0 flex-1 items-center gap-1">
                                  <div className={`flex h-5 w-5 shrink-0 items-center justify-center rounded-full border text-[10px] font-semibold ${stepperCircleClass(state)}`}>
                                    {state === "completed" ? "✓" : targetIndex + 1}
                                  </div>
                                  {index < WORKBENCH_PROGRESS_STEPS.length - 1 ? (
                                    <div className={`h-0.5 min-w-2 flex-1 rounded-full ${stepperLineClass(state)}`} />
                                  ) : null}
                                </div>
                              );
                            })}
                          </div>
                          <div className="text-[11px] text-muted-foreground">
                            {WORKBENCH_PROGRESS_LABELS[workbenchStepForStage(selected.current_stage)]} in progress
                          </div>
                        </>
                      )}
                    </div>
                    <div className="grid grid-cols-2 gap-3">
                      <MetricCard label="Operations" value={String(selected.operation_count)} />
                      <MetricCard label="Fields" value={String(selected.field_count)} />
                      <MetricCard label="Drafts Ready" value={String(selected.draft_ready_count || 0)} />
                      <MetricCard label="Pending" value={String(selected.pending_proposal_count)} />
                    </div>
                    <div className="rounded-lg border border-border/70 bg-muted/20 px-3 py-3 text-sm text-muted-foreground">
                      {selected.preparation_status === "preparing" || selected.preparation_status === "blocked" ? (
                        <>Worker task: <span className="font-medium text-foreground">{selected.worker_current_task || "Preparing AI drafts"}</span></>
                      ) : (
                        <>Next review step: <span className="font-medium text-foreground">{WORKBENCH_PROGRESS_LABELS[workbenchStepForStage(selected.current_stage)]}</span></>
                      )}
                    </div>
                    <Button type="button" className="w-full" asChild>
                      <Link href={`/onboarding-runs/${selected.id}`}>Continue Workspace</Link>
                    </Button>
                  </div>
                ) : <EmptyPanel message="No onboarding run selected." />}
              </InspectorPanel>
            </div>
          ) : null}
          {!loading && !error && !items.length ? <EmptyPanel message="No onboarding workspaces yet. Upload a source to start one." /> : null}
        </div>
      }
    />
  );
}
