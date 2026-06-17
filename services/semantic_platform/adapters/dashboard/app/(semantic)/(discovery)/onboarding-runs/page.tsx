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
import { EmptyPanel, ErrorPanel, InfoLine, LoadingPanel } from "@/components/semantic/common/state-panel";
import { MetricCard } from "@/components/semantic/common/meta-card";
import type { OnboardingRun } from "@/types/semantic";

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
      title="Onboarding Runs"
      description="Review source onboarding runs, evidence snapshots, discovered metadata, AI suggestion batches, and generated proposals."
      actions={
        <Button type="button" variant="outline" onClick={() => void reload()} disabled={loading}>
          <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      }
      body={
        <div className="space-y-4">
          <InfoLine>
            <Badge variant="info">{items.length} runs</Badge>
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
                      <th className="w-[38%] px-3 py-2.5 font-medium">Run</th>
                      <th className="w-[18%] px-3 py-2.5 font-medium">Discovered</th>
                      <th className="w-[22%] px-3 py-2.5 font-medium">Proposals</th>
                      <th className="w-[12%] px-3 py-2.5 font-medium">Status</th>
                      <th className="w-[10%] px-3 py-2.5 font-medium">Open</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-200/80">
                    {items.map((item) => (
                      <tr key={item.id} className={`cursor-pointer transition hover:bg-primary/[0.04] ${selected?.id === item.id ? "bg-primary/[0.08] shadow-[inset_2px_0_0_0_hsl(var(--primary))]" : ""}`} onClick={() => setSelectedId(item.id)}>
                        <td className="px-3 py-2.5">
                          <div className="truncate text-[13px] font-semibold text-foreground">{item.source_name}</div>
                          <div className="mt-0.5 truncate font-mono text-[10px] text-muted-foreground">{item.id}</div>
                        </td>
                        <td className="px-3 py-2.5 text-muted-foreground">{item.operation_count} ops · {item.field_count} fields</td>
                        <td className="px-3 py-2.5 text-muted-foreground">{item.proposal_count} total · {item.pending_proposal_count} pending</td>
                        <td className="px-3 py-2.5"><Badge variant={item.pending_proposal_count ? "warning" : "default"}>{item.status}</Badge></td>
                        <td className="px-3 py-2.5">
                          <Button type="button" size="sm" variant="ghost" asChild>
                            <Link href={`/onboarding-runs/${item.id}`}>Open</Link>
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
                    <div className="grid grid-cols-2 gap-3">
                      <MetricCard label="Operations" value={String(selected.operation_count)} />
                      <MetricCard label="Fields" value={String(selected.field_count)} />
                      <MetricCard label="Mappings" value={String(selected.mapping_count)} />
                      <MetricCard label="Pending" value={String(selected.pending_proposal_count)} />
                    </div>
                    <div className="rounded-lg border border-border/70 bg-muted/20 px-3 py-3 text-sm text-muted-foreground">
                      Suggestion status: <span className="font-medium text-foreground">{selected.suggestion_status}</span>
                    </div>
                    <Button type="button" className="w-full" asChild>
                      <Link href={`/onboarding-runs/${selected.id}`}>Open Run Detail</Link>
                    </Button>
                    <Button type="button" variant="outline" className="w-full" asChild>
                      <Link href={`/sources/${selected.source_id}`}>Open Source Detail</Link>
                    </Button>
                  </div>
                ) : <EmptyPanel message="No onboarding run selected." />}
              </InspectorPanel>
            </div>
          ) : null}
          {!loading && !error && !items.length ? <EmptyPanel message="No onboarding runs yet. Upload a source to start onboarding." /> : null}
        </div>
      }
    />
  );
}
