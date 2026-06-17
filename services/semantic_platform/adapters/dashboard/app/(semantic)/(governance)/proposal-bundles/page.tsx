"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { RefreshCw } from "lucide-react";
import { listProposalBundles } from "@/api/semantic-admin";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { TablePanel } from "@/components/layout/table-panel";
import { InspectorPanel } from "@/components/layout/inspector-panel";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { EmptyPanel, ErrorPanel, InfoLine, LoadingPanel } from "@/components/semantic/common/state-panel";
import type { ProposalBundle } from "@/types/semantic";

export default function ProposalBundlesPage() {
  const router = useRouter();
  const [items, setItems] = useState<ProposalBundle[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      const next = await listProposalBundles();
      setItems(next);
      setSelectedId((current) => current && next.some((item) => item.id === current) ? current : next[0]?.id || "");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load proposal bundles.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { void reload(); }, []);
  const selected = useMemo(() => items.find((item) => item.id === selectedId) || items[0] || null, [items, selectedId]);

  return (
    <SectionPlaceholder
      title="Proposal Bundles"
      description="Review source-run proposal bundles before drilling into individual semantic, mapping, variant, and capability changes."
      actions={<Button type="button" variant="outline" onClick={() => void reload()} disabled={loading}><RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />Refresh</Button>}
      body={
        <div className="space-y-4">
          <InfoLine><Badge variant="info">{items.length} bundles</Badge><Badge variant="warning">{items.reduce((total, item) => total + item.pending_count, 0)} pending</Badge></InfoLine>
          {error ? <ErrorPanel message={error} /> : null}
          {loading ? <LoadingPanel message="Loading proposal bundles..." /> : null}
          {!loading && !error ? (
            <div className="grid gap-5 xl:grid-cols-[minmax(0,1.1fr)_380px]">
              <TablePanel>
                <table className="min-w-full table-fixed text-left text-[12px]">
                  <thead className="sticky top-0 z-10 border-b border-border/70 bg-slate-50/70 text-[10px] uppercase tracking-[0.08em] text-muted-foreground backdrop-blur">
                    <tr><th className="w-[42%] px-3 py-2.5 font-medium">Bundle</th><th className="w-[22%] px-3 py-2.5 font-medium">Counts</th><th className="w-[20%] px-3 py-2.5 font-medium">Status</th><th className="w-[16%] px-3 py-2.5 font-medium">Action</th></tr>
                  </thead>
                  <tbody className="divide-y divide-slate-200/80">
                    {items.map((item) => (
                      <tr key={item.id} className={`cursor-pointer transition hover:bg-primary/[0.04] ${selected?.id === item.id ? "bg-primary/[0.08] shadow-[inset_2px_0_0_0_hsl(var(--primary))]" : ""}`} onClick={() => setSelectedId(item.id)}>
                        <td className="px-3 py-2.5"><div className="truncate text-[13px] font-semibold text-foreground">{item.source_name}</div><div className="mt-0.5 truncate font-mono text-[10px] text-muted-foreground">{item.id}</div></td>
                        <td className="px-3 py-2.5 text-muted-foreground">{item.proposal_count} proposals · {item.pending_count} pending</td>
                        <td className="px-3 py-2.5"><Badge variant={item.pending_count ? "warning" : "default"}>{item.status}</Badge></td>
                        <td className="px-3 py-2.5"><Button type="button" size="sm" variant="ghost" disabled={!item.proposal_ids.length} onClick={(event) => { event.stopPropagation(); router.push(`/proposals?ids=${encodeURIComponent(item.proposal_ids.join(","))}`); }}>Open</Button></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </TablePanel>
              <InspectorPanel>
                {selected ? <div className="space-y-4 p-4"><div><div className="text-sm font-semibold text-foreground">{selected.source_name}</div><div className="mt-1 font-mono text-[11px] text-muted-foreground">{selected.evidence_snapshot_id}</div></div><div className="rounded-lg border border-border/70 bg-muted/20 px-3 py-3"><div className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Entity breakdown</div><div className="mt-2 flex flex-wrap gap-2">{Object.entries(selected.entity_counts).length ? Object.entries(selected.entity_counts).map(([key, value]) => <Badge key={key} variant="default">{key} {value}</Badge>) : <span className="text-sm text-muted-foreground">No proposals linked.</span>}</div></div><div className="grid grid-cols-3 gap-2 text-center"><Badge variant="warning">pending {selected.pending_count}</Badge><Badge variant="default">approved {selected.approved_count}</Badge><Badge variant="default">rejected {selected.rejected_count}</Badge></div></div> : <EmptyPanel message="No proposal bundle selected." />}
              </InspectorPanel>
            </div>
          ) : null}
        </div>
      }
    />
  );
}
