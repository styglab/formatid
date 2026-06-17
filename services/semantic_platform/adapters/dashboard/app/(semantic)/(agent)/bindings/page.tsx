"use client";

import { useEffect, useMemo, useState } from "react";
import { RefreshCw } from "lucide-react";
import { listCapabilityBindings } from "@/api/semantic-admin";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { TablePanel } from "@/components/layout/table-panel";
import { InspectorPanel } from "@/components/layout/inspector-panel";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { EmptyPanel, ErrorPanel, InfoLine, LoadingPanel } from "@/components/semantic/common/state-panel";
import { MetaCard } from "@/components/semantic/common/meta-card";
import type { CapabilityBinding } from "@/types/semantic";

export default function CapabilityBindingsPage() {
  const [items, setItems] = useState<CapabilityBinding[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      const next = await listCapabilityBindings();
      setItems(next);
      setSelectedId((current) => current && next.some((item) => item.id === current) ? current : next[0]?.id || "");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load capability bindings.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { void reload(); }, []);
  const selected = useMemo(() => items.find((item) => item.id === selectedId) || items[0] || null, [items, selectedId]);

  return (
    <SectionPlaceholder
      title="Capability Bindings"
      description="Separate planner-facing capability implementations from operation field mappings."
      actions={<Button type="button" variant="outline" onClick={() => void reload()} disabled={loading}><RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />Refresh</Button>}
      body={<div className="space-y-4"><InfoLine><Badge variant="info">{items.length} bindings</Badge><Badge variant="warning">{items.filter((item) => item.status === "candidate").length} candidates</Badge></InfoLine>{error ? <ErrorPanel message={error} /> : null}{loading ? <LoadingPanel message="Loading capability bindings..." /> : null}{!loading && !error ? <div className="grid gap-5 xl:grid-cols-[minmax(0,1.1fr)_380px]"><TablePanel><table className="min-w-full table-fixed text-left text-[12px]"><thead className="sticky top-0 z-10 border-b border-border/70 bg-slate-50/70 text-[10px] uppercase tracking-[0.08em] text-muted-foreground backdrop-blur"><tr><th className="w-[38%] px-3 py-2.5 font-medium">Capability</th><th className="w-[34%] px-3 py-2.5 font-medium">Operation</th><th className="w-[16%] px-3 py-2.5 font-medium">Coverage</th><th className="w-[12%] px-3 py-2.5 font-medium">Status</th></tr></thead><tbody className="divide-y divide-slate-200/80">{items.map((item) => <tr key={item.id} className={`cursor-pointer transition hover:bg-primary/[0.04] ${selected?.id === item.id ? "bg-primary/[0.08] shadow-[inset_2px_0_0_0_hsl(var(--primary))]" : ""}`} onClick={() => setSelectedId(item.id)}><td className="px-3 py-2.5"><div className="truncate text-[13px] font-semibold text-foreground">{item.capability_name}</div><div className="mt-0.5 truncate font-mono text-[10px] text-muted-foreground">{item.capability_key}</div></td><td className="px-3 py-2.5"><div className="truncate text-[12px] text-foreground">{item.operation_name}</div><div className="mt-0.5 truncate font-mono text-[10px] text-muted-foreground">{item.operation_id}</div></td><td className="px-3 py-2.5 text-muted-foreground">{Math.round(item.semantic_coverage * 100)}%</td><td className="px-3 py-2.5"><Badge variant={item.status === "ready" ? "default" : "warning"}>{item.status}</Badge></td></tr>)}</tbody></table></TablePanel><InspectorPanel>{selected ? <div className="space-y-4 p-4"><div><div className="text-sm font-semibold text-foreground">{selected.capability_name}</div><div className="mt-1 font-mono text-[11px] text-muted-foreground">{selected.id}</div></div><div className="grid gap-3"><MetaCard label="Capability" value={selected.capability_key} /><MetaCard label="Operation" value={selected.operation_name} /><MetaCard label="Variants" value={String(selected.variant_count)} /><MetaCard label="Evidence" value={selected.evidence} /></div></div> : <EmptyPanel message="No binding selected." />}</InspectorPanel></div> : null}</div>}
    />
  );
}
