"use client";

import { useEffect, useMemo, useState } from "react";
import { RefreshCw, Route, Search } from "lucide-react";
import {
  listCapabilities,
  listCapabilitySteps,
} from "@/api/context-admin";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { Capability, ContextCapabilityStep } from "@/types/context";

export default function CapabilitiesPage() {
  const [capabilities, setCapabilities] = useState<Capability[]>([]);
  const [capabilitySteps, setCapabilitySteps] = useState<ContextCapabilityStep[]>([]);
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      const [loadedCapabilities, loadedCapabilitySteps] = await Promise.all([
        listCapabilities(),
        listCapabilitySteps(),
      ]);
      setCapabilities(loadedCapabilities);
      setCapabilitySteps(loadedCapabilitySteps);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load capabilities.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, []);

  const stepsByCapabilityId = useMemo(() => countBy(capabilitySteps, "capability_id"), [capabilitySteps]);
  const normalizedQuery = query.trim().toLowerCase();
  const filteredCapabilities = capabilities.filter((capability) =>
    `${capability.capability_key} ${capability.name} ${capability.description || ""} ${capability.status || ""}`
      .toLowerCase()
      .includes(normalizedQuery)
  );
  const usableCount = capabilities.filter((item) => isUsableStatus(item.status)).length;

  return (
    <div className="mx-auto flex max-w-[1280px] flex-col gap-5">
      <section className="flex flex-col gap-4 border-b border-border pb-5 lg:flex-row lg:items-end lg:justify-between">
        <div className="min-w-0">
          <div className="flex items-center gap-2 text-xs uppercase tracking-[0.12em] text-muted-foreground">
            <Route className="h-3.5 w-3.5" />
            Catalog
          </div>
          <h1 className="mt-2 text-2xl font-semibold text-foreground">Capabilities</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Planner-facing executable meaning contracts and their source operation steps.
          </p>
        </div>
        <Button type="button" variant="outline" onClick={() => void reload()} disabled={loading}>
          <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </section>

      {error ? <Notice message={error} /> : null}

      <section className="grid gap-3 md:grid-cols-3">
        <Metric label="Capabilities" value={capabilities.length} />
        <Metric label="Usable" value={usableCount} />
        <Metric label="Capability Steps" value={capabilitySteps.length} />
      </section>

      <Card>
        <CardHeader>
          <div>
            <CardTitle>Capability Catalog</CardTitle>
            <CardDescription>Capabilities describe what can be achieved; capability steps describe how.</CardDescription>
          </div>
          <Route className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent className="space-y-3">
          <label className="flex h-9 max-w-md items-center gap-2 rounded-lg border border-border bg-background px-3">
            <Search className="h-4 w-4 text-muted-foreground" />
            <input className="min-w-0 flex-1 bg-transparent text-sm outline-none placeholder:text-muted-foreground" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search capabilities" />
          </label>
          <div className="overflow-hidden rounded-lg border border-border">
            <div className="grid grid-cols-[minmax(240px,1.4fr)_minmax(220px,1fr)_140px_110px] border-b border-border bg-muted/35 px-3 py-2 text-xs font-medium text-muted-foreground">
              <div>Capability</div>
              <div>Description</div>
              <div>Steps</div>
              <div>Status</div>
            </div>
            <div className="max-h-[42rem] overflow-auto">
              {filteredCapabilities.length ? (
                filteredCapabilities.map((capability) => (
                  <div key={capability.id} className="grid grid-cols-[minmax(240px,1.4fr)_minmax(220px,1fr)_140px_110px] gap-3 border-b border-border px-3 py-2.5 text-sm last:border-b-0">
                    <div className="min-w-0">
                      <div className="truncate font-medium text-foreground">{capability.capability_key}</div>
                      <div className="mt-0.5 truncate text-xs text-muted-foreground">{capability.name}</div>
                    </div>
                    <div className="truncate text-muted-foreground">{capability.description || "-"}</div>
                    <div>{stepsByCapabilityId[capability.id] || 0}</div>
                    <StatusBadge status={capability.status} />
                  </div>
                ))
              ) : (
                <EmptyState message={loading ? "Loading capabilities..." : "No capabilities found."} />
              )}
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function countBy<T extends Record<string, unknown>>(items: T[], key: keyof T) {
  return items.reduce<Record<string, number>>((acc, item) => {
    const value = String(item[key] || "");
    if (value) acc[value] = (acc[value] || 0) + 1;
    return acc;
  }, {});
}

function Metric({ label, value }: { label: string; value: string | number }) {
  return <div className="rounded-lg border border-border bg-card px-4 py-3"><div className="text-xs text-muted-foreground">{label}</div><div className="mt-2 text-2xl font-semibold text-foreground">{value}</div></div>;
}

function StatusBadge({ status }: { status?: string | null }) {
  const value = status || "draft";
  const variant = value === "approved" || value === "published" ? "success" : value === "failed" ? "danger" : value === "proposed" ? "info" : "warning";
  return <Badge variant={variant}>{value}</Badge>;
}

function isUsableStatus(status?: string | null) {
  return status === "active" || status === "approved" || status === "published";
}

function EmptyState({ message }: { message: string }) {
  return <div className="px-4 py-8 text-center text-sm text-muted-foreground">{message}</div>;
}

function Notice({ message }: { message: string }) {
  return <div className="rounded-lg border border-red-500/25 bg-red-500/10 px-4 py-3 text-sm text-red-700 dark:text-red-300">{message}</div>;
}
