"use client";

import { useEffect, useState } from "react";
import { CheckCircle2, PlayCircle, RefreshCw } from "lucide-react";
import {
  listBindings,
  listCapabilities,
  listCapabilitySteps,
  listPlans,
  listSourceOperations,
} from "@/api/context-admin";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { Capability, ContextBinding, ContextCapabilityStep, ContextPlan, ContextSourceOperation } from "@/types/context";

export default function PlannerPage() {
  const [plans, setPlans] = useState<ContextPlan[]>([]);
  const [capabilities, setCapabilities] = useState<Capability[]>([]);
  const [capabilitySteps, setCapabilitySteps] = useState<ContextCapabilityStep[]>([]);
  const [operations, setOperations] = useState<ContextSourceOperation[]>([]);
  const [bindings, setBindings] = useState<ContextBinding[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      const [loadedPlans, loadedCapabilities, loadedCapabilitySteps, loadedOperations, loadedBindings] = await Promise.all([
        listPlans(),
        listCapabilities(),
        listCapabilitySteps(),
        listSourceOperations(),
        listBindings(),
      ]);
      setPlans(loadedPlans);
      setCapabilities(loadedCapabilities);
      setCapabilitySteps(loadedCapabilitySteps);
      setOperations(loadedOperations);
      setBindings(loadedBindings);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load planner data.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, []);

  const usableCapabilities = capabilities.filter((item) => isUsableStatus(item.status)).length;
  const approvedBindings = bindings.filter((item) => isUsableStatus(item.status)).length;

  return (
    <div className="mx-auto flex max-w-[1280px] flex-col gap-5">
      <section className="flex flex-col gap-4 border-b border-border pb-5 lg:flex-row lg:items-end lg:justify-between">
        <div className="min-w-0">
          <div className="flex items-center gap-2 text-xs uppercase tracking-[0.12em] text-muted-foreground">
            <PlayCircle className="h-3.5 w-3.5" />
            Runtime
          </div>
          <h1 className="mt-2 text-2xl font-semibold text-foreground">Planner</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Validated plans select approved capabilities, source operations, parameter bindings, and expected representations before execution.
          </p>
        </div>
        <Button type="button" variant="outline" onClick={() => void reload()} disabled={loading}>
          <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </section>

      {error ? <Notice message={error} /> : null}

      <section className="grid gap-3 md:grid-cols-4">
        <Metric label="Plans" value={plans.length} />
        <Metric label="Usable Capabilities" value={usableCapabilities} />
        <Metric label="Capability Steps" value={capabilitySteps.length} />
        <Metric label="Resolution Edges" value={approvedBindings} />
      </section>

      <section className="grid gap-5 lg:grid-cols-[360px_minmax(0,1fr)]">
        <Card>
          <CardHeader>
            <div>
              <CardTitle>Readiness</CardTitle>
              <CardDescription>Minimum runtime catalog requirements.</CardDescription>
            </div>
            <CheckCircle2 className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent className="space-y-2">
            <CheckRow ok={usableCapabilities > 0} label="Usable capability exists" />
            <CheckRow ok={capabilitySteps.length > 0} label="Capability step exists" />
            <CheckRow ok={approvedBindings > 0} label="Approved resolution edge exists" />
            <CheckRow ok={operations.length > 0} label="Source operation exists" />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <div>
              <CardTitle>Plans</CardTitle>
              <CardDescription>Planner output awaiting confirmation or execution.</CardDescription>
            </div>
            <PlayCircle className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="overflow-hidden rounded-lg border border-border">
              <div className="grid grid-cols-[minmax(220px,1.2fr)_150px_150px_120px_110px] border-b border-border bg-muted/35 px-3 py-2 text-xs font-medium text-muted-foreground">
                <div>Plan</div>
                <div>Capability</div>
                <div>Operation</div>
                <div>Confidence</div>
                <div>Status</div>
              </div>
              <div className="max-h-[34rem] overflow-auto">
                {plans.length ? (
                  plans.map((plan) => (
                    <div key={plan.id} className="grid grid-cols-[minmax(220px,1.2fr)_150px_150px_120px_110px] gap-3 border-b border-border px-3 py-2.5 text-sm last:border-b-0">
                      <div className="truncate font-medium text-foreground">{plan.id}</div>
                      <div className="truncate text-muted-foreground">{plan.selected_capability_id || "-"}</div>
                      <div className="truncate text-muted-foreground">{plan.selected_source_operation_id || "-"}</div>
                      <div>{typeof plan.confidence === "number" ? plan.confidence.toFixed(2) : "-"}</div>
                      <StatusBadge status={plan.status} />
                    </div>
                  ))
                ) : (
                  <EmptyState message={loading ? "Loading plans..." : "No plans yet."} />
                )}
              </div>
            </div>
          </CardContent>
        </Card>
      </section>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string | number }) {
  return <div className="rounded-lg border border-border bg-card px-4 py-3"><div className="text-xs text-muted-foreground">{label}</div><div className="mt-2 text-2xl font-semibold text-foreground">{value}</div></div>;
}

function CheckRow({ ok, label }: { ok: boolean; label: string }) {
  return <div className="flex items-center justify-between rounded-lg border border-border px-3 py-2"><span className="text-sm text-foreground">{label}</span><Badge variant={ok ? "success" : "warning"}>{ok ? "ready" : "missing"}</Badge></div>;
}

function StatusBadge({ status }: { status?: string | null }) {
  const value = status || "draft";
  const variant = value === "approved" || value === "published" || value === "validated" ? "success" : value === "failed" ? "danger" : value === "running" ? "info" : "warning";
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
