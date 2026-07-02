"use client";

import { useEffect, useState } from "react";
import { RefreshCw, Route } from "lucide-react";
import { listExecutions, listPlans } from "@/api/context-admin";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { ContextExecution, ContextPlan } from "@/types/context";

export default function ExecutionsPage() {
  const [executions, setExecutions] = useState<ContextExecution[]>([]);
  const [plans, setPlans] = useState<ContextPlan[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      const [loadedExecutions, loadedPlans] = await Promise.all([
        listExecutions(),
        listPlans(),
      ]);
      setExecutions(loadedExecutions);
      setPlans(loadedPlans);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load executions.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, []);

  const completed = executions.filter((item) => item.status === "completed" || item.status === "succeeded").length;
  const failed = executions.filter((item) => item.status === "failed").length;

  return (
    <div className="mx-auto flex max-w-[1280px] flex-col gap-5">
      <section className="flex flex-col gap-4 border-b border-border pb-5 lg:flex-row lg:items-end lg:justify-between">
        <div className="min-w-0">
          <div className="flex items-center gap-2 text-xs uppercase tracking-[0.12em] text-muted-foreground">
            <Route className="h-3.5 w-3.5" />
            Runtime
          </div>
          <h1 className="mt-2 text-2xl font-semibold text-foreground">Executions</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Runtime execution records created from validated planner plans.
          </p>
        </div>
        <Button type="button" variant="outline" onClick={() => void reload()} disabled={loading}>
          <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </section>

      {error ? <Notice message={error} /> : null}

      <section className="grid gap-3 md:grid-cols-4">
        <Metric label="Executions" value={executions.length} />
        <Metric label="Completed" value={completed} />
        <Metric label="Failed" value={failed} />
        <Metric label="Plans" value={plans.length} />
      </section>

      <Card>
        <CardHeader>
          <div>
            <CardTitle>Execution Log</CardTitle>
            <CardDescription>Validated plan execution history.</CardDescription>
          </div>
          <Route className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="overflow-hidden rounded-lg border border-border">
            <div className="grid grid-cols-[minmax(220px,1.2fr)_minmax(180px,1fr)_160px_160px_110px] border-b border-border bg-muted/35 px-3 py-2 text-xs font-medium text-muted-foreground">
              <div>Execution</div>
              <div>Plan</div>
              <div>Started</div>
              <div>Completed</div>
              <div>Status</div>
            </div>
            <div className="max-h-[42rem] overflow-auto">
              {executions.length ? (
                executions.map((execution) => (
                  <div key={execution.id} className="grid grid-cols-[minmax(220px,1.2fr)_minmax(180px,1fr)_160px_160px_110px] gap-3 border-b border-border px-3 py-2.5 text-sm last:border-b-0">
                    <div className="truncate font-medium text-foreground">{execution.id}</div>
                    <div className="truncate text-muted-foreground">{execution.plan_id}</div>
                    <div className="truncate text-muted-foreground">{formatDate(execution.started_at || execution.created_at)}</div>
                    <div className="truncate text-muted-foreground">{formatDate(execution.completed_at)}</div>
                    <StatusBadge status={execution.status} />
                  </div>
                ))
              ) : (
                <EmptyState message={loading ? "Loading executions..." : "No executions yet."} />
              )}
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function formatDate(value?: string | null) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString();
}

function Metric({ label, value }: { label: string; value: string | number }) {
  return <div className="rounded-lg border border-border bg-card px-4 py-3"><div className="text-xs text-muted-foreground">{label}</div><div className="mt-2 text-2xl font-semibold text-foreground">{value}</div></div>;
}

function StatusBadge({ status }: { status?: string | null }) {
  const value = status || "draft";
  const variant = value === "completed" || value === "succeeded" ? "success" : value === "failed" ? "danger" : value === "running" || value === "started" ? "info" : "warning";
  return <Badge variant={variant}>{value}</Badge>;
}

function EmptyState({ message }: { message: string }) {
  return <div className="px-4 py-8 text-center text-sm text-muted-foreground">{message}</div>;
}

function Notice({ message }: { message: string }) {
  return <div className="rounded-lg border border-red-500/25 bg-red-500/10 px-4 py-3 text-sm text-red-700 dark:text-red-300">{message}</div>;
}
