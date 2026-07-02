"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import {
  Activity,
  ArrowRight,
  Boxes,
  CheckCircle2,
  ClipboardList,
  Database,
  GitBranch,
  Layers3,
  PackageCheck,
  RefreshCw,
  Route,
} from "lucide-react";
import {
  listBindings,
  listCapabilities,
  listCapabilityOperations,
  listCanonicalClassSlotUsages,
  listCanonicalClasses,
  listCanonicalEnums,
  listCanonicalSlots,
  listCanonicalTypes,
  listContextSourcesPage,
  listOnboardingRuns,
  listOverview,
  listProposalBundles,
  listSourceDocuments,
  listSourceFields,
  listSourceOperations,
  type ContextBinding,
  type ContextCapabilityOperation,
  type ContextOnboardingRun,
  type ContextOverview,
  type ContextProposalBundle,
  type ContextSource,
  type ContextSourceDocument,
  type ContextSourceField,
  type ContextSourceOperation,
} from "@/api/context-admin";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { Capability, CanonicalClassSlotUsage, CanonicalClass, CanonicalEnum, CanonicalSlot, CanonicalType, PaginatedResult } from "@/types/context";

type OverviewSnapshot = {
  overview: ContextOverview | null;
  sources: PaginatedResult<ContextSource>;
  documents: ContextSourceDocument[];
  runs: ContextOnboardingRun[];
  operations: ContextSourceOperation[];
  fields: ContextSourceField[];
  types: CanonicalType[];
  enums: CanonicalEnum[];
  slots: CanonicalSlot[];
  classes: CanonicalClass[];
  classSlotUsages: CanonicalClassSlotUsage[];
  bindings: ContextBinding[];
  capabilities: Capability[];
  capabilityOperations: ContextCapabilityOperation[];
  bundles: ContextProposalBundle[];
};

const emptySources: PaginatedResult<ContextSource> = { items: [], total: 0, page: 1, page_size: 8 };

export default function ContextPlatformOverview() {
  const [snapshot, setSnapshot] = useState<OverviewSnapshot>({
    overview: null,
    sources: emptySources,
    documents: [],
    runs: [],
    operations: [],
    fields: [],
    types: [],
    enums: [],
    slots: [],
    classes: [],
    classSlotUsages: [],
    bindings: [],
    capabilities: [],
    capabilityOperations: [],
    bundles: [],
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      const [
        overview,
        sources,
        documents,
        runs,
        operations,
        fields,
        types,
        enums,
        slots,
        classes,
        classSlotUsages,
        bindings,
        capabilities,
        capabilityOperations,
        bundles,
      ] = await Promise.all([
        listOverview(),
        listContextSourcesPage({ page: 1, pageSize: 8 }),
        listSourceDocuments(),
        listOnboardingRuns(),
        listSourceOperations(),
        listSourceFields(),
        listCanonicalTypes(),
        listCanonicalEnums(),
        listCanonicalSlots(),
        listCanonicalClasses(),
        listCanonicalClassSlotUsages(),
        listBindings(),
        listCapabilities(),
        listCapabilityOperations(),
        listProposalBundles(),
      ]);
      setSnapshot({
        overview,
        sources,
        documents,
        runs,
        operations,
        fields,
        types,
        enums,
        slots,
        classes,
        classSlotUsages,
        bindings,
        capabilities,
        capabilityOperations,
        bundles,
      });
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load Context Platform overview.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, []);

  const counts = snapshot.overview?.counts || {};
  const usableCapabilities = snapshot.capabilities.filter((item) => isUsableStatus(item.status)).length;
  const approvedBindings = snapshot.bindings.filter((item) => isUsableStatus(item.status)).length;
  const runningRuns = snapshot.runs.filter((run) => run.status === "running" || run.status === "submitted").length;
  const failedRuns = snapshot.runs.filter((run) => run.status === "failed").length;
  const executableCoverage = useMemo(() => {
    if (!snapshot.operations.length) return 0;
    const linkedOperationIds = new Set(snapshot.capabilityOperations.map((item) => item.source_operation_id));
    return Math.round((linkedOperationIds.size / snapshot.operations.length) * 100);
  }, [snapshot.capabilityOperations, snapshot.operations]);

  return (
    <div className="mx-auto flex max-w-[1480px] flex-col gap-5">
      <section className="flex flex-col gap-4 border-b border-border pb-5 lg:flex-row lg:items-end lg:justify-between">
        <div className="min-w-0">
          <div className="flex items-center gap-2 text-xs uppercase tracking-[0.12em] text-muted-foreground">
            <Layers3 className="h-3.5 w-3.5" />
            Overview
          </div>
          <h1 className="mt-2 text-2xl font-semibold text-foreground">Dashboard</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Track source intake, agent ingestion status, review, catalog coverage, and runtime readiness.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button type="button" variant="outline" onClick={() => void reload()} disabled={loading}>
            <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
          <Button asChild>
            <Link href="/workbench">
              Open Source Intake
              <ArrowRight className="h-4 w-4" />
            </Link>
          </Button>
        </div>
      </section>

      {error ? <Notice message={error} /> : null}

      <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <KpiCard icon={Database} label="Sources" value={counts.sources ?? snapshot.sources.total} detail={`${snapshot.documents.length} documents`} />
        <KpiCard icon={ClipboardList} label="Review" value={counts.pending_bundles ?? 0} detail={`${counts.pending_proposals ?? 0} pending items`} />
        <KpiCard icon={PackageCheck} label="Capabilities" value={usableCapabilities} detail={`${snapshot.capabilities.length} total`} />
        <KpiCard icon={Activity} label="Agent Runs" value={runningRuns} detail={`${failedRuns} failed`} />
      </section>

      <section className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_420px]">
        <Card id="catalog-coverage">
          <CardHeader>
            <div>
              <CardTitle>Graph Coverage</CardTitle>
              <CardDescription>Meaning, representations, source fields, resolution edges, and runtime links.</CardDescription>
            </div>
            <Boxes className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            <CoverageTile label="Object Types" value={snapshot.classes.length} />
            <CoverageTile label="Property Types" value={snapshot.slots.length} />
            <CoverageTile label="Representations" value={snapshot.classSlotUsages.length} />
            <CoverageTile label="Schemas / Domains" value={`${snapshot.types.length} / ${snapshot.enums.length}`} />
            <CoverageTile label="Extracted Fields" value={snapshot.fields.length} />
            <CoverageTile label="Source Operations" value={snapshot.operations.length} />
            <CoverageTile label="Resolution Edges" value={approvedBindings} />
            <CoverageTile label="Capability Step Coverage" value={`${executableCoverage}%`} />
          </CardContent>
        </Card>

        <Card id="planner-readiness">
          <CardHeader>
            <div>
              <CardTitle>Runtime Readiness</CardTitle>
              <CardDescription>Minimum catalog requirements for validated execution.</CardDescription>
            </div>
            <Route className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent className="space-y-3">
            <CheckRow ok={snapshot.capabilities.some((item) => isUsableStatus(item.status))} label="Usable capability exists" />
            <CheckRow ok={snapshot.capabilityOperations.length > 0} label="Capability step exists" />
            <CheckRow ok={approvedBindings > 0} label="Approved resolution edge exists" />
            <CheckRow ok={snapshot.operations.length > 0} label="Executable source operation exists" />
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-5 xl:grid-cols-3">
        <ListCard
          id="recent-documents"
          title="Documents"
          description="Latest uploaded source documents."
          empty="No source documents yet."
          rows={snapshot.documents.slice(0, 6).map((document) => ({
            title: document.name,
            detail: document.document_type,
            status: document.status || "draft",
          }))}
        />
        <ListCard
          title="Runs"
          description="Latest ingestion activity."
          empty="No ingestion runs yet."
          rows={snapshot.runs.slice(0, 6).map((run) => ({
            title: run.stage,
            detail: run.source_document_id || run.id,
            status: run.status,
          }))}
        />
        <ListCard
          id="proposal-bundles"
          title="Review"
          description="Bundles waiting for approval."
          empty="No proposal bundles yet."
          rows={snapshot.bundles.slice(0, 6).map((bundle) => ({
            title: bundle.title,
            detail: `${bundle.proposal_count || 0} proposal items`,
            status: bundle.status,
          }))}
        />
      </section>
    </div>
  );
}

function KpiCard({ icon: Icon, label, value, detail }: { icon: typeof Database; label: string; value: number; detail: string }) {
  return (
    <div className="rounded-lg border border-border bg-card px-4 py-3">
      <div className="flex items-center justify-between gap-3">
        <div className="text-xs text-muted-foreground">{label}</div>
        <Icon className="h-4 w-4 text-muted-foreground" />
      </div>
      <div className="mt-2 text-2xl font-semibold text-foreground">{value}</div>
      <div className="mt-1 text-xs text-muted-foreground">{detail}</div>
    </div>
  );
}

function CoverageTile({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-lg border border-border px-3 py-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="mt-2 text-xl font-semibold text-foreground">{value}</div>
    </div>
  );
}

function CheckRow({ ok, label }: { ok: boolean; label: string }) {
  return (
    <div className="flex items-center justify-between rounded-lg border border-border px-3 py-2">
      <div className="flex min-w-0 items-center gap-2">
        <CheckCircle2 className={`h-4 w-4 ${ok ? "text-emerald-600" : "text-muted-foreground"}`} />
        <span className="truncate text-sm text-foreground">{label}</span>
      </div>
      <Badge variant={ok ? "success" : "warning"}>{ok ? "ready" : "missing"}</Badge>
    </div>
  );
}

function ListCard({
  id,
  title,
  description,
  rows,
  empty,
}: {
  id?: string;
  title: string;
  description: string;
  rows: Array<{ title: string; detail: string; status: string }>;
  empty: string;
}) {
  return (
    <Card id={id}>
      <CardHeader>
        <div>
          <CardTitle>{title}</CardTitle>
          <CardDescription>{description}</CardDescription>
        </div>
        <GitBranch className="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent className="space-y-2">
        {rows.length ? (
          rows.map((row) => (
            <div key={`${row.title}-${row.detail}`} className="rounded-lg border border-border px-3 py-2.5">
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="truncate text-sm font-medium text-foreground">{row.title}</div>
                  <div className="mt-1 truncate text-xs text-muted-foreground">{row.detail}</div>
                </div>
                <StatusBadge status={row.status} />
              </div>
            </div>
          ))
        ) : (
          <div className="rounded-lg border border-dashed border-border px-4 py-8 text-center text-sm text-muted-foreground">
            {empty}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function StatusBadge({ status }: { status?: string }) {
  const value = status || "draft";
  const variant =
    value === "complete" || value === "completed" || value === "approved" || value === "published"
      ? "success"
      : value === "failed" || value === "rejected"
        ? "danger"
        : value === "running" || value === "submitted" || value === "proposed"
          ? "info"
          : "warning";
  return <Badge variant={variant}>{value}</Badge>;
}

function Notice({ message }: { message: string }) {
  return <div className="rounded-lg border border-red-500/25 bg-red-500/10 px-4 py-3 text-sm text-red-700 dark:text-red-300">{message}</div>;
}

function isUsableStatus(status?: string | null) {
  return status === "active" || status === "approved" || status === "published";
}
