"use client";

import { useEffect, useMemo, useState } from "react";
import {
  CheckCircle2,
  ClipboardCheck,
  ClipboardList,
  FileText,
  Layers3,
  PackageCheck,
  RefreshCw,
  Search,
  XCircle,
} from "lucide-react";
import {
  approveProposalBundle,
  listProposalBundleItems,
  listProposalBundles,
  rejectProposalBundle,
} from "@/api/context-admin";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { ContextProposalBundle, Proposal } from "@/types/context";

type StatusFilter = "proposed" | "approved" | "rejected" | "all";

export default function ProposalsPage() {
  const [bundles, setBundles] = useState<ContextProposalBundle[]>([]);
  const [items, setItems] = useState<Proposal[]>([]);
  const [selectedBundleId, setSelectedBundleId] = useState("");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("proposed");
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [itemsLoading, setItemsLoading] = useState(false);
  const [approving, setApproving] = useState(false);
  const [rejecting, setRejecting] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");

  async function reload(nextSelectedBundleId = selectedBundleId) {
    setLoading(true);
    setError("");
    setNotice("");
    try {
      const loadedBundles = await listProposalBundles({ status: statusFilter });
      const sortedBundles = [...loadedBundles].sort(sortBundles);
      setBundles(sortedBundles);
      const selected =
        sortedBundles.find((bundle) => bundle.id === nextSelectedBundleId) ||
        sortedBundles.find((bundle) => bundle.status === "proposed") ||
        sortedBundles[0] ||
        null;
      setSelectedBundleId(selected?.id || "");
      if (selected) {
        await loadItems(selected.id);
      } else {
        setItems([]);
      }
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load proposal bundles.");
    } finally {
      setLoading(false);
    }
  }

  async function loadItems(bundleId: string) {
    setItemsLoading(true);
    setError("");
    try {
      const loadedItems = await listProposalBundleItems(bundleId);
      setItems(loadedItems);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load proposal items.");
      setItems([]);
    } finally {
      setItemsLoading(false);
    }
  }

  async function approveSelectedBundle() {
    const bundle = selectedBundle;
    if (!bundle || bundle.status === "approved" || bundle.status === "rejected") return;
    setApproving(true);
    setError("");
    setNotice("");
    try {
      const result = await approveProposalBundle(bundle.id, { reviewer: "dashboard" });
      setNotice(`Applied ${result.applied_count || 0} changes; skipped ${result.skipped_count || 0}.`);
      await reload(bundle.id);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to approve proposal bundle.");
    } finally {
      setApproving(false);
    }
  }

  async function rejectSelectedBundle() {
    const bundle = selectedBundle;
    if (!bundle || bundle.status === "approved" || bundle.status === "rejected") return;
    setRejecting(true);
    setError("");
    setNotice("");
    try {
      const result = await rejectProposalBundle(bundle.id, {
        reviewer: "dashboard",
        rationale: "Rejected from dashboard review. Regenerate the source proposal bundle after fixing ingestion output.",
      });
      setNotice(`Rejected ${result.rejected_count || 0} proposal items.`);
      await reload(bundle.id);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to reject proposal bundle.");
    } finally {
      setRejecting(false);
    }
  }

  useEffect(() => {
    void reload("");
  }, [statusFilter]);

  const normalizedQuery = query.trim().toLowerCase();
  const filteredBundles = useMemo(() => {
    if (!normalizedQuery) return bundles;
    return bundles.filter((bundle) => {
      const summary = JSON.stringify(bundle.summary || {});
      return `${bundle.title} ${bundle.id} ${bundle.run_id} ${bundle.status} ${summary}`
        .toLowerCase()
        .includes(normalizedQuery);
    });
  }, [bundles, normalizedQuery]);

  const selectedBundle =
    bundles.find((bundle) => bundle.id === selectedBundleId) ||
    filteredBundles[0] ||
    null;
  const itemCounts = useMemo(() => countBy(items, "entity_type"), [items]);
  const proposedCount = bundles.filter((bundle) => bundle.status === "proposed").length;
  const approvedCount = bundles.filter((bundle) => bundle.status === "approved").length;

  return (
    <div className="mx-auto flex max-w-[1480px] flex-col gap-5">
      <section className="flex flex-col gap-4 border-b border-border pb-5 lg:flex-row lg:items-end lg:justify-between">
        <div className="min-w-0">
          <div className="flex items-center gap-2 text-xs uppercase tracking-[0.12em] text-muted-foreground">
            <ClipboardList className="h-3.5 w-3.5" />
            Build
          </div>
          <h1 className="mt-2 text-2xl font-semibold text-foreground">Review</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Inspect generated changes and apply the bundle to the catalog.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button type="button" variant="outline" onClick={() => void reload()} disabled={loading || approving || rejecting}>
            <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
          <Button
            type="button"
            variant="outline"
            onClick={() => void rejectSelectedBundle()}
            disabled={!selectedBundle || selectedBundle.status === "approved" || selectedBundle.status === "rejected" || approving || rejecting}
          >
            <XCircle className="h-4 w-4" />
            {selectedBundle?.status === "rejected" ? "Rejected" : rejecting ? "Rejecting" : "Reject Bundle"}
          </Button>
          <Button
            type="button"
            onClick={() => void approveSelectedBundle()}
            disabled={!selectedBundle || selectedBundle.status === "approved" || selectedBundle.status === "rejected" || approving || rejecting}
          >
            {selectedBundle?.status === "approved" ? <CheckCircle2 className="h-4 w-4" /> : <PackageCheck className="h-4 w-4" />}
            {selectedBundle?.status === "approved" ? "Applied" : approving ? "Applying" : "Approve & Apply"}
          </Button>
        </div>
      </section>

      {error ? <Notice tone="danger" message={error} /> : null}
      {notice ? <Notice tone="success" message={notice} /> : null}

      <section className="grid gap-3 md:grid-cols-3">
        <Metric icon={ClipboardList} label="Bundles" value={bundles.length} detail={`${proposedCount} proposed`} />
        <Metric icon={CheckCircle2} label="Applied" value={approvedCount} detail="approved bundles" />
        <Metric icon={Layers3} label="Selected Items" value={items.length} detail={selectedBundle?.status || "none"} />
      </section>

      <section className="grid gap-5 xl:grid-cols-[430px_minmax(0,1fr)]">
        <Card>
          <CardHeader>
            <div>
              <CardTitle>Pending Bundles</CardTitle>
              <CardDescription>Generated changes grouped for one approval action.</CardDescription>
            </div>
            <ClipboardCheck className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex flex-col gap-2 sm:flex-row">
              <label className="flex h-9 min-w-0 flex-1 items-center gap-2 rounded-lg border border-border bg-background px-3">
                <Search className="h-4 w-4 shrink-0 text-muted-foreground" />
                <input
                  className="min-w-0 flex-1 bg-transparent text-sm outline-none placeholder:text-muted-foreground"
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                  placeholder="Search bundles"
                />
              </label>
              <Segmented
                value={statusFilter}
                items={[
                  { value: "proposed", label: "Proposed" },
                  { value: "approved", label: "Approved" },
                  { value: "rejected", label: "Rejected" },
                  { value: "all", label: "All" },
                ]}
                onChange={(value) => setStatusFilter(value as StatusFilter)}
              />
            </div>

            <div className="max-h-[46rem] space-y-2 overflow-auto pr-1">
              {filteredBundles.length ? (
                filteredBundles.map((bundle) => {
                  const selected = bundle.id === selectedBundle?.id;
                  return (
                    <button
                      key={bundle.id}
                      type="button"
                      onClick={() => {
                        setSelectedBundleId(bundle.id);
                        void loadItems(bundle.id);
                      }}
                      className={`w-full rounded-lg border px-3 py-3 text-left transition ${
                        selected ? "border-primary/35 bg-primary/[0.08]" : "border-border hover:bg-muted/25"
                      }`}
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="truncate text-sm font-medium text-foreground">{bundle.title}</div>
                          <div className="mt-1 truncate text-xs text-muted-foreground">{bundle.id}</div>
                        </div>
                        <StatusBadge status={bundle.status} />
                      </div>
                      <div className="mt-3 grid grid-cols-3 gap-2 text-xs">
                        <MiniStat label="Items" value={bundle.proposal_count || 0} />
                        <MiniStat label="Canonical" value={summaryCount(bundle, "canonical_decision_counts")} />
                        <MiniStat label="Relations" value={summaryCount(bundle, "relation_decision_counts")} />
                      </div>
                    </button>
                  );
                })
              ) : (
                <EmptyState message={loading ? "Loading bundles..." : "No proposal bundles found."} />
              )}
            </div>
          </CardContent>
        </Card>

        <div className="flex min-w-0 flex-col gap-5">
          <Card>
            <CardHeader>
              <div className="min-w-0">
                <CardTitle className="truncate">{selectedBundle?.title || "Bundle Detail"}</CardTitle>
                <CardDescription>{selectedBundle?.id || "Select a proposal bundle."}</CardDescription>
              </div>
              <StatusBadge status={selectedBundle?.status} />
            </CardHeader>
            <CardContent>
              {selectedBundle ? (
                <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                  <SummaryTile label="Proposal Items" value={selectedBundle.proposal_count || items.length} />
                  <SummaryTile label="Canonical" value={summaryCount(selectedBundle, "canonical_decision_counts")} />
                  <SummaryTile label="Relations" value={summaryCount(selectedBundle, "relation_decision_counts")} />
                  <SummaryTile label="Capabilities" value={summaryCount(selectedBundle, "capability_decision_counts")} />
                </div>
              ) : (
                <EmptyState message="No bundle selected." />
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <div>
                <CardTitle>Proposal Items</CardTitle>
                <CardDescription>Grouped changes that will be applied together.</CardDescription>
              </div>
              <FileText className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              {items.length ? (
                <div className="grid gap-4 xl:grid-cols-[260px_minmax(0,1fr)]">
                  <div className="space-y-2">
                    {Object.entries(itemCounts).map(([entityType, count]) => (
                      <div key={entityType} className="flex items-center justify-between rounded-lg border border-border px-3 py-2 text-sm">
                        <span className="truncate text-muted-foreground">{entityType}</span>
                        <Badge>{count}</Badge>
                      </div>
                    ))}
                  </div>

                  <div className="max-h-[38rem] overflow-auto rounded-lg border border-border">
                    <div className="grid grid-cols-[minmax(220px,1.3fr)_170px_120px_110px] border-b border-border bg-muted/35 px-3 py-2 text-xs font-medium text-muted-foreground">
                      <div>Title</div>
                      <div>Entity</div>
                      <div>Change</div>
                      <div>Status</div>
                    </div>
                    {items.map((item) => (
                      <details key={item.id} className="group border-b border-border last:border-b-0">
                        <summary className="grid cursor-pointer grid-cols-[minmax(220px,1.3fr)_170px_120px_110px] gap-3 px-3 py-2.5 text-sm hover:bg-muted/20">
                          <div className="min-w-0">
                            <div className="truncate font-medium text-foreground">{item.title}</div>
                            <div className="mt-0.5 truncate text-xs text-muted-foreground">{item.id}</div>
                          </div>
                          <div className="truncate text-muted-foreground">{item.entity_type}</div>
                          <div className="truncate text-muted-foreground">{item.change_type}</div>
                          <StatusBadge status={item.status} />
                        </summary>
                        <pre className="max-h-80 overflow-auto border-t border-border bg-muted/20 p-3 text-xs leading-5 text-muted-foreground">
                          {JSON.stringify(item.payload || {}, null, 2)}
                        </pre>
                      </details>
                    ))}
                  </div>
                </div>
              ) : (
                <EmptyState message={itemsLoading ? "Loading proposal items..." : "No proposal items for this bundle."} />
              )}
            </CardContent>
          </Card>
        </div>
      </section>
    </div>
  );
}

function sortBundles(left: ContextProposalBundle, right: ContextProposalBundle) {
  const leftTime = Date.parse(left.created_at || left.updated_at || "");
  const rightTime = Date.parse(right.created_at || right.updated_at || "");
  return (Number.isFinite(rightTime) ? rightTime : 0) - (Number.isFinite(leftTime) ? leftTime : 0);
}

function countBy<T extends Record<string, unknown>>(items: T[], key: keyof T) {
  return items.reduce<Record<string, number>>((acc, item) => {
    const value = String(item[key] || "unknown");
    acc[value] = (acc[value] || 0) + 1;
    return acc;
  }, {});
}

function summaryCount(bundle: ContextProposalBundle, key: string): number {
  const value = bundle.summary?.[key];
  if (!value || typeof value !== "object" || Array.isArray(value)) return 0;
  return Object.values(value as Record<string, unknown>).reduce<number>((total, item) => {
    const numeric = typeof item === "number" ? item : Number(item);
    return total + (Number.isFinite(numeric) ? numeric : 0);
  }, 0);
}

function Metric({
  icon: Icon,
  label,
  value,
  detail,
}: {
  icon: typeof ClipboardList;
  label: string;
  value: string | number;
  detail: string;
}) {
  return (
    <div className="rounded-lg border border-border bg-card px-4 py-3">
      <div className="flex items-center justify-between gap-3">
        <div className="text-xs font-medium uppercase tracking-[0.12em] text-muted-foreground">{label}</div>
        <Icon className="h-4 w-4 text-muted-foreground" />
      </div>
      <div className="mt-3 text-2xl font-semibold text-foreground">{value}</div>
      <div className="mt-1 text-xs text-muted-foreground">{detail}</div>
    </div>
  );
}

function SummaryTile({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-lg border border-border bg-muted/20 px-3 py-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="mt-2 text-lg font-semibold text-foreground">{value}</div>
    </div>
  );
}

function MiniStat({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-md bg-muted/30 px-2 py-1.5">
      <div className="text-[11px] text-muted-foreground">{label}</div>
      <div className="mt-0.5 font-medium text-foreground">{value}</div>
    </div>
  );
}

function Segmented({
  value,
  items,
  onChange,
}: {
  value: string;
  items: Array<{ value: string; label: string }>;
  onChange: (value: string) => void;
}) {
  return (
    <div className="flex h-9 rounded-lg border border-border bg-muted/30 p-1">
      {items.map((item) => (
        <button
          key={item.value}
          type="button"
          onClick={() => onChange(item.value)}
          className={`rounded-md px-2 text-xs transition ${
            value === item.value
              ? "bg-primary text-primary-foreground"
              : "text-muted-foreground hover:bg-accent hover:text-foreground"
          }`}
        >
          {item.label}
        </button>
      ))}
    </div>
  );
}

function StatusBadge({ status }: { status?: string | null }) {
  const normalized = String(status || "draft").toLowerCase();
  if (normalized === "approved" || normalized === "published") {
    return <Badge variant="success">{normalized}</Badge>;
  }
  if (normalized === "proposed" || normalized === "review") {
    return <Badge variant="warning">{normalized}</Badge>;
  }
  if (normalized === "failed" || normalized === "rejected") {
    return <Badge variant="danger">{normalized}</Badge>;
  }
  return <Badge>{normalized}</Badge>;
}

function EmptyState({ message }: { message: string }) {
  return (
    <div className="flex min-h-28 items-center justify-center rounded-lg border border-dashed border-border px-4 py-8 text-center text-sm text-muted-foreground">
      {message}
    </div>
  );
}

function Notice({ message, tone }: { message: string; tone: "success" | "danger" }) {
  const className =
    tone === "success"
      ? "border-emerald-500/25 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
      : "border-red-500/25 bg-red-500/10 text-red-700 dark:text-red-300";
  return <div className={`rounded-lg border px-4 py-3 text-sm ${className}`}>{message}</div>;
}
