"use client";

import { useEffect, useMemo, useState } from "react";
import { RefreshCw } from "lucide-react";
import { reviewProposal } from "@/api/semantic-admin";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { TablePanel } from "@/components/layout/table-panel";
import { InspectorPanel } from "@/components/layout/inspector-panel";
import { FilterToolbar } from "@/components/semantic/filters/filter-toolbar";
import { EmptyPanel, ErrorPanel, InfoLine, LoadingPanel } from "@/components/semantic/common/state-panel";
import { PaginationBar } from "@/components/semantic/common/pagination-bar";
import { ProposalInspector } from "@/components/semantic/inspector/proposal-inspector";
import { ProposalsTable } from "@/components/semantic/tables/proposals-table";
import { usePendingProposals } from "@/hooks/semantic/use-proposals";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

export default function ProposalsPage() {
  const [selectedId, setSelectedId] = useState("");
  const [query, setQuery] = useState("");
  const [proposalIdsFilter, setProposalIdsFilter] = useState<string[]>([]);
  const [page, setPage] = useState(1);
  const pageSize = proposalIdsFilter.length ? Math.max(proposalIdsFilter.length, 12) : 12;
  const { data, loading, error, reload } = usePendingProposals({ query, ids: proposalIdsFilter, page, pageSize });
  const [submitting, setSubmitting] = useState("");

  useEffect(() => {
    if (typeof window === "undefined") return;
    const params = new URLSearchParams(window.location.search);
    const nextQuery = params.get("query") || "";
    const rawIds = params.get("ids") || "";
    const nextIds = rawIds
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
    if (nextQuery) {
      setQuery(nextQuery);
      setPage(1);
    }
    if (nextIds.length) {
      setProposalIdsFilter(nextIds);
      setPage(1);
    }
  }, []);
  const filteredProposals = useMemo(() => {
    if (!proposalIdsFilter.length) return data.items;
    return data.items.filter((item) => proposalIdsFilter.includes(item.id));
  }, [data.items, proposalIdsFilter]);
  const selectedProposal = useMemo(
    () => filteredProposals.find((item) => item.id === selectedId) || filteredProposals[0] || null,
    [filteredProposals, selectedId]
  );

  useEffect(() => {
    if (!filteredProposals.length) {
      setSelectedId("");
      return;
    }
    const queryMatch = filteredProposals.find((item) => item.id === query);
    if (queryMatch) {
      setSelectedId(queryMatch.id);
      return;
    }
    if (!selectedId || !filteredProposals.some((item) => item.id === selectedId)) {
      setSelectedId(filteredProposals[0].id);
    }
  }, [filteredProposals, query, selectedId]);

  async function handleReview(proposalId: string, decision: "approve" | "reject") {
    setSubmitting(`${proposalId}:${decision}`);
    try {
      await reviewProposal(proposalId, decision);
      await reload();
    } finally {
      setSubmitting("");
    }
  }

  return (
    <SectionPlaceholder
      title="Proposals"
      description="Review pending semantic, mapping, and capability changes with evidence, diff, and approval actions."
      actions={
        <Button type="button" variant="outline" onClick={() => void reload()} disabled={loading}>
          <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      }
      body={
        <div className="space-y-4">
          <InfoLine>
            <Badge variant="warning">{filteredProposals.length} pending</Badge>
            <Badge variant="default">{selectedProposal ? "1 selected" : "0 selected"}</Badge>
          </InfoLine>

          <FilterToolbar
            query={query}
            onQueryChange={(next) => {
              setQuery(next);
              setProposalIdsFilter([]);
              setPage(1);
            }}
            queryPlaceholder="Search proposals by title, entity type, or entity id"
            status="pending_review"
            onStatusChange={() => {}}
            statusOptions={[{ value: "pending_review", label: "Pending Review" }]}
          />

          {error ? <ErrorPanel message={error} /> : null}

          {loading ? <LoadingPanel message="Loading proposals..." /> : null}

          {!loading && !error ? (
            <div className="grid gap-5 xl:grid-cols-[minmax(0,1.05fr)_minmax(420px,0.95fr)]">
              <TablePanel footer={<PaginationBar page={page} pageSize={pageSize} total={data.total} onPageChange={setPage} />}>
                <ProposalsTable items={filteredProposals} selectedId={selectedProposal?.id || ""} onSelect={setSelectedId} />
              </TablePanel>
              <InspectorPanel>
                <ProposalInspector
                  proposal={selectedProposal}
                  submitting={submitting}
                  onApprove={(id) => void handleReview(id, "approve")}
                  onReject={(id) => void handleReview(id, "reject")}
                />
              </InspectorPanel>
            </div>
          ) : null}

          {!loading && !error && !filteredProposals.length ? <EmptyPanel message="No pending proposals match the current filters." /> : null}
        </div>
      }
    />
  );
}
