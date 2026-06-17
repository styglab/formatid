"use client";

import { useMemo, useState } from "react";
import { RefreshCw } from "lucide-react";
import { SectionPlaceholder } from "@/components/layout/section-placeholder";
import { FilterToolbar } from "@/components/semantic/filters/filter-toolbar";
import { OperationInspector } from "@/components/semantic/inspector/operation-inspector";
import { EmptyPanel, ErrorPanel, InfoLine, LoadingPanel } from "@/components/semantic/common/state-panel";
import { OperationsTable } from "@/components/semantic/tables/operations-table";
import { useOperationFields, useOperations } from "@/hooks/semantic/use-operations";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

export default function OperationCatalogPage() {
  const { data: operations, loading, error, reload } = useOperations({ page: 1, pageSize: 200 });
  const { data: fields, loading: fieldsLoading, error: fieldsError, reload: reloadFields } = useOperationFields();
  const [selectedId, setSelectedId] = useState("");
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");

  const filteredOperations = useMemo(
    () =>
      operations.items.filter((item) => {
        const lowered = query.toLowerCase();
        const matchesQuery =
          !query ||
          item.name.toLowerCase().includes(lowered) ||
          item.operation_key.toLowerCase().includes(lowered) ||
          String(item.source_name || "").toLowerCase().includes(lowered);
        const matchesStatus = status === "all" || (item.status || "") === status;
        return matchesQuery && matchesStatus;
      }),
    [operations.items, query, status]
  );

  const fieldCounts = useMemo(
    () =>
      fields.reduce<Record<string, number>>((accumulator, field) => {
        accumulator[field.operation_id] = (accumulator[field.operation_id] || 0) + 1;
        return accumulator;
      }, {}),
    [fields]
  );

  const selectedOperation = useMemo(
    () => filteredOperations.find((item) => item.id === selectedId) || filteredOperations[0] || null,
    [filteredOperations, selectedId]
  );

  const selectedFields = useMemo(
    () => fields.filter((field) => field.operation_id === selectedOperation?.id),
    [fields, selectedOperation]
  );

  return (
    <SectionPlaceholder
      title="Operations"
      description="Review executable operation implementations, access metadata, and field structure that capabilities and variants depend on."
      actions={
        <Button
          type="button"
          variant="outline"
          onClick={() => {
            void reload();
            void reloadFields();
          }}
          disabled={loading || fieldsLoading}
        >
          <RefreshCw className={`h-4 w-4 ${loading || fieldsLoading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      }
      body={
        <div className="space-y-4">
          <InfoLine>
            <Badge variant="info">{filteredOperations.length} operations</Badge>
            <span>This view frames operation implementations that capabilities and variants compose.</span>
          </InfoLine>

          <FilterToolbar
            query={query}
            onQueryChange={setQuery}
            queryPlaceholder="Search operation catalog by name, key, or source"
            status={status}
            onStatusChange={setStatus}
          />

          {error || fieldsError ? <ErrorPanel message={error || fieldsError} /> : null}
          {loading ? <LoadingPanel message="Loading operation catalog..." /> : null}

          {!loading && !error ? (
            <div className="grid gap-4 xl:grid-cols-[minmax(0,1.15fr)_minmax(380px,0.85fr)]">
              <OperationsTable
                items={filteredOperations}
                selectedId={selectedOperation?.id || ""}
                onSelect={setSelectedId}
                fieldCounts={fieldCounts}
              />
              <OperationInspector operation={selectedOperation} fields={selectedFields} />
            </div>
          ) : null}

          {!loading && !error && !filteredOperations.length ? <EmptyPanel message="No operation catalog entries match the current filters." /> : null}
        </div>
      }
    />
  );
}
