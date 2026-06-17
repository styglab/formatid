"use client";

import { useMemo, useState } from "react";
import {
  type ColumnDef,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  type SortingState,
  useReactTable,
} from "@tanstack/react-table";
import { ArrowUpDown, ChevronDown, ChevronUp } from "lucide-react";
export type MappingTableRow = {
  id: string;
  kind: "mapped" | "unmapped";
  fieldLabel: string;
  fieldPath: string;
  sourceLabel: string;
  operationId: string;
  semanticTypeLabel: string;
  semanticTypeId: string;
  pendingProposalId: string;
  reviewState: string;
  mappedId?: string;
};

export function MappingsTable({
  items,
  selectedId,
  selectedIds,
  onSelect,
  onToggleSelection,
  onToggleAllVisible,
  onOpenProposal
}: {
  items: MappingTableRow[];
  selectedId: string;
  selectedIds: string[];
  onSelect: (id: string) => void;
  onToggleSelection: (id: string, checked: boolean) => void;
  onToggleAllVisible: (checked: boolean) => void;
  onOpenProposal?: (proposalId: string) => void;
}) {
  const [sorting, setSorting] = useState<SortingState>([{ id: "field", desc: false }]);

  const columns = useMemo<ColumnDef<MappingTableRow>[]>(
    () => [
      {
        id: "select",
        header: "",
        cell: () => null,
        enableSorting: false,
        meta: { headerClassName: "w-[40px] px-3 py-2.5", cellClassName: "w-[40px] px-3 py-2.5" },
      },
      {
        id: "field",
        header: "Field",
        accessorFn: (row) => `${row.fieldLabel} ${row.fieldPath} ${row.sourceLabel} ${row.operationId}`,
        cell: ({ row }) => (
          <div className="min-w-0">
            <div className="truncate text-[13px] font-semibold leading-4 text-foreground">{row.original.fieldLabel}</div>
            <div className="mt-0.5 truncate text-[10px] leading-3.5 text-muted-foreground">
              {row.original.sourceLabel} · {row.original.operationId}
              <span className="mx-1.5 text-muted-foreground/60">·</span>
              <span className="font-mono text-muted-foreground/80">{row.original.fieldPath}</span>
            </div>
          </div>
        ),
        meta: {
          headerClassName: "sticky left-0 z-20 w-[50%] bg-slate-50/70 px-3 py-2.5",
          cellClassName: "sticky left-0 z-10 w-[50%] px-3 py-2.5",
        },
      },
      {
        id: "semanticType",
        header: "Semantic Type",
        accessorFn: (row) => row.semanticTypeLabel,
        cell: ({ row }) => (
          <div className="min-w-0">
            <div className="truncate text-[13px] font-semibold leading-4 text-foreground">
              {row.original.semanticTypeLabel || "Not mapped"}
            </div>
            <div className="mt-0.5 truncate font-mono text-[10px] leading-3.5 text-muted-foreground/90">
              {row.original.semanticTypeId || "Select a semantic type"}
            </div>
          </div>
        ),
        meta: { headerClassName: "w-[30%] px-3 py-2.5", cellClassName: "w-[30%] px-3 py-2.5" },
      },
      {
        id: "review",
        header: "Review",
        accessorFn: (row) => row.reviewState,
        cell: ({ row }) => (
          <div className="min-w-0">
            <div className="truncate text-[11px] leading-4 text-foreground">{row.original.reviewState}</div>
            {row.original.pendingProposalId ? <button type="button" className="mt-0.5 truncate font-mono text-[10px] leading-3.5 text-amber-700 underline-offset-2 hover:underline" onClick={(event) => { event.stopPropagation(); onOpenProposal?.(row.original.pendingProposalId); }}>proposal {row.original.pendingProposalId}</button> : null}
          </div>
        ),
        meta: { headerClassName: "w-[20%] px-3 py-2.5", cellClassName: "w-[20%] px-3 py-2.5" },
      },
    ],
    []
  );

  const table = useReactTable({
    data: items,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  const visibleIds = table.getRowModel().rows.filter((row) => row.original.kind === "mapped").map((row) => row.original.id);
  const allVisibleSelected = visibleIds.length > 0 && visibleIds.every((id) => selectedIds.includes(id));

  return (
    <table className="min-w-full table-fixed text-left text-[12px]">
      <thead className="sticky top-0 z-10 border-b border-border/70 bg-slate-50/70 text-[10px] uppercase tracking-[0.08em] text-muted-foreground backdrop-blur">
        {table.getHeaderGroups().map((headerGroup) => (
          <tr key={headerGroup.id}>
            {headerGroup.headers.map((header) => {
              const meta = (header.column.columnDef.meta as { headerClassName?: string; cellClassName?: string } | undefined);
              const className = meta?.headerClassName || "px-3 py-2.5";
              const sorted = header.column.getIsSorted();
              return (
                <th key={header.id} className={`${className} font-medium`}>
                  {header.id === "select" ? (
                    <input
                      type="checkbox"
                      checked={allVisibleSelected}
                      onChange={(event) => onToggleAllVisible(event.target.checked)}
                      className="h-4 w-4 cursor-pointer rounded border-border"
                      aria-label="Select visible mappings"
                    />
                  ) : header.isPlaceholder ? null : (
                    <button
                      type="button"
                    className="flex items-center gap-1.5 text-left transition hover:text-foreground"
                      onClick={header.column.getToggleSortingHandler()}
                    >
                      <span>{flexRender(header.column.columnDef.header, header.getContext())}</span>
                      {sorted === "asc" ? (
                        <ChevronUp className="h-3.5 w-3.5" />
                      ) : sorted === "desc" ? (
                        <ChevronDown className="h-3.5 w-3.5" />
                      ) : (
                        <ArrowUpDown className="h-3.5 w-3.5 opacity-50" />
                      )}
                    </button>
                  )}
                </th>
              );
            })}
          </tr>
        ))}
      </thead>
      <tbody className="divide-y divide-slate-200/80">
        {table.getRowModel().rows.map((row) => {
          const selected = row.original.id === selectedId;
          return (
            <tr
              key={row.id}
              className={`cursor-pointer align-top transition-colors hover:bg-primary/[0.04] hover:shadow-[inset_2px_0_0_0_hsl(var(--primary))] ${selected ? "bg-primary/[0.08] shadow-[inset_2px_0_0_0_hsl(var(--primary))]" : ""}`}
              onClick={() => onSelect(row.original.id)}
            >
              {row.getVisibleCells().map((cell) => {
                const meta = (cell.column.columnDef.meta as { headerClassName?: string; cellClassName?: string } | undefined);
                const className = meta?.cellClassName || "px-3 py-2.5";
                return (
                  <td
                    key={cell.id}
                    className={className}
                  >
                    {cell.column.id === "select" ? (
                      row.original.kind === "mapped" ? (
                        <input
                          type="checkbox"
                          checked={selectedIds.includes(row.original.id)}
                          onClick={(event) => event.stopPropagation()}
                          onChange={(event) => onToggleSelection(row.original.id, event.target.checked)}
                          className="h-4 w-4 cursor-pointer rounded border-border"
                          aria-label={`Select ${row.original.fieldLabel}`}
                        />
                      ) : null
                    ) : (
                      flexRender(cell.column.columnDef.cell, cell.getContext())
                    )}
                  </td>
                );
              })}
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
