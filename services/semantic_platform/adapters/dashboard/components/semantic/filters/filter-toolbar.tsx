"use client";

import type { ReactNode } from "react";
import { Search } from "lucide-react";
import { Input } from "@/components/ui/input";

type FilterToolbarProps = {
  query: string;
  onQueryChange: (value: string) => void;
  queryPlaceholder: string;
  status: string;
  onStatusChange: (value: string) => void;
  statusOptions?: Array<{ value: string; label: string }>;
  statusLabel?: string;
  extra?: ReactNode;
};

const defaultStatusOptions = [
  { value: "all", label: "All Statuses" },
  { value: "approved", label: "Approved" },
  { value: "draft", label: "Draft" },
  { value: "review", label: "Review" },
  { value: "pending_review", label: "Pending Review" },
  { value: "published", label: "Published" },
  { value: "deprecated", label: "Deprecated" },
  { value: "rejected", label: "Rejected" }
];

export function FilterToolbar({
  query,
  onQueryChange,
  queryPlaceholder,
  status,
  onStatusChange,
  statusOptions = defaultStatusOptions,
  statusLabel = "Status",
  extra
}: FilterToolbarProps) {
  return (
    <div className="flex flex-col gap-3 rounded-xl border border-border/70 bg-card/60 p-3 md:flex-row md:items-center">
      <label className="relative block min-w-0 md:flex-[1.2]">
        <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
        <Input
          value={query}
          onChange={(event) => onQueryChange(event.target.value)}
          placeholder={queryPlaceholder}
          className="pl-9"
        />
      </label>
      <label className="flex items-center gap-2 text-sm text-muted-foreground md:shrink-0">
        <span>{statusLabel}</span>
        <select
          value={status}
          onChange={(event) => onStatusChange(event.target.value)}
          className="h-9 rounded-lg border border-border bg-background px-3 text-sm text-foreground"
        >
          {statusOptions.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </label>
      {extra ? <div className="flex flex-1 flex-wrap items-center justify-end gap-2">{extra}</div> : null}
    </div>
  );
}
