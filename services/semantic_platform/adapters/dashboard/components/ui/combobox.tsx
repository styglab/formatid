"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { Check, ChevronsUpDown } from "lucide-react";
import { cn } from "@/lib/utils";

export type ComboboxOption = {
  value: string;
  label: string;
  description?: string;
  meta?: string;
};

type ComboboxProps = {
  value: string;
  options: ComboboxOption[];
  onValueChange: (value: string) => void;
  placeholder?: string;
  searchPlaceholder?: string;
  emptyLabel?: string;
  allowCustomInput?: boolean;
  customInputValue?: string;
  onCustomInputChange?: (value: string) => void;
  disabled?: boolean;
};

export function Combobox({
  value,
  options,
  onValueChange,
  placeholder = "Select",
  searchPlaceholder = "Search",
  emptyLabel = "No results",
  allowCustomInput = false,
  customInputValue,
  onCustomInputChange,
  disabled = false,
}: ComboboxProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");

  const selectedOption = useMemo(
    () => options.find((item) => item.value === value) || null,
    [options, value]
  );

  useEffect(() => {
    if (!allowCustomInput) {
      setQuery(selectedOption?.label || "");
      return;
    }
    setQuery(customInputValue ?? selectedOption?.label ?? "");
  }, [allowCustomInput, customInputValue, selectedOption]);

  useEffect(() => {
    function handlePointerDown(event: MouseEvent) {
      if (!rootRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    }

    document.addEventListener("mousedown", handlePointerDown);
    return () => document.removeEventListener("mousedown", handlePointerDown);
  }, []);

  const filteredOptions = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    if (!normalized) return options;
    return options.filter((item) => {
      const haystack = `${item.label} ${item.description || ""} ${item.meta || ""}`.toLowerCase();
      return haystack.includes(normalized);
    });
  }, [options, query]);

  return (
    <div ref={rootRef} className="relative">
      <div className="relative">
        <input
          value={query}
          placeholder={placeholder}
          onFocus={() => { if (!disabled) setOpen(true); }}
          onChange={(event) => {
            const nextValue = event.target.value;
            setQuery(nextValue);
            if (!disabled) setOpen(true);
            if (allowCustomInput && onCustomInputChange) {
              onCustomInputChange(nextValue);
            } else {
              if (!nextValue.trim()) {
                onValueChange("");
              } else if (selectedOption && nextValue !== selectedOption.label) {
                onValueChange("");
              }
            }
          }}
          disabled={disabled}
          readOnly={disabled}
          className={`h-10 w-full rounded-lg border border-border/80 bg-muted/50 px-3 pr-10 text-sm text-foreground placeholder:text-muted-foreground outline-none transition focus:border-primary/70 focus:ring-2 focus:ring-primary/20 ${disabled ? "cursor-not-allowed opacity-75" : ""}`}
        />
        <button
          type="button"
          aria-label="Toggle options"
          disabled={disabled}
          className="absolute right-0 top-0 flex h-10 w-10 items-center justify-center text-muted-foreground disabled:cursor-not-allowed disabled:opacity-60"
          onClick={() => { if (!disabled) setOpen((current) => !current); }}
        >
          <ChevronsUpDown className="h-4 w-4" />
        </button>
      </div>
      {open ? (
        <div className="absolute left-0 right-0 top-[calc(100%+0.375rem)] z-30 overflow-hidden rounded-xl border border-border/80 bg-background shadow-lg">
          <div className="border-b border-border/70 px-3 py-2 text-[11px] uppercase tracking-[0.12em] text-muted-foreground">
            {searchPlaceholder}
          </div>
          <div className="max-h-72 overflow-auto p-1.5">
            {filteredOptions.length ? (
              filteredOptions.map((item) => {
                const active = item.value === value;
                return (
                  <button
                    key={item.value}
                    type="button"
                    className={cn(
                      "flex w-full items-start justify-between gap-3 rounded-lg px-3 py-2.5 text-left transition hover:bg-muted/40",
                      active ? "bg-primary/[0.06]" : ""
                    )}
                    onClick={() => {
                      onValueChange(item.value);
                      if (allowCustomInput && onCustomInputChange) {
                        onCustomInputChange(item.label);
                      }
                      setQuery(item.label);
                      setOpen(false);
                    }}
                  >
                    <div className="min-w-0">
                      <div className="truncate text-sm text-foreground">{item.label}</div>
                      {item.description ? <div className="mt-0.5 truncate text-xs text-muted-foreground">{item.description}</div> : null}
                      {item.meta ? <div className="mt-0.5 truncate font-mono text-[11px] text-muted-foreground/80">{item.meta}</div> : null}
                    </div>
                    {active ? <Check className="mt-0.5 h-4 w-4 shrink-0 text-primary" /> : null}
                  </button>
                );
              })
            ) : (
              <div className="px-3 py-3 text-sm text-muted-foreground">{emptyLabel}</div>
            )}
          </div>
        </div>
      ) : null}
    </div>
  );
}
