"use client";

import { usePathname } from "next/navigation";
import { useTheme } from "next-themes";
import { Layers3 } from "lucide-react";
import { findContextNavContext } from "@/lib/context/navigation";

export function ContextTopbar() {
  const pathname = usePathname();
  const currentContext = findContextNavContext(pathname);
  const { resolvedTheme, setTheme } = useTheme();
  const screenMode = resolvedTheme === "dark" ? "dark" : "light";

  return (
    <header className="sticky top-0 z-20 flex h-14 items-center justify-between gap-4 border-b border-border bg-background/95 px-5 backdrop-blur">
      <div className="min-w-0">
        <div className="flex items-center gap-2 text-xs uppercase tracking-[0.12em] text-muted-foreground">
          <Layers3 className="h-3.5 w-3.5" />
          <span>{currentContext?.section.title || "Context Platform"}</span>
        </div>
        <h1 className="mt-0.5 truncate text-sm font-semibold text-foreground">
          {currentContext?.item.label || "Context Platform"}
        </h1>
      </div>

      <Segmented
        value={screenMode}
        items={[
          { value: "light", label: "Light" },
          { value: "dark", label: "Dark" },
        ]}
        onChange={(value) => setTheme(value)}
      />
    </header>
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
    <div className="flex rounded-lg border border-border bg-muted/30 p-1">
      {items.map((item) => (
        <button
          key={item.value}
          type="button"
          onClick={() => onChange(item.value)}
          className={`h-7 rounded-md px-2 text-xs transition ${
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
