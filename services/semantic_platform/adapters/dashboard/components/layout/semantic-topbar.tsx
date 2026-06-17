"use client";

import { useState } from "react";
import { usePathname } from "next/navigation";
import { useTheme } from "next-themes";
import { Bell, Layers3 } from "lucide-react";
import { findSemanticNavContext } from "@/lib/semantic/navigation";
import { usePendingProposals } from "@/hooks/semantic/use-proposals";

export function SemanticTopbar() {
  const pathname = usePathname();
  const currentContext = findSemanticNavContext(pathname);
  const [language, setLanguage] = useState<"ko" | "en">("ko");
  const { resolvedTheme, setTheme } = useTheme();
  const screenMode = resolvedTheme === "dark" ? "dark" : "light";
  const { data: proposals } = usePendingProposals({ page: 1, pageSize: 5 });
  const pendingCount = proposals.total;
  const notifications = proposals.items.map((proposal) => ({
    id: proposal.id,
    title: proposal.title,
    meta: `${proposal.entity_type} · ${proposal.change_type}`,
  }));

  return (
    <header className="sticky top-0 z-20 flex h-16 items-center justify-between gap-4 border-b border-border bg-background/88 px-5 backdrop-blur">
      <div className="min-w-0">
        <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-muted-foreground">
          <Layers3 className="h-3.5 w-3.5" />
          <span>{currentContext?.section.title || "Semantic Platform"}</span>
        </div>
        <div className="mt-1 flex items-center gap-2">
          <h1 className="truncate text-lg font-semibold text-foreground">{currentContext?.item.label || "Semantic Platform"}</h1>
        </div>
      </div>

      <div className="flex items-center gap-2">
        <Segmented
          value={language}
          items={[
            { value: "ko", label: "한글" },
            { value: "en", label: "EN" },
          ]}
          onChange={(value) => setLanguage(value as "ko" | "en")}
        />
        <Segmented
          value={screenMode}
          items={[
            { value: "light", label: "Light" },
            { value: "dark", label: "Dark" },
          ]}
          onChange={(value) => setTheme(value)}
        />
        <div className="group relative">
          <button
            type="button"
            className="relative rounded-xl border border-border bg-card/70 p-2 text-muted-foreground transition hover:bg-accent hover:text-foreground"
            aria-label="Notifications"
          >
            <Bell className="h-4 w-4" />
            {pendingCount ? (
              <span className="absolute -right-1.5 -top-1.5 inline-flex h-5 min-w-5 items-center justify-center rounded-full bg-primary px-1 text-[10px] font-semibold text-primary-foreground">
                {pendingCount}
              </span>
            ) : null}
          </button>
          <div className="pointer-events-none absolute right-0 top-12 z-30 w-80 translate-y-1 rounded-2xl border border-border bg-background/98 p-3 opacity-0 shadow-2xl backdrop-blur transition duration-150 group-hover:pointer-events-auto group-hover:translate-y-0 group-hover:opacity-100">
            <div className="mb-2 flex items-center justify-between">
              <div className="text-sm font-semibold">Notifications</div>
              <div className="text-xs text-muted-foreground">{pendingCount} pending</div>
            </div>
            <div className="space-y-2">
              {notifications.length ? (
                notifications.map((notification, index) => (
                  <div key={notification.id} className="rounded-xl border border-border/70 bg-muted/20 px-3 py-2.5">
                    <div className="flex items-start gap-3">
                      <div className="mt-0.5 inline-flex h-5 min-w-5 items-center justify-center rounded-full bg-primary/12 text-[10px] font-semibold text-primary">
                        {index + 1}
                      </div>
                      <div className="min-w-0">
                        <div className="truncate text-sm font-medium">{notification.title}</div>
                        <div className="mt-0.5 text-xs text-muted-foreground">{notification.meta}</div>
                      </div>
                    </div>
                  </div>
                ))
              ) : (
                <div className="rounded-xl border border-dashed border-border/70 px-3 py-6 text-center text-sm text-muted-foreground">
                  No pending reviews
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
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
              ? "bg-primary text-primary-foreground shadow-sm"
              : "text-muted-foreground hover:bg-accent hover:text-foreground"
          }`}
        >
          {item.label}
        </button>
      ))}
    </div>
  );
}
