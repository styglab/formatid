import type { ReactNode } from "react";

export function InspectorPanel({ children }: { children: ReactNode }) {
  return (
    <aside className="min-w-0 self-start rounded-2xl border border-border/70 bg-muted/[0.14] xl:sticky xl:top-24 xl:max-h-[calc(100vh-8rem)] xl:overflow-auto">
      {children}
    </aside>
  );
}
