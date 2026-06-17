import type { ReactNode } from "react";

export function WorkbenchSplit({
  list,
  detail,
  className = "xl:grid-cols-[minmax(0,1.1fr)_minmax(380px,0.9fr)]"
}: {
  list: ReactNode;
  detail: ReactNode;
  className?: string;
}) {
  return (
    <div className={`grid gap-0 overflow-hidden rounded-2xl border border-border/70 bg-card/55 ${className}`}>
      <div className="min-w-0 border-b border-border/70 bg-background/72 xl:border-b-0 xl:border-r">{list}</div>
      <aside className="min-w-0 bg-muted/[0.18] xl:sticky xl:top-24 xl:max-h-[calc(100vh-8rem)] xl:overflow-auto">
        {detail}
      </aside>
    </div>
  );
}
