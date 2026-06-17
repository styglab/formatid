import type { ReactNode } from "react";

export function InspectorEmpty({
  title,
  message
}: {
  title: string;
  message: string;
}) {
  return (
    <div className="space-y-3 p-3">
      <div className="border-b border-border/70 pb-3">
        <h3 className="text-base font-semibold text-foreground">{title}</h3>
      </div>
      <div className="text-sm text-muted-foreground">{message}</div>
    </div>
  );
}

export function InspectorShell({
  title,
  subtitle,
  actions,
  children
}: {
  title: string;
  subtitle?: string;
  actions?: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="space-y-4 p-3">
      <div className="flex items-start justify-between gap-4 border-b border-border/70 pb-3">
        <div className="min-w-0">
          <h3 className="truncate text-base font-semibold text-foreground">{title}</h3>
          {subtitle ? <div className="mt-1 text-xs text-muted-foreground">{subtitle}</div> : null}
        </div>
        {actions ? <div className="shrink-0">{actions}</div> : null}
      </div>
      {children}
    </div>
  );
}
