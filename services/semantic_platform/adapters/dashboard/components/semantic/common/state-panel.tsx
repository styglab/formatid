import type { ReactNode } from "react";

export function ErrorPanel({ message }: { message: string }) {
  return (
    <div className="rounded-xl border border-red-500/20 bg-red-500/5 px-4 py-3 text-sm text-red-700 dark:text-red-300">
      {message}
    </div>
  );
}

export function LoadingPanel({ message }: { message: string }) {
  return <div className="rounded-xl border border-dashed border-border px-4 py-6 text-sm text-muted-foreground">{message}</div>;
}

export function EmptyPanel({ message }: { message: string }) {
  return <div className="rounded-xl border border-dashed border-border px-4 py-6 text-sm text-muted-foreground">{message}</div>;
}

export function InfoLine({ children }: { children: ReactNode }) {
  return <div className="flex flex-wrap items-center gap-2 text-sm">{children}</div>;
}
