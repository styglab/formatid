import * as React from "react";

export function InspectorSection({
  title,
  children
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section className="space-y-2">
      <div className="text-xs uppercase tracking-[0.12em] text-muted-foreground">{title}</div>
      {children}
    </section>
  );
}

export function InspectorSurface({
  children,
  muted = false
}: {
  children: React.ReactNode;
  muted?: boolean;
}) {
  return (
    <div className={`rounded-xl border border-border/70 p-4 ${muted ? "bg-muted/15" : "bg-background"}`}>{children}</div>
  );
}

export function InspectorJson({ value }: { value: unknown }) {
  return <pre className="overflow-x-auto whitespace-pre-wrap text-xs leading-6 text-foreground">{JSON.stringify(value, null, 2)}</pre>;
}
