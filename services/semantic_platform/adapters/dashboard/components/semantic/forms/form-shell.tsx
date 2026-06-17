"use client";

import { useEffect, type FormEvent, type ReactNode } from "react";
import { X } from "lucide-react";
import { Button } from "@/components/ui/button";

type FormShellProps = {
  title: string;
  description: string;
  actions?: ReactNode;
  onSubmit?: (event: FormEvent<HTMLFormElement>) => void;
  onCancel: () => void;
  submitLabel?: string;
  submitting?: boolean;
  children: ReactNode;
};

export function FormShell({ title, description, actions, onSubmit, onCancel, submitLabel, submitting, children }: FormShellProps) {
  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onCancel();
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onCancel]);

  return (
    <div className="fixed inset-0 z-[60]">
      <button
        type="button"
        aria-label="Close form"
        className="absolute inset-0 bg-transparent"
        onClick={onCancel}
      />
      <section
        role="dialog"
        aria-modal="true"
        aria-label={title}
        className="fixed right-0 top-0 bottom-0 flex w-full max-w-[720px] flex-col border-l border-border/80 bg-background shadow-2xl"
      >
        <header className="flex items-start justify-between gap-4 border-b border-border/70 px-4 py-2.5">
          <div className="min-w-0">
            <h3 className="text-base font-semibold text-foreground">{title}</h3>
            <p className="mt-0.5 text-sm text-muted-foreground">{description}</p>
          </div>
          <div className="flex shrink-0 items-center gap-2">
            {actions}
            <Button type="button" variant="ghost" size="icon" onClick={onCancel} aria-label="Close form">
              <X className="h-4 w-4" />
            </Button>
          </div>
        </header>
        {onSubmit ? (
          <form className="flex min-h-0 flex-1 flex-col" onSubmit={onSubmit}>
            <div className="min-h-0 flex-1 overflow-y-auto px-4 py-2.5">{children}</div>
            {!actions && submitLabel ? (
              <div className="flex items-center justify-end gap-2 border-t border-border/70 bg-background px-4 py-3">
                <Button type="button" variant="outline" onClick={onCancel}>
                  Cancel
                </Button>
                <Button type="submit" disabled={submitting}>
                  {submitLabel}
                </Button>
              </div>
            ) : null}
          </form>
        ) : (
          <div className="min-h-0 flex-1 overflow-y-auto px-4 py-3">{children}</div>
        )}
      </section>
    </div>
  );
}

export function FormGrid({ children }: { children: ReactNode }) {
  return <div className="grid gap-4 md:grid-cols-2">{children}</div>;
}

export function FormField({
  label,
  action,
  children
}: {
  label: string;
  action?: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="block space-y-1.5">
      <span className="flex min-h-7 items-center justify-between gap-3 text-xs font-medium uppercase tracking-[0.12em] text-muted-foreground">
        <span>{label}</span>
        {action ? <span className="shrink-0">{action}</span> : null}
      </span>
      {children}
    </div>
  );
}

export function FormTextarea(props: React.TextareaHTMLAttributes<HTMLTextAreaElement>) {
  return (
    <textarea
      {...props}
      className={`min-h-24 w-full rounded-lg border border-border/80 bg-muted/50 px-3 py-2 text-sm text-foreground placeholder:text-muted-foreground outline-none transition focus:border-primary/70 focus:ring-2 focus:ring-primary/20 ${props.className || ""}`}
    />
  );
}

export function FormSelect(props: React.SelectHTMLAttributes<HTMLSelectElement>) {
  return (
    <select
      {...props}
      className={`h-10 w-full rounded-lg border border-border/80 bg-muted/50 px-3 text-sm text-foreground outline-none transition focus:border-primary/70 focus:ring-2 focus:ring-primary/20 ${props.className || ""}`}
    />
  );
}
