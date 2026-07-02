"use client";

import { useEffect, type ReactNode } from "react";
import { X } from "lucide-react";
import { Button } from "@/components/ui/button";

type DetailDrawerProps = {
  open: boolean;
  title: string;
  subtitle?: string;
  onClose: () => void;
  actions?: ReactNode;
  children: ReactNode;
};

export function DetailDrawer({ open, title, subtitle, onClose, actions, children }: DetailDrawerProps) {
  useEffect(() => {
    if (!open) {
      return;
    }

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [open, onClose]);

  if (!open) {
    return null;
  }

  return (
    <div className="fixed inset-0 z-[60]">
      <button
        type="button"
        aria-label="Close detail drawer"
        className="absolute inset-0 bg-transparent"
        onClick={onClose}
      />
      <section
        role="dialog"
        aria-modal="true"
        aria-label={title}
        className="fixed right-0 top-0 bottom-0 flex w-full max-w-[720px] flex-col border-l border-border/80 bg-background"
      >
        <header className="flex items-start justify-between gap-4 border-b border-border/70 px-4 py-2.5">
          <div className="min-w-0">
            <h3 className="truncate text-base font-semibold text-foreground">{title}</h3>
            {subtitle ? <p className="mt-0.5 text-sm text-muted-foreground">{subtitle}</p> : null}
          </div>
          <div className="flex shrink-0 items-center gap-2">
            {actions}
            <Button type="button" variant="ghost" size="icon" onClick={onClose} aria-label="Close detail">
              <X className="h-4 w-4" />
            </Button>
          </div>
        </header>
        <div className="min-h-0 flex-1 overflow-y-auto px-4 py-2.5">
          {children}
        </div>
      </section>
    </div>
  );
}
