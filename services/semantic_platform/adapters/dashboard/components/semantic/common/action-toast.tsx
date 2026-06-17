"use client";

import { CheckCircle2, X } from "lucide-react";
import { Button } from "@/components/ui/button";

type ActionToastProps = {
  open: boolean;
  message: string;
  onClose: () => void;
  actionLabel?: string;
  onAction?: () => void;
};

export function ActionToast({ open, message, onClose, actionLabel, onAction }: ActionToastProps) {
  if (!open || !message) {
    return null;
  }

  return (
    <div className="pointer-events-none fixed bottom-5 right-5 z-[80]">
      <div className="pointer-events-auto flex min-w-[320px] max-w-[420px] items-start gap-3 rounded-2xl border border-emerald-500/20 bg-background px-4 py-3 shadow-[0_20px_50px_rgba(15,23,42,0.12)]">
        <div className="mt-0.5 inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-emerald-500/12 text-emerald-700">
          <CheckCircle2 className="h-4 w-4" />
        </div>
        <div className="min-w-0 flex-1">
          <div className="text-sm font-semibold text-foreground">Updated</div>
          <div className="mt-0.5 text-sm text-muted-foreground">{message}</div>
          {actionLabel && onAction ? (
            <button
              type="button"
              className="mt-2 text-xs font-medium text-emerald-700 underline-offset-2 hover:underline"
              onClick={onAction}
            >
              {actionLabel}
            </button>
          ) : null}
        </div>
        <Button type="button" variant="ghost" size="sm" className="h-8 px-2 text-xs" onClick={onClose}>
          <X className="h-3.5 w-3.5" />
        </Button>
      </div>
    </div>
  );
}
