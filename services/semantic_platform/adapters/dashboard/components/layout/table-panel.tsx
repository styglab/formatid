import type { ReactNode } from "react";

export function TablePanel({
  children,
  footer
}: {
  children: ReactNode;
  footer?: ReactNode;
}) {
  return (
    <section className="flex h-[41rem] flex-col">
      <div className="min-h-0 flex-1 overflow-auto">{children}</div>
      {footer ? <div className="mt-auto border-t border-border/70 px-1 py-2">{footer}</div> : null}
    </section>
  );
}
