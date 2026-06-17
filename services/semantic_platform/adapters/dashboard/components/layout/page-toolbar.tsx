"use client";

import { usePathname } from "next/navigation";
import { SemanticBreadcrumbs } from "@/components/layout/semantic-breadcrumbs";

type PageToolbarProps = {
  title: string;
  description?: string;
  actions?: React.ReactNode;
};

export function PageToolbar({ title, actions }: PageToolbarProps) {
  const pathname = usePathname();

  return (
    <div className="space-y-2">
      <SemanticBreadcrumbs pathname={pathname} />
      <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <div className="min-w-0">
          <h2 className="text-2xl font-semibold tracking-tight text-foreground">{title}</h2>
        </div>
        {actions ? <div className="shrink-0">{actions}</div> : null}
      </div>
    </div>
  );
}
