"use client";

import Link from "next/link";
import { ChevronRight } from "lucide-react";
import { contextBreadcrumbs } from "@/lib/context/navigation";

export function ContextBreadcrumbs({ pathname }: { pathname: string }) {
  const items = contextBreadcrumbs(pathname);

  return (
    <nav aria-label="Breadcrumb" className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
      {items.map((item, index) => (
        <div key={`${item.label}-${index}`} className="flex items-center gap-2">
          {index > 0 ? <ChevronRight className="h-3.5 w-3.5" /> : null}
          {item.href ? (
            <Link href={item.href} className="transition hover:text-foreground">
              {item.label}
            </Link>
          ) : (
            <span className="text-foreground">{item.label}</span>
          )}
        </div>
      ))}
    </nav>
  );
}
