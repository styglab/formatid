"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { PanelLeftClose, PanelLeftOpen } from "lucide-react";
import { semanticNavSections } from "@/lib/semantic/navigation";

type SemanticSidebarProps = {
  collapsed: boolean;
  onToggle: () => void;
};

export function SemanticSidebar({ collapsed, onToggle }: SemanticSidebarProps) {
  const pathname = usePathname();

  return (
    <aside
      className={`sidebar-scroll hidden h-screen shrink-0 overflow-y-auto border-r border-border bg-card/90 px-2.5 py-3 backdrop-blur transition-[width] duration-200 md:block ${
        collapsed ? "w-20" : "w-72"
      }`}
    >
      <div className={`mb-4 flex px-2 ${collapsed ? "justify-center" : "items-center justify-between"}`}>
        {!collapsed ? (
          <div>
            <div className="text-sm font-semibold">Semantic Platform</div>
            <div className="mt-0.5 text-xs text-muted-foreground">control plane</div>
          </div>
        ) : null}
        <button
          type="button"
          onClick={onToggle}
          className="rounded-lg border border-transparent bg-transparent p-2 text-muted-foreground transition hover:text-foreground"
          aria-label={collapsed ? "Open sidebar" : "Collapse sidebar"}
        >
          {collapsed ? <PanelLeftOpen className="h-4 w-4" /> : <PanelLeftClose className="h-4 w-4" />}
        </button>
      </div>

      <div className="space-y-4">
        {semanticNavSections.map((section) => (
          <section key={section.title}>
            {!collapsed ? (
              <div className="mb-2 px-3 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                {section.title}
              </div>
            ) : null}
            <nav className="space-y-1">
              {section.items.map((item) => {
                const active = pathname === item.href || pathname.startsWith(`${item.href}/`);
                const Icon = item.icon;
                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    className={`block rounded-lg border px-3 py-2.5 transition-colors ${
                      active
                        ? "border-primary/15 bg-primary/[0.08] text-foreground"
                        : "border-transparent text-muted-foreground hover:bg-muted/10 hover:text-foreground"
                    }`}
                  >
                    {collapsed ? (
                      <div className="flex justify-center">
                        <Icon className="h-4 w-4" />
                      </div>
                    ) : (
                      <div className="flex items-center gap-2.5">
                        <Icon className="h-4 w-4 shrink-0" />
                        <div className="text-sm font-medium">{item.label}</div>
                      </div>
                    )}
                  </Link>
                );
              })}
            </nav>
          </section>
        ))}
      </div>
    </aside>
  );
}
