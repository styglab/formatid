"use client";

import { useState } from "react";
import { ContextSidebar } from "@/components/layout/context-sidebar";
import { ContextTopbar } from "@/components/layout/context-topbar";

export function ContextLayout({ children }: { children: React.ReactNode }) {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  return (
    <div className="flex h-screen overflow-hidden bg-background">
      <ContextSidebar collapsed={sidebarCollapsed} onToggle={() => setSidebarCollapsed((current) => !current)} />
      <div className="flex min-h-0 min-w-0 flex-1 flex-col">
        <ContextTopbar />
        <main className="min-h-0 flex-1 overflow-y-auto overflow-x-hidden p-5 md:p-6">{children}</main>
      </div>
    </div>
  );
}
