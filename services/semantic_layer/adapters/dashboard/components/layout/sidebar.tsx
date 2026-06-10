import {
  Blocks,
  BookMarked,
  Database,
  FileCheck2,
  GitBranch,
  PanelLeftClose,
  PanelLeftOpen,
  type LucideIcon
} from "lucide-react";

type SidebarProps = {
  activeItem: string;
  collapsed: boolean;
  onToggle: () => void;
  onSelect: (itemId: string) => void;
};

const items: Array<{
  id: string;
  label: string;
  note: string;
  icon: LucideIcon;
  enabled: boolean;
}> = [
  {
    id: "semantic-model",
    label: "Semantic Model",
    note: "types and relationships",
    icon: Database,
    enabled: true
  },
  {
    id: "capability-catalog",
    label: "Capability Catalog",
    note: "provider-neutral capabilities",
    icon: Blocks,
    enabled: false
  },
  {
    id: "execution-contracts",
    label: "Execution Contracts",
    note: "operations and variants",
    icon: GitBranch,
    enabled: true
  },
  {
    id: "governance",
    label: "Reviews",
    note: "proposals and approvals",
    icon: FileCheck2,
    enabled: true
  },
  {
    id: "ingestion",
    label: "Source Ingestion",
    note: "document evidence and runs",
    icon: BookMarked,
    enabled: false
  }
];

export function Sidebar({ activeItem, collapsed, onToggle, onSelect }: SidebarProps) {
  const activeItems = items.filter((item) => item.enabled);
  const upcomingItems = items.filter((item) => !item.enabled);

  return (
    <aside
      className={`hidden h-screen shrink-0 border-r border-border bg-card/90 px-3 py-4 backdrop-blur transition-[width] duration-200 md:block ${
        collapsed ? "w-20" : "w-60"
      }`}
    >
      <div className={`mb-6 flex px-2 ${collapsed ? "justify-center" : "items-center justify-between"}`}>
        {!collapsed ? <div className="text-sm font-semibold">Semantic Layer</div> : null}
        <button
          type="button"
          onClick={onToggle}
          className="rounded-lg border border-transparent bg-transparent p-2 text-muted-foreground transition hover:bg-transparent hover:text-foreground"
          aria-label={collapsed ? "Open sidebar" : "Collapse sidebar"}
        >
          {collapsed ? <PanelLeftOpen className="h-4 w-4" /> : <PanelLeftClose className="h-4 w-4" />}
        </button>
      </div>

      {!collapsed ? <div className="mb-3 px-3 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Active</div> : null}
      <nav className="space-y-1.5">
        {activeItems.map((item) => {
          const Icon = item.icon;
          return (
            <button
              key={item.id}
              type="button"
              onClick={() => onSelect(item.id)}
              className={`w-full rounded-xl px-3 py-3 text-left transition ${
                activeItem === item.id
                  ? "border border-primary/20 bg-primary/10 text-foreground shadow-sm"
                  : item.enabled
                    ? "border border-transparent bg-transparent text-muted-foreground"
                    : "border border-transparent bg-transparent text-muted-foreground/70"
              }`}
            >
              <div className={`flex w-full gap-3 ${collapsed ? "items-center justify-center" : "items-start justify-start text-left"}`}>
                <Icon className="mt-0.5 h-4 w-4 shrink-0" />
                {!collapsed ? (
                  <div className="min-w-0 text-left">
                    <div className="text-sm font-medium">{item.label}</div>
                    <div className="mt-0.5 text-xs text-muted-foreground">{item.note}</div>
                  </div>
                ) : null}
              </div>
            </button>
          );
        })}
      </nav>

      {!collapsed ? <div className="mt-6 mb-3 px-3 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Coming Next</div> : null}
      <nav className="space-y-1.5">
        {upcomingItems.map((item) => {
          const Icon = item.icon;
          return (
            <div
              key={item.id}
              className="w-full rounded-xl border border-transparent bg-transparent px-3 py-3 text-left text-muted-foreground/70 transition"
            >
              <div className={`flex w-full gap-3 ${collapsed ? "items-center justify-center" : "items-start justify-start text-left"}`}>
                <Icon className="mt-0.5 h-4 w-4 shrink-0" />
                {!collapsed ? (
                  <div className="min-w-0 text-left">
                    <div className="text-sm font-medium">{item.label}</div>
                    <div className="mt-0.5 text-xs text-muted-foreground">{item.note}</div>
                  </div>
                ) : null}
              </div>
            </div>
          );
        })}
      </nav>
    </aside>
  );
}
