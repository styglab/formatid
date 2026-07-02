import type { ReactNode } from "react";
import { PageToolbar } from "@/components/layout/page-toolbar";

type SectionPlaceholderProps = {
  title: string;
  description?: string;
  body: ReactNode;
  actions?: ReactNode;
};

export function SectionPlaceholder({ title, description, body, actions }: SectionPlaceholderProps) {
  return (
    <div className="space-y-5">
      <PageToolbar title={title} description={description} actions={actions} />
      <section className="space-y-3">{body}</section>
    </div>
  );
}
