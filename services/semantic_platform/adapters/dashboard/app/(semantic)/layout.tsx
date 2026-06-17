import { SemanticLayout } from "@/components/layout/semantic-layout";

export default function SemanticRouteLayout({ children }: { children: React.ReactNode }) {
  return <SemanticLayout>{children}</SemanticLayout>;
}
