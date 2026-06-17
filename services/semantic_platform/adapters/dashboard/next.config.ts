import type { NextConfig } from "next";

const basePath = process.env.NEXT_PUBLIC_BASE_PATH || "/semantic";

const nextConfig: NextConfig = {
  basePath,
  output: "standalone",
  reactStrictMode: true,
  async redirects() {
    return [
      { source: "/discovery/sources", destination: "/sources", permanent: true },
      { source: "/discovery/onboarding-runs", destination: "/onboarding-runs", permanent: true },
      { source: "/discovery/onboarding-runs/:runId", destination: "/onboarding-runs/:runId", permanent: true },
      { source: "/discovery/source-operations", destination: "/source-operations", permanent: true },
      { source: "/discovery/schemas", destination: "/schemas", permanent: true },
      { source: "/semantic-platform/semantic-types", destination: "/semantic-types", permanent: true },
      { source: "/semantic-platform/canonical-model", destination: "/canonical-model", permanent: true },
      { source: "/semantic-platform/mappings", destination: "/mappings", permanent: true },
      { source: "/semantic-platform/lineage", destination: "/lineage", permanent: true },
      { source: "/agent/capabilities", destination: "/capabilities", permanent: true },
      { source: "/agent/operation-catalog", destination: "/operations", permanent: true },
      { source: "/agent/variants", destination: "/variants", permanent: true },
      { source: "/agent/capability-bindings", destination: "/bindings", permanent: true },
      { source: "/governance/lineage", destination: "/lineage", permanent: true },
      { source: "/governance/proposal-bundles", destination: "/proposal-bundles", permanent: true },
      { source: "/governance/proposals", destination: "/proposals", permanent: true },
      { source: "/governance/reviews", destination: "/reviews", permanent: true },
      { source: "/governance/audit", destination: "/audit", permanent: true },
      { source: "/release/publish", destination: "/publish", permanent: true },
    ];
  }
};

export default nextConfig;
