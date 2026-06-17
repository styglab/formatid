import type { NextConfig } from "next";

const basePath = process.env.NEXT_PUBLIC_BASE_PATH || "/data-score-dashboard";

const nextConfig: NextConfig = {
  basePath,
  output: "standalone",
  reactStrictMode: true
};

export default nextConfig;
