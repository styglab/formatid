"use client";

import { useEffect, useState } from "react";
import { listOverview, listPendingProposalsPage, listSemanticRelationships, listSemanticTypes } from "@/api/semantic-admin";
import type { Overview, Proposal } from "@/types/governance";
import type { PaginatedResult, SemanticRelationship, SemanticType } from "@/types/semantic";

export function usePendingProposals(params: { query?: string; entityType?: string; ids?: string[]; page?: number; pageSize?: number } = {}) {
  const [data, setData] = useState<PaginatedResult<Proposal>>({ items: [], total: 0, page: 1, page_size: params.pageSize || 20 });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      setData(await listPendingProposalsPage(params));
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load proposals.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, [params.entityType, params.ids?.join(","), params.page, params.pageSize, params.query]);

  return { data, loading, error, reload };
}

export function useSemanticOverview() {
  const [data, setData] = useState<Overview | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      setData(await listOverview());
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load overview.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, []);

  return { data, loading, error, reload };
}

export function useSemanticRegistry() {
  const [semanticTypes, setSemanticTypes] = useState<SemanticType[]>([]);
  const [relationships, setRelationships] = useState<SemanticRelationship[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      const [types, links] = await Promise.all([listSemanticTypes(), listSemanticRelationships()]);
      setSemanticTypes(types);
      setRelationships(links);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load semantic registry.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, []);

  return { semanticTypes, relationships, loading, error, reload };
}
