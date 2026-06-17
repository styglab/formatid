"use client";

import { useEffect, useState } from "react";
import {
  listCanonicalAttributes,
  listCanonicalEntities,
  listCanonicalRelations,
} from "@/api/semantic-admin";
import type { CanonicalAttribute, CanonicalEntity, CanonicalRelation } from "@/types/semantic";

export function useCanonicalModel() {
  const [entities, setEntities] = useState<CanonicalEntity[]>([]);
  const [attributes, setAttributes] = useState<CanonicalAttribute[]>([]);
  const [relations, setRelations] = useState<CanonicalRelation[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function reload() {
    setLoading(true);
    setError("");
    try {
      const [nextEntities, nextAttributes, nextRelations] = await Promise.all([
        listCanonicalEntities(),
        listCanonicalAttributes(),
        listCanonicalRelations(),
      ]);
      setEntities(nextEntities);
      setAttributes(nextAttributes);
      setRelations(nextRelations);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load canonical model.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void reload();
  }, []);

  return { entities, attributes, relations, loading, error, reload };
}
