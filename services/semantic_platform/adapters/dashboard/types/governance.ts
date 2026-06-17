export type Proposal = {
  id: string;
  title: string;
  entity_type: string;
  entity_id?: string;
  change_type: string;
  status: string;
  payload?: Record<string, unknown>;
  created_at?: string | null;
  reviewed_at?: string | null;
  reviewed_by?: string | null;
};

export type Overview = {
  counts: {
    semantic_types: number;
    approved_semantic_types: number;
    draft_semantic_types: number;
    execution_sources?: number;
    pending_proposals: number;
    relationships: number;
  };
  recent_proposals: Proposal[];
};
