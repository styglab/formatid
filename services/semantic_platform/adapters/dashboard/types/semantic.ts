import type { Proposal } from "@/types/governance";

export type SemanticType = {
  id: string;
  name: string;
  description?: string;
  namespace?: string;
  datatype?: string;
  entity_kind?: string;
  parent_entity_id?: string;
  parent_entity_name?: string;
  semantic_role?: string;
  aliases?: string[];
  owners?: string[];
  tags?: string[];
  status?: string;
  created_at?: string | null;
  updated_at?: string | null;
  draft_change_type?: string;
  draft_snapshot?: SemanticType | null;
  approved_snapshot?: SemanticType | null;
  pending_proposal_id?: string;
};

export type SemanticRelationship = {
  id: string;
  source_id: string;
  source_name: string;
  target_id: string;
  target_name: string;
  relation_type: string;
  status?: string;
  created_at?: string | null;
  updated_at?: string | null;
  draft_change_type?: string;
  draft_snapshot?: SemanticRelationship | null;
  approved_snapshot?: SemanticRelationship | null;
  pending_proposal_id?: string;
};

export type ExecutionSource = {
  id: string;
  name: string;
  provider?: string;
  source_type: string;
  description?: string;
  status?: string;
  config?: Record<string, unknown>;
  created_at?: string | null;
  updated_at?: string | null;
  draft_change_type?: string;
  draft_snapshot?: ExecutionSource | null;
  approved_snapshot?: ExecutionSource | null;
  pending_proposal_id?: string;
  asset_count?: number;
  operation_count?: number;
  field_count?: number;
  latest_run_id?: string;
  latest_run_stage?: string;
  latest_run_status?: string;
  pending_proposal_count?: number;
};

export type ExecutionAsset = {
  id: string;
  source_id: string;
  source_name?: string;
  source_type?: string;
  name: string;
  asset_type: string;
  locator: string;
  description?: string;
  version?: string;
  lifecycle?: string;
  status?: string;
  metadata?: Record<string, unknown>;
  created_at?: string | null;
  updated_at?: string | null;
};

export type ExecutionOperation = {
  id: string;
  access_path_id: string;
  asset_id?: string | null;
  source_id?: string | null;
  operation_key: string;
  namespace?: string;
  name: string;
  description?: string;
  version?: string;
  lifecycle?: string;
  status?: string;
  input_spec?: unknown[];
  output_spec?: unknown[];
  auth_spec?: Record<string, unknown>;
  contract_spec?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  source_name?: string;
  source_type?: string;
  asset_name?: string;
  asset_type?: string;
  access_path_name?: string;
  access_type?: string;
  access_path_locator?: string;
  http_method?: string;
  created_at?: string | null;
  updated_at?: string | null;
};

export type OperationField = {
  id: string;
  operation_id: string;
  variant_id?: string | null;
  scope: string;
  raw_name: string;
  display_name?: string;
  field_path?: string;
  data_type?: string;
  is_required?: boolean;
  description?: string;
  version?: string;
  lifecycle?: string;
  metadata?: Record<string, unknown>;
  created_at?: string | null;
  updated_at?: string | null;
};

export type OperationVariant = {
  id: string;
  operation_id: string;
  variant_key: string;
  name: string;
  description?: string;
  version?: string;
  lifecycle?: string;
  status?: string;
  fixed_semantic_arguments?: Record<string, unknown>;
  fixed_raw_arguments?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  created_at?: string | null;
  updated_at?: string | null;
  draft_change_type?: string;
  draft_snapshot?: OperationVariant | null;
  approved_snapshot?: OperationVariant | null;
  pending_proposal_id?: string;
};

export type CanonicalEntity = {
  id: string;
  semantic_type_id?: string | null;
  name: string;
  namespace?: string;
  description?: string;
  version?: string;
  lifecycle?: string;
  status?: string;
  metadata?: Record<string, unknown>;
  created_at?: string | null;
  updated_at?: string | null;
  draft_change_type?: string;
  draft_snapshot?: CanonicalEntity | null;
  approved_snapshot?: CanonicalEntity | null;
  pending_proposal_id?: string;
};

export type CanonicalAttribute = {
  id: string;
  entity_id: string;
  entity_name?: string;
  semantic_type_id?: string | null;
  name: string;
  namespace?: string;
  description?: string;
  datatype?: string;
  identity_role?: string;
  version?: string;
  lifecycle?: string;
  status?: string;
  metadata?: Record<string, unknown>;
  created_at?: string | null;
  updated_at?: string | null;
  draft_change_type?: string;
  draft_snapshot?: CanonicalAttribute | null;
  approved_snapshot?: CanonicalAttribute | null;
  pending_proposal_id?: string;
};

export type CanonicalRelation = {
  id: string;
  source_entity_id: string;
  target_entity_id: string;
  source_entity_name?: string;
  target_entity_name?: string;
  relation_type: string;
  forward_label?: string;
  reverse_label?: string;
  version?: string;
  lifecycle?: string;
  status?: string;
  metadata?: Record<string, unknown>;
  created_at?: string | null;
  updated_at?: string | null;
  draft_change_type?: string;
  draft_snapshot?: CanonicalRelation | null;
  approved_snapshot?: CanonicalRelation | null;
  pending_proposal_id?: string;
};

export type Capability = {
  id: string;
  capability_key: string;
  namespace?: string;
  name: string;
  description?: string;
  version?: string;
  lifecycle?: string;
  status?: string;
  input_semantic_types?: string[];
  output_semantic_types?: string[];
  intent_spec?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  evidence?: unknown[];
  confidence?: number | null;
  created_at?: string | null;
  updated_at?: string | null;
  draft_change_type?: string;
  draft_snapshot?: Capability | null;
  approved_snapshot?: Capability | null;
  pending_proposal_id?: string;
};

export type FieldMapping = {
  id: string;
  field_id?: string | null;
  source_id?: string | null;
  operation_id: string;
  variant_id?: string | null;
  access_path_id?: string | null;
  field_path: string;
  semantic_type_id: string;
  canonical_attribute_id?: string | null;
  mapping_kind?: string;
  mapping_type?: string;
  version?: string;
  lifecycle?: string;
  status?: string;
  namespace?: string;
  transform_spec?: Record<string, unknown>;
  enum_mapping?: Record<string, unknown>;
  notes?: string;
  evidence?: unknown[];
  confidence?: number | null;
  created_at?: string | null;
  updated_at?: string | null;
  draft_change_type?: string;
  draft_snapshot?: FieldMapping | null;
  approved_snapshot?: FieldMapping | null;
  pending_proposal_id?: string;
};

export type OnboardingRun = {
  id: string;
  source_id: string;
  source_name: string;
  status: string;
  stage?: string;
  current_stage?: string;
  stage_status?: string;
  run_mode?: string;
  next_action?: string;
  trigger_type?: string;
  evidence_snapshot_id: string;
  operation_count: number;
  field_count: number;
  mapping_count: number;
  proposal_count: number;
  pending_proposal_count: number;
  suggestion_status: string;
  preparation_status?: string;
  worker_progress_percent?: number | null;
  task_count?: number;
  completed_task_count?: number;
  draft_ready_count?: number;
  draft_failed_count?: number;
  draft_active_count?: number;
  draft_queued_count?: number;
  current_stage_task_count?: number;
  current_stage_ready_count?: number;
  current_stage_failed_count?: number;
  current_stage_completed_count?: number;
  worker_current_task?: string;
  created_at?: string | null;
  updated_at?: string | null;
};

export type EvidenceSnapshot = {
  id: string;
  run_id: string;
  source_id: string;
  snapshot_type: string;
  content_hash?: string;
  source_ref?: Record<string, unknown>;
  operation_evidence?: unknown[];
  schema_evidence?: unknown[];
  sample_values?: Record<string, unknown>;
  ai_context?: Record<string, unknown>;
  created_at?: string | null;
};

export type ProposalBundle = {
  id: string;
  run_id: string;
  source_id: string;
  source_name: string;
  status: string;
  proposal_count: number;
  pending_count: number;
  approved_count: number;
  rejected_count: number;
  entity_counts: Record<string, number>;
  evidence_snapshot_id?: string;
  proposal_ids: string[];
  updated_at?: string | null;
};

export type WorkQueueTask = {
  id: string;
  run_id: string;
  source_id: string;
  evidence_snapshot_id?: string;
  operation_id?: string | null;
  operation_name?: string;
  field_id?: string | null;
  field_name?: string;
  field_path?: string;
  stage?: string;
  task_type: string;
  status: string;
  supports_ai_draft?: boolean;
  draft_status?: string;
  depends_on?: string[];
  recommended_action?: string;
  draft_payload?: Record<string, unknown>;
  draft_rationale?: string;
  draft_confidence?: number | null;
  priority: number;
  title: string;
  payload?: Record<string, unknown>;
  proposal_id?: string | null;
  assigned_to?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type OnboardingRunDetail = {
  run: OnboardingRun;
  source: ExecutionSource;
  evidence_snapshots: EvidenceSnapshot[];
  operations: ExecutionOperation[];
  fields: OperationField[];
  mappings: FieldMapping[];
  work_queue_tasks: WorkQueueTask[];
  proposal_bundle: ProposalBundle;
  proposals: Proposal[];
};

export type CapabilityBinding = {
  id: string;
  capability_id: string;
  capability_key: string;
  capability_name: string;
  operation_id: string;
  operation_name: string;
  variant_ids: string[];
  variant_count: number;
  semantic_coverage: number;
  status: string;
  evidence: string;
};

export type PaginatedResult<T> = {
  items: T[];
  total: number;
  page: number;
  page_size: number;
};
