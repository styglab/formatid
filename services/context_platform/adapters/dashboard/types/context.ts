export type PaginatedResult<T> = {
  items: T[];
  total: number;
  page: number;
  page_size: number;
};

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

export type ContextOverview = {
  counts: Record<string, number>;
  recent_proposals?: Proposal[];
};

export type WorkbenchStepState = "blocked" | "ready" | "running" | "complete" | "warning";

export type WorkbenchWorkflowStep = {
  key: string;
  number: number;
  title: string;
  state: WorkbenchStepState;
  detail: string;
  depends_on?: string[];
};

export type WorkbenchWorkflow = {
  mode: "executable" | "knowledge_only" | string;
  requires_operation: boolean;
  execution_ready: boolean;
  active_document?: ContextSourceDocument | null;
  active_run?: ContextOnboardingRun | null;
  active_bundle?: ContextProposalBundle | null;
  counts: Record<string, number>;
  steps: WorkbenchWorkflowStep[];
};

export type WorkbenchActionResult = {
  action: string;
  status: string;
  reason?: string;
  applied_count?: number;
  skipped_count?: number;
  rejected_count?: number;
  rationale?: string;
  submission?: Record<string, unknown>;
  validation?: Record<string, unknown>;
  proposal_bundle?: ContextProposalBundle | null;
  workflow?: WorkbenchWorkflow;
  agent_response_received?: boolean;
  manual_llm_response_received?: boolean;
};

export type ContextSource = {
  id: string;
  namespace?: string;
  name: string;
  provider?: string;
  source_type?: string;
  description?: string;
  lifecycle?: string;
  status?: string;
  created_at?: string | null;
  updated_at?: string | null;
};

export type ContextSourceOperation = {
  id: string;
  source_id: string;
  source_document_id?: string | null;
  operation_key: string;
  method: string;
  path: string;
  name: string;
  description?: string;
  lifecycle?: string;
  status?: string;
  request_spec?: Record<string, unknown>;
  response_spec?: Record<string, unknown>;
  created_at?: string | null;
};

export type ContextSourceDocument = {
  id: string;
  source_id: string;
  document_type: string;
  name: string;
  uri?: string;
  content_hash?: string;
  content_type?: string;
  status?: string;
  metadata?: Record<string, unknown>;
  created_at?: string | null;
};

export type ContextSourceParameter = {
  id: string;
  source_operation_id: string;
  name: string;
  raw_name?: string;
  location: string;
  data_type?: string;
  is_required?: boolean;
  status?: string;
};

export type ContextSourceField = {
  id: string;
  source_id?: string | null;
  source_document_id?: string | null;
  source_operation_id?: string | null;
  direction: "input" | "output";
  field_path: string;
  raw_name?: string;
  data_type?: string;
  is_required?: boolean;
  status?: string;
};

export type MeaningScope = {
  id: string;
  stable_key: string;
  name: string;
  description?: string;
  status?: string;
};

export type ConceptScheme = {
  id: string;
  stable_key: string;
  meaning_scope_id?: string | null;
  name: string;
  description?: string;
  status?: string;
};

export type Concept = {
  id: string;
  stable_key: string;
  meaning_scope_id: string;
  scheme_id?: string | null;
  kind: string;
  code?: string | null;
  label_ko?: string | null;
  label_en?: string | null;
  definition?: string | null;
  status?: string;
};

export type ValueDomain = {
  id: string;
  stable_key: string;
  meaning_scope_id?: string | null;
  name: string;
  description?: string;
  status?: string;
};

export type ValueDomainValue = {
  id: string;
  value_domain_id: string;
  code: string;
  concept_id?: string | null;
  label_ko?: string | null;
  label_en?: string | null;
  description?: string;
  status?: string;
};

export type ObjectType = {
  id: string;
  stable_key: string;
  name: string;
  description?: string;
  concept_id?: string | null;
  linkml_class?: string;
  status?: string;
  metadata?: Record<string, unknown>;
};

export type PropertyType = {
  id: string;
  stable_key: string;
  name: string;
  description?: string;
  broad_datatype?: string;
  linkml_slot?: string;
  status?: string;
  metadata?: Record<string, unknown>;
};

export type LinkType = {
  id: string;
  stable_key: string;
  name: string;
  source_object_type_id?: string | null;
  target_object_type_id?: string | null;
  description?: string;
  status?: string;
  metadata?: Record<string, unknown>;
};

export type CanonicalRepresentation = {
  id: string;
  stable_key: string;
  concept_id: string;
  carrier_object_type_id: string;
  value_property_type_id?: string | null;
  fixed_context_json?: Record<string, unknown>;
  required_context_json?: string[] | unknown[];
  representation_kind: string;
  priority?: number;
  is_preferred?: boolean;
  status?: string;
  metadata?: Record<string, unknown>;
};

export type RepresentationSchema = {
  id: string;
  stable_key: string;
  representation_id: string;
  datatype: string;
  value_domain_id?: string | null;
  pattern?: string | null;
  cardinality?: string | null;
  required?: boolean | null;
  examples_json?: unknown[];
  validation_json?: Record<string, unknown>;
  status?: string;
};

export type CanonicalType = {
  id: string;
  namespace?: string;
  name: string;
  description?: string;
  base_type?: string;
  typeof?: string;
  status?: string;
};

export type CanonicalEnum = {
  id: string;
  namespace?: string;
  name: string;
  description?: string;
  permissible_values?: Record<string, unknown>;
  status?: string;
};

export type CanonicalSlot = {
  id: string;
  namespace?: string;
  name: string;
  description?: string;
  range_kind?: string;
  range_ref?: string;
  datatype?: string;
  aliases?: string[];
  status?: string;
};

export type CanonicalClass = {
  id: string;
  namespace?: string;
  name: string;
  description?: string;
  metadata?: Record<string, unknown>;
  lifecycle?: string;
  status?: string;
};

export type CanonicalClassSlotUsage = {
  id: string;
  class_id: string;
  canonical_slot_id?: string | null;
  class_name?: string;
  slot_name?: string;
  namespace?: string;
  name: string;
  description?: string;
  datatype?: string;
  identity_role?: string;
  annotations?: Record<string, unknown>;
  constraints?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  lifecycle?: string;
  status?: string;
};

export type CanonicalClassSlot = {
  id: string;
  class_id: string;
  slot_id: string;
  usage_name?: string;
  required?: boolean;
  multivalued?: boolean;
  slot_order?: number;
  status?: string;
};

export type CanonicalRelation = {
  id: string;
  source_class_id: string;
  target_class_id: string;
  source_class_name?: string;
  target_class_name?: string;
  relation_type: string;
  forward_label?: string;
  reverse_label?: string;
  metadata?: Record<string, unknown>;
  lifecycle?: string;
  status?: string;
  confidence?: number | null;
};

export type ContextBinding = {
  id: string;
  source_id: string;
  source_document_id?: string | null;
  source_operation_id?: string | null;
  source_parameter_id?: string | null;
  source_field_id?: string | null;
  canonical_class_slot_id?: string;
  representation_id?: string | null;
  representation_schema_id?: string | null;
  fills_property_type_id?: string | null;
  required_concept_id?: string | null;
  context_key?: string | null;
  direction: "input" | "output" | "output_context";
  binding_type?: string;
  review_status?: string;
  status?: string;
  confidence?: number | null;
};

export type ContextOnboardingRun = {
  id: string;
  source_id: string;
  source_document_id?: string | null;
  status: string;
  stage: string;
  metadata?: Record<string, unknown>;
  started_at?: string | null;
  completed_at?: string | null;
  updated_at?: string | null;
};

export type ContextProposalBundle = {
  id: string;
  run_id: string;
  source_id: string;
  evidence_snapshot_id?: string | null;
  title: string;
  status: string;
  summary?: Record<string, unknown>;
  proposal_count?: number;
  created_at?: string | null;
  updated_at?: string | null;
};

export type Capability = {
  id: string;
  capability_key: string;
  namespace?: string;
  name: string;
  description?: string;
  intent_spec?: Record<string, unknown>;
  lifecycle?: string;
  status?: string;
};

export type ContextCapabilityStep = {
  id: string;
  capability_id: string;
  source_operation_id?: string | null;
  step_order?: number;
  step_kind?: string;
  binding_spec?: Record<string, unknown>;
  status?: string;
  priority?: number;
};

export type ContextCapabilityOperation = ContextCapabilityStep;

export type ContextPlan = {
  id: string;
  selected_capability_id?: string | null;
  selected_source_operation_id?: string | null;
  status: string;
  confidence?: number | null;
  requires_confirmation?: boolean;
  validation_result?: Record<string, unknown>;
  created_at?: string | null;
};

export type ContextExecution = {
  id: string;
  plan_id: string;
  status: string;
  request_payload?: Record<string, unknown>;
  result_payload?: Record<string, unknown>;
  started_at?: string | null;
  completed_at?: string | null;
  created_at?: string | null;
};
