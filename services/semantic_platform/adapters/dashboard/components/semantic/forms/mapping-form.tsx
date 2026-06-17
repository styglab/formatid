"use client";

import type { FormEvent } from "react";
import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { Plus, RefreshCw } from "lucide-react";
import type { SemanticTypeSuggestion, TransformSuggestion } from "@/api/semantic-admin";
import type {
  CanonicalAttribute,
  CanonicalEntity,
  ExecutionOperation,
  ExecutionSource,
  OperationField,
  SemanticType,
} from "@/types/semantic";
import { Button } from "@/components/ui/button";
import { Combobox } from "@/components/ui/combobox";
import { FormField, FormGrid, FormSelect, FormShell, FormTextarea } from "@/components/semantic/forms/form-shell";
import { Input } from "@/components/ui/input";

const RECENT_SEMANTIC_TYPE_STORAGE_KEY = "semantic-platform:mappings:recent-semantic-types";

export type MappingFormState = {
  operationFieldId?: string;
  sourceId: string;
  operationId: string;
  fieldPath: string;
  semanticTypeId: string;
  canonicalAttributeId: string;
  mappingType: string;
  mappingKind: string;
  namespace: string;
  lifecycle: string;
  version: string;
  confidence: string;
  notes: string;
  transformSpec: string;
  enumMapping: string;
};


function fieldDisplayLabel(field: { display_name?: string; raw_name?: string; field_path?: string }) {
  const preferred = (field.display_name || "").trim();
  if (preferred) return preferred;
  const raw = String(field.raw_name || "").trim();
  if (raw) return raw;
  const path = String(field.field_path || "").trim();
  return path || "-";
}

function transformSummary(spec: Record<string, unknown>) {
  const kind = String(spec.kind || "identity");
  if (kind === "date_parse") {
    return `Date parse · ${String(spec.input_format || "auto")} -> ${String(spec.output_format || "ISO_DATE")}`;
  }
  if (kind === "number_parse") {
    return "Number parse";
  }
  if (kind === "enum_map") {
    return "Enum value mapping";
  }
  if (kind === "identity") {
    return "No transform needed";
  }
  return kind.replaceAll("_", " ");
}

function transformDetailItems(spec: Record<string, unknown>) {
  const kind = String(spec.kind || "identity");
  if (kind === "date_parse") {
    return [
      { label: "Type", value: "Date parse" },
      { label: "Input format", value: String(spec.input_format || "auto") },
      { label: "Output", value: String(spec.output_format || "ISO_DATE") },
    ];
  }
  if (kind === "number_parse") {
    return [
      { label: "Type", value: "Number parse" },
      { label: "Thousands", value: String(spec.thousands_separator || "none") },
      { label: "Invalid values", value: String(spec.invalid_policy || "reject") },
    ];
  }
  if (kind === "enum_map") {
    return [
      { label: "Type", value: "Enum mapping" },
      { label: "Empty values", value: String(spec.empty_policy || "null") },
      { label: "Invalid values", value: String(spec.invalid_policy || "reject") },
    ];
  }
  return [
    { label: "Type", value: kind === "identity" ? "No transform" : kind },
    { label: "Empty values", value: String(spec.empty_policy || "null") },
    { label: "Invalid values", value: String(spec.invalid_policy || "keep") },
  ];
}

export const mappingFormDefaults: MappingFormState = {
  operationFieldId: "",
  sourceId: "",
  operationId: "",
  fieldPath: "",
  semanticTypeId: "",
  canonicalAttributeId: "",
  mappingType: "exact",
  mappingKind: "direct",
  namespace: "public",
  lifecycle: "draft",
  version: "1.0.0",
  confidence: "",
  notes: "",
  transformSpec: "",
  enumMapping: "",
};

type QuickSemanticTypePayload = {
  name: string;
  description: string;
  datatype: string;
};

type QuickCanonicalEntityPayload = {
  name: string;
  description: string;
  semanticTypeId: string;
};

type QuickCanonicalAttributePayload = {
  entityId: string;
  name: string;
  description: string;
  datatype: string;
  semanticTypeId: string;
  identityRole: string;
};

type MappingFormProps = {
  title: string;
  description: string;
  form: MappingFormState;
  sources: ExecutionSource[];
  operations: ExecutionOperation[];
  operationFields?: OperationField[];
  semanticTypes: SemanticType[];
  canonicalEntities?: CanonicalEntity[];
  canonicalAttributes: CanonicalAttribute[];
  onChange: (next: MappingFormState) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onCancel: () => void;
  submitLabel: string;
  submitting?: boolean;
  submitDisabled?: boolean;
  formValidationMessage?: string;
  hideTransform?: boolean;
  sourceFieldLocked?: boolean;
  sourceFieldMultiSelect?: boolean;
  selectedOperationFieldIds?: string[];
  onSelectedOperationFieldIdsChange?: (next: string[]) => void;
  sourceFieldValidationMessage?: string;
  sourceFieldValidationAction?: ReactNode;
  transformSuggestion?: TransformSuggestion | null;
  transformSuggestionLoading?: boolean;
  transformSuggestionError?: string;
  semanticTypeSuggestions?: SemanticTypeSuggestion[];
  semanticSuggestionLoading?: boolean;
  semanticSuggestionError?: string;
  onGenerateMappingSuggestion?: () => void;
  onApplySemanticTypeSuggestion?: (semanticTypeId: string) => void;
  onGenerateTransformSuggestion?: () => void;
  onCreateSemanticType?: (payload: QuickSemanticTypePayload) => Promise<string>;
  onCreateCanonicalEntity?: (payload: QuickCanonicalEntityPayload) => Promise<string>;
  onCreateCanonicalAttribute?: (payload: QuickCanonicalAttributePayload) => Promise<string>;
};

export function MappingForm(props: MappingFormProps) {
  const { form, onChange, sources, operations, operationFields = [], semanticTypes, submitting, submitDisabled, formValidationMessage, sourceFieldLocked, sourceFieldValidationMessage, sourceFieldValidationAction } = props;
  return (
    <FormShell title={props.title} description={props.description} onCancel={props.onCancel} actions={<></>}>
      <form id="mapping-create-form" className="space-y-4" onSubmit={props.onSubmit}>
        <div className="flex items-center justify-between gap-3 border-b border-border/60 pb-3">
          <div className="min-w-0 text-xs text-rose-700">
            {formValidationMessage || ""}
          </div>
          <div className="flex shrink-0 items-center gap-2">
          <Button type="button" variant="outline" onClick={props.onCancel}>
            Cancel
          </Button>
          <Button type="submit" form="mapping-create-form" disabled={submitting || submitDisabled}>
            {props.submitLabel}
          </Button>
          </div>
        </div>
        <MappingFormFields
          form={form}
          onChange={onChange}
          sources={sources}
          operations={operations}
          operationFields={operationFields}
          semanticTypes={semanticTypes}
          canonicalEntities={props.canonicalEntities}
          canonicalAttributes={props.canonicalAttributes}
          sourceFieldLocked={sourceFieldLocked}
          hideTransform={props.hideTransform}
          sourceFieldMultiSelect={props.sourceFieldMultiSelect}
          selectedOperationFieldIds={props.selectedOperationFieldIds}
          onSelectedOperationFieldIdsChange={props.onSelectedOperationFieldIdsChange}
          sourceFieldValidationAction={sourceFieldValidationAction}
          transformSuggestion={props.transformSuggestion}
          transformSuggestionLoading={props.transformSuggestionLoading}
          transformSuggestionError={props.transformSuggestionError}
          semanticTypeSuggestions={props.semanticTypeSuggestions}
          semanticSuggestionLoading={props.semanticSuggestionLoading}
          semanticSuggestionError={props.semanticSuggestionError}
          onGenerateMappingSuggestion={props.onGenerateMappingSuggestion}
          onApplySemanticTypeSuggestion={props.onApplySemanticTypeSuggestion}
          onGenerateTransformSuggestion={props.onGenerateTransformSuggestion}
          onCreateSemanticType={props.onCreateSemanticType}
          onCreateCanonicalEntity={props.onCreateCanonicalEntity}
          onCreateCanonicalAttribute={props.onCreateCanonicalAttribute}
          sourceFieldValidationMessage={sourceFieldValidationMessage}
        />
      </form>
    </FormShell>
  );
}

type MappingFormFieldsProps = {
  form: MappingFormState;
  sources: ExecutionSource[];
  operations: ExecutionOperation[];
  operationFields?: OperationField[];
  semanticTypes: SemanticType[];
  canonicalEntities?: CanonicalEntity[];
  canonicalAttributes: CanonicalAttribute[];
  onChange: (next: MappingFormState) => void;
  onCreateSemanticType?: (payload: QuickSemanticTypePayload) => Promise<string>;
  onCreateCanonicalEntity?: (payload: QuickCanonicalEntityPayload) => Promise<string>;
  onCreateCanonicalAttribute?: (payload: QuickCanonicalAttributePayload) => Promise<string>;
  afterNotes?: ReactNode;
  hideTransform?: boolean;
  sourceFieldLocked?: boolean;
  sourceFieldMultiSelect?: boolean;
  selectedOperationFieldIds?: string[];
  onSelectedOperationFieldIdsChange?: (next: string[]) => void;
  sourceFieldValidationMessage?: string;
  sourceFieldValidationAction?: ReactNode;
  transformSuggestion?: TransformSuggestion | null;
  transformSuggestionLoading?: boolean;
  transformSuggestionError?: string;
  semanticTypeSuggestions?: SemanticTypeSuggestion[];
  semanticSuggestionLoading?: boolean;
  semanticSuggestionError?: string;
  onGenerateMappingSuggestion?: () => void;
  onApplySemanticTypeSuggestion?: (semanticTypeId: string) => void;
  onGenerateTransformSuggestion?: () => void;
};

export function MappingFormFields({
  form,
  sources,
  operations,
  operationFields = [],
  semanticTypes,
  onChange,
  onCreateSemanticType,
  afterNotes,
  hideTransform = false,
  sourceFieldLocked = false,
  sourceFieldMultiSelect = false,
  selectedOperationFieldIds = [],
  onSelectedOperationFieldIdsChange,
  sourceFieldValidationMessage,
  sourceFieldValidationAction,
  transformSuggestion,
  transformSuggestionLoading = false,
  transformSuggestionError = "",
  semanticTypeSuggestions = [],
  semanticSuggestionLoading = false,
  semanticSuggestionError = "",
  onGenerateMappingSuggestion,
  onApplySemanticTypeSuggestion,
  onGenerateTransformSuggestion,
}: MappingFormFieldsProps) {
  const [quickCreateOpen, setQuickCreateOpen] = useState(false);
  const [quickCreateBusy, setQuickCreateBusy] = useState(false);
  const [quickCreateError, setQuickCreateError] = useState("");
  const [semanticTypeSearch, setSemanticTypeSearch] = useState("");
  const [fieldQuery, setFieldQuery] = useState("");
  const [recentSemanticTypeIds, setRecentSemanticTypeIds] = useState<string[]>([]);
  const [advancedTransformOpen, setAdvancedTransformOpen] = useState(false);
  const [semanticTypeDraft, setSemanticTypeDraft] = useState<QuickSemanticTypePayload>({
    name: "",
    description: "",
    datatype: "string",
  });

  const selectedSemanticType = useMemo(() => semanticTypes.find((item) => item.id === form.semanticTypeId) || null, [form.semanticTypeId, semanticTypes]);
  const selectedOperationFieldSet = useMemo(() => new Set(selectedOperationFieldIds), [selectedOperationFieldIds]);

  const semanticTypeOptions = useMemo(
    () => {
      const recentIndex = new Map(recentSemanticTypeIds.map((id, index) => [id, index]));
      return semanticTypes
        .map((item) => {
        const display = item.draft_snapshot || item;
        return {
          value: item.id,
          label: display.name,
          description: display.description || display.datatype || "",
          meta: [display.namespace || "", ...(display.aliases || [])].filter(Boolean).join(" · "),
        };
      })
        .sort((left, right) => {
          const leftRecent = recentIndex.get(left.value);
          const rightRecent = recentIndex.get(right.value);
          if (leftRecent != null && rightRecent != null) return leftRecent - rightRecent;
          if (leftRecent != null) return -1;
          if (rightRecent != null) return 1;
          return `${left.meta || ""} ${left.label}`.localeCompare(`${right.meta || ""} ${right.label}`);
        });
    },
    [recentSemanticTypeIds, semanticTypes]
  );

  const sourceFieldOptions = useMemo(
    () =>
      operationFields
        .map((item) => {
          const operation = operations.find((candidate) => candidate.id === item.operation_id);
          const source = sources.find((candidate) => candidate.id === operation?.source_id);
          const path = String(item.field_path || item.raw_name);
          const displayName = fieldDisplayLabel(item);
          const sourceName = source ? (source.draft_snapshot || source).name : "";
          const operationName = operation?.name || "";
          const sourceContext = [sourceName || null, operationName || null].filter(Boolean).join(" · ");
          const fieldDescription = (item.description || "").trim();
          return {
            value: item.id,
            label: displayName,
            description: fieldDescription || sourceContext,
            meta: [sourceContext || null, path].filter(Boolean).join(" · "),
            fieldPath: path,
            sourceContext,
            fieldDescription,
            operationId: item.operation_id,
            sourceId: operation?.source_id || "",
          };
        })
        .sort((left, right) => `${left.label} ${left.meta || ""}`.localeCompare(`${right.label} ${right.meta || ""}`)),
    [operationFields, operations, sources]
  );
  const filteredSourceFieldOptions = useMemo(() => {
    const normalizedQuery = fieldQuery.trim().toLowerCase();
    return sourceFieldOptions.filter((item) => {
      if (!normalizedQuery) return true;
      return [
        item.label,
        item.description,
        item.meta,
        item.fieldPath,
        item.sourceContext,
        item.operationId,
      ]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(normalizedQuery));
    });
  }, [fieldQuery, sourceFieldOptions]);

  useEffect(() => {
    const matched = semanticTypeOptions.find((item) => item.value === form.semanticTypeId);
    setSemanticTypeSearch(matched?.label || "");
    if (matched) {
      setQuickCreateOpen(false);
      setQuickCreateError("");
    }
  }, [form.semanticTypeId, semanticTypeOptions]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      const raw = window.localStorage.getItem(RECENT_SEMANTIC_TYPE_STORAGE_KEY);
      const parsed = raw ? JSON.parse(raw) : [];
      if (Array.isArray(parsed)) {
        setRecentSemanticTypeIds(parsed.filter((item): item is string => typeof item === "string"));
      }
    } catch {
      setRecentSemanticTypeIds([]);
    }
  }, []);

  useEffect(() => {
    if (!form.semanticTypeId || typeof window === "undefined") return;
    setRecentSemanticTypeIds((current) => {
      const next = [form.semanticTypeId, ...current.filter((item) => item !== form.semanticTypeId)].slice(0, 8);
      window.localStorage.setItem(RECENT_SEMANTIC_TYPE_STORAGE_KEY, JSON.stringify(next));
      return next;
    });
  }, [form.semanticTypeId]);

  useEffect(() => {
    if (!quickCreateOpen) return;

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setQuickCreateOpen(false);
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [quickCreateOpen]);

  async function handleSemanticTypeCreate() {
    if (!onCreateSemanticType) return;
    setQuickCreateBusy(true);
    setQuickCreateError("");
    try {
      const createdId = await onCreateSemanticType(semanticTypeDraft);
      onChange({ ...form, semanticTypeId: createdId });
      setSemanticTypeDraft({ name: "", description: "", datatype: "string" });
      setQuickCreateOpen(false);
    } catch (error) {
      setQuickCreateError(error instanceof Error ? error.message : "Failed to create semantic type.");
    } finally {
      setQuickCreateBusy(false);
    }
  }

  function handleSemanticTypeSearchChange(value: string) {
    setSemanticTypeSearch(value);
    const normalized = value.trim().toLowerCase();
    const matched = semanticTypeOptions.find((item) => item.label.toLowerCase() === normalized);
    if (matched) {
      onChange({ ...form, semanticTypeId: matched.value });
      return;
    }
    if (!normalized) {
      onChange({ ...form, semanticTypeId: "" });
    }
  }

  return (
    <>
      <FormGrid>
        <FormField label="Source Field">
          {sourceFieldMultiSelect && !sourceFieldLocked ? (
            <div className="space-y-2">
              <div>
                <Input
                  value={fieldQuery}
                  onChange={(event) => setFieldQuery(event.target.value)}
                  placeholder="Search unmapped fields"
                />
              </div>
              <div className="rounded-lg border border-border/70 bg-muted/10">
                <div className="flex items-center justify-between gap-3 border-b border-border/70 px-3 py-2 text-[11px] uppercase tracking-[0.12em] text-muted-foreground">
                  <span>Unmapped Fields</span>
                  <span className="normal-case tracking-normal text-muted-foreground/80">
                    {filteredSourceFieldOptions.length} visible
                  </span>
                </div>
                <div className="max-h-72 overflow-auto divide-y divide-border/60">
                  {filteredSourceFieldOptions.map((item) => {
                    const checked = selectedOperationFieldSet.has(item.value);
                    return (
                      <label key={item.value} className="flex cursor-pointer items-start gap-3 px-3 py-2.5 transition hover:bg-muted/20">
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={(event) => {
                            const next = event.target.checked
                              ? [...selectedOperationFieldIds, item.value]
                              : selectedOperationFieldIds.filter((value) => value !== item.value);
                            onSelectedOperationFieldIdsChange?.(next);
                          }}
                          className="mt-0.5 h-4 w-4 rounded border-border"
                        />
                        <div className="min-w-0 flex-1">
                          <div className="truncate text-sm font-medium text-foreground">{item.label}</div>
                          <div className="mt-0.5 truncate text-xs text-muted-foreground">
                            {item.fieldDescription || item.sourceContext || item.operationId}
                          </div>
                          <div className="mt-0.5 truncate font-mono text-[11px] text-muted-foreground/80">{item.fieldPath}</div>
                        </div>
                      </label>
                    );
                  })}
                  {!filteredSourceFieldOptions.length ? (
                    <div className="px-3 py-3 text-xs text-muted-foreground">
                      {sourceFieldOptions.length ? "No unmapped fields match the current search and filters." : "No unmapped fields available."}
                    </div>
                  ) : null}
                </div>
              </div>
            </div>
          ) : (
            <Combobox
              value={
                form.operationFieldId ||
                sourceFieldOptions.find(
                  (item) =>
                    item.fieldPath === form.fieldPath &&
                    item.operationId === form.operationId &&
                    (item.sourceId || "") === (form.sourceId || "")
                )?.value ||
                ""
              }
              options={sourceFieldOptions}
              onValueChange={(next) => {
                const matched = sourceFieldOptions.find((item) => item.value === next);
                if (!matched) return;
                onChange({
                  ...form,
                  operationFieldId: matched.value,
                  fieldPath: matched.fieldPath,
                  operationId: matched.operationId,
                  sourceId: matched.sourceId || form.sourceId,
                });
              }}
              placeholder="Search extracted source field"
              searchPlaceholder="Extracted fields"
              emptyLabel="No extracted fields"
              disabled={sourceFieldLocked}
            />
          )}
          {sourceFieldValidationMessage ? (
            <div className="mt-1 flex items-center justify-between gap-3">
              <p className="text-xs text-rose-700">{sourceFieldValidationMessage}</p>
              {sourceFieldValidationAction ? <div className="shrink-0">{sourceFieldValidationAction}</div> : null}
            </div>
          ) : null}
          {sourceFieldMultiSelect && !sourceFieldLocked ? (
            <div className="mt-2 min-h-[92px] rounded-lg border border-border/70 bg-muted/20 px-3 py-2.5">
              <div className="text-sm font-medium leading-5 text-foreground">
                {selectedOperationFieldIds.length
                  ? `${selectedOperationFieldIds.length} source fields selected`
                  : "Select one or more unmapped fields"}
              </div>
              <div className="mt-1 text-xs leading-5 text-muted-foreground">
                {selectedOperationFieldIds.length
                  ? "Each selected source field will create its own mapping proposal using the semantic type below."
                  : "Choose unmapped fields to assign the same semantic type in one action."}
              </div>
            </div>
          ) : (() => {
            const matched = sourceFieldOptions.find((item) => item.value === form.operationFieldId);

            if (matched) {
              return (
                <div className="mt-2 min-h-[92px] rounded-lg border border-border/70 bg-muted/20 px-3 py-2.5">
                  <div className="text-sm font-medium leading-5 text-foreground">{matched.label}</div>
                  <div className="mt-1 text-xs leading-5 text-muted-foreground">
                    {matched.fieldDescription || matched.sourceContext || form.operationId || "Mapped from extracted source field"}
                  </div>
                  {matched.fieldDescription && matched.sourceContext ? (
                    <div className="mt-1 truncate text-[11px] leading-5 text-muted-foreground/80">{matched.sourceContext}</div>
                  ) : null}
                  <div className="mt-1 truncate font-mono text-[11px] leading-5 text-muted-foreground/80">{matched.fieldPath}</div>
                </div>
              );
            }

            return (
              <div className="mt-2 flex min-h-[92px] items-start rounded-lg border border-dashed border-border/70 bg-muted/10 px-3 py-2.5 text-xs leading-5 text-muted-foreground">
                {sourceFieldLocked
                  ? "Source field identity is fixed for this mapping. Create a new mapping to change the field."
                  : "Choose one extracted field from source onboarding. Manual field path input is not allowed here."}
              </div>
            );
          })()}
        </FormField>

        <FormField
          label="Semantic Type"
          action={
            <div className="flex items-center gap-1">
            {onCreateSemanticType ? (
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="h-7 px-2 text-[11px] tracking-normal"
                onClick={() => {
                  setQuickCreateError("");
                  setSemanticTypeDraft((current) => ({ ...current, name: form.fieldPath.split(".").slice(-1)[0] || current.name }));
                  setQuickCreateOpen(true);
                }}
              >
                <Plus className="h-3.5 w-3.5" />
                New
              </Button>
            ) : null}
            </div>
          }
        >
          <Combobox
            value={form.semanticTypeId}
            options={semanticTypeOptions}
            onValueChange={(next) => onChange({ ...form, semanticTypeId: next })}
            allowCustomInput
            placeholder="Search semantic type"
            searchPlaceholder="Search semantic types"
            emptyLabel="No semantic types"
            customInputValue={semanticTypeSearch}
            onCustomInputChange={handleSemanticTypeSearchChange}
          />
          {form.semanticTypeId ? (
            <div className="mt-2 min-h-[92px] rounded-lg border border-border/70 bg-muted/20 px-3 py-2.5">
              <div className="text-sm font-medium leading-5 text-foreground">
                {(selectedSemanticType?.draft_snapshot || selectedSemanticType)?.name || form.semanticTypeId}
              </div>
              {((selectedSemanticType?.draft_snapshot || selectedSemanticType)?.description || "").trim() ? (
                <div className="mt-1 text-xs leading-5 text-muted-foreground">
                  {(selectedSemanticType?.draft_snapshot || selectedSemanticType)?.description}
                </div>
              ) : null}
              <div className="mt-1 flex flex-wrap gap-x-2 gap-y-1 text-[11px] leading-5 text-muted-foreground/80">
                {(selectedSemanticType?.draft_snapshot || selectedSemanticType)?.namespace ? <span>namespace {(selectedSemanticType?.draft_snapshot || selectedSemanticType)?.namespace}</span> : null}
                {(selectedSemanticType?.draft_snapshot || selectedSemanticType)?.datatype ? <span>datatype {(selectedSemanticType?.draft_snapshot || selectedSemanticType)?.datatype}</span> : null}
                {((selectedSemanticType?.draft_snapshot || selectedSemanticType)?.aliases || []).length ? <span>aliases {((selectedSemanticType?.draft_snapshot || selectedSemanticType)?.aliases || []).join(", ")}</span> : null}
              </div>
            </div>
          ) : (
            <div className="mt-2 flex min-h-[92px] items-start rounded-lg border border-dashed border-border/70 bg-muted/10 px-3 py-2.5 text-xs leading-5 text-muted-foreground">
              Choose the meaning for this source field.
            </div>
          )}
          {semanticSuggestionError ? (
            <div className="mt-2 flex items-center justify-between gap-3 rounded-lg border border-rose-500/20 bg-rose-500/5 px-3 py-2 text-xs text-rose-700">
              <span>{semanticSuggestionError}</span>
              {onGenerateMappingSuggestion ? (
                <Button type="button" variant="ghost" size="sm" className="h-7 px-2 text-[11px]" onClick={onGenerateMappingSuggestion}>
                  Retry
                </Button>
              ) : null}
            </div>
          ) : null}
          {semanticSuggestionLoading && !semanticTypeSuggestions.length ? (
            <div className="mt-2 rounded-xl border border-sky-500/20 bg-sky-500/[0.06] px-3 py-3 text-xs text-muted-foreground">
              Generating semantic and transform suggestions...
            </div>
          ) : null}
          {semanticTypeSuggestions.length ? (
            <div className="mt-2 rounded-xl border border-sky-500/20 bg-sky-500/[0.06] p-3">
              <div className="mb-2 flex items-center justify-between gap-3">
                <div>
                  <div className="text-xs font-semibold uppercase tracking-[0.12em] text-sky-700">AI Suggestions</div>
                  <div className="mt-0.5 text-[11px] text-muted-foreground">Select one candidate to fill semantic type and transform contract.</div>
                </div>
                {onGenerateMappingSuggestion ? (
                  <Button type="button" variant="ghost" size="sm" className="h-7 px-2 text-[11px]" onClick={onGenerateMappingSuggestion} disabled={semanticSuggestionLoading}>
                    <RefreshCw className={`h-3.5 w-3.5 ${semanticSuggestionLoading ? "animate-spin" : ""}`} />
                    Refresh
                  </Button>
                ) : null}
              </div>
              <div className="space-y-2">
                {semanticTypeSuggestions.slice(0, 3).map((item) => (
                  <button
                    type="button"
                    key={item.semantic_type_id}
                    className={`w-full rounded-lg border px-3 py-2 text-left transition hover:border-primary/40 hover:bg-primary/[0.04] ${
                      form.semanticTypeId === item.semantic_type_id ? "border-primary/50 bg-primary/[0.07]" : "border-border/60 bg-background/60"
                    }`}
                    onClick={() => onApplySemanticTypeSuggestion?.(item.semantic_type_id)}
                  >
                    <div className="flex items-center justify-between gap-3">
                      <span className="truncate text-sm font-medium text-foreground">{item.name}</span>
                      <span className="shrink-0 text-[11px] text-muted-foreground">{Math.round(item.confidence * 100)}%</span>
                    </div>
                    <div className="mt-0.5 truncate text-xs text-muted-foreground">
                      {item.datatype}
                      {item.rationale ? ` · ${item.rationale}` : ""}
                    </div>
                    {form.semanticTypeId === item.semantic_type_id ? (
                      <div className="mt-1 text-[11px] font-medium text-primary">Selected. Transform contract below was filled from this suggestion.</div>
                    ) : null}
                  </button>
                ))}
              </div>
            </div>
          ) : null}
        </FormField>
      </FormGrid>

      {!hideTransform ? (
      <div className="rounded-xl border border-border/70 bg-muted/10 p-3">
        <div className="mb-3 flex items-center justify-between gap-3">
          <div>
            <div className="text-sm font-semibold text-foreground">Transform</div>
            <div className="mt-0.5 text-xs text-muted-foreground">Define value conversion when the raw field shape differs from the semantic type.</div>
          </div>
        </div>
        {transformSuggestionError ? (
          <div className="mb-3 flex items-center justify-between gap-3 rounded-lg border border-rose-500/20 bg-rose-500/5 px-3 py-2 text-xs text-rose-700">
            <span>{transformSuggestionError}</span>
            {(onGenerateTransformSuggestion || onGenerateMappingSuggestion) ? (
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="h-7 px-2 text-[11px]"
                onClick={onGenerateTransformSuggestion || onGenerateMappingSuggestion}
              >
                Retry
              </Button>
            ) : null}
          </div>
        ) : null}
        {(transformSuggestionLoading || semanticSuggestionLoading) && !transformSuggestion ? (
          <div className="mb-4 rounded-xl border border-sky-500/20 bg-sky-500/[0.06] px-3 py-3 text-xs text-muted-foreground">
            Generating transform suggestion...
          </div>
        ) : null}
        {transformSuggestion ? (
          <div className="mb-4 rounded-xl border border-sky-500/20 bg-sky-500/[0.06] p-3">
            <div className="flex items-start justify-between gap-3">
              <div>
                <div className="text-xs font-semibold uppercase tracking-[0.12em] text-sky-700">Recommended Transform</div>
                <div className="mt-1 text-sm font-medium text-foreground">{transformSummary(transformSuggestion.transform_spec)}</div>
                <div className="mt-1 text-xs leading-5 text-muted-foreground">{transformSuggestion.rationale}</div>
              </div>
              <span className="shrink-0 rounded-full border border-sky-500/20 bg-background/70 px-2 py-1 text-[11px] text-sky-700">
                {Math.round(transformSuggestion.confidence * 100)}%
              </span>
            </div>
            <div className="mt-3 grid gap-2 md:grid-cols-3">
              {transformDetailItems(transformSuggestion.transform_spec).map((item) => (
                <div key={item.label} className="rounded-lg border border-border/60 bg-background/60 px-3 py-2">
                  <div className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">{item.label}</div>
                  <div className="mt-1 truncate text-xs font-medium text-foreground">{item.value}</div>
                </div>
              ))}
            </div>
            {transformSuggestion.preview.length ? (
              <div className="mt-3 overflow-hidden rounded-lg border border-border/60 bg-background/60">
                <div className="grid grid-cols-[1fr_1fr_72px] border-b border-border/60 px-3 py-2 text-[11px] font-semibold uppercase tracking-[0.12em] text-muted-foreground">
                  <span>Input</span>
                  <span>Output</span>
                  <span>Status</span>
                </div>
                {transformSuggestion.preview.map((item, index) => (
                  <div key={`${item.input}-${index}`} className="grid grid-cols-[1fr_1fr_72px] gap-3 border-b border-border/40 px-3 py-2 last:border-b-0">
                    <span className="truncate font-mono text-xs text-foreground">{item.input}</span>
                    <span className="truncate font-mono text-xs text-foreground">{item.output == null ? "-" : String(item.output)}</span>
                    <span className={item.ok ? "text-xs text-emerald-700" : "text-xs text-rose-700"}>{item.ok ? "ok" : "fail"}</span>
                  </div>
                ))}
              </div>
            ) : (
              <div className="mt-3 rounded-lg border border-dashed border-border/70 bg-background/50 px-3 py-2 text-xs text-muted-foreground">
                No sample values are available yet. Review the suggested spec before saving.
              </div>
            )}
          </div>
        ) : null}
        <div className="rounded-lg border border-border/70 bg-background/60">
          <button
            type="button"
            className="flex w-full items-center justify-between gap-3 px-3 py-2 text-left"
            onClick={() => setAdvancedTransformOpen((current) => !current)}
          >
            <span>
              <span className="block text-xs font-semibold uppercase tracking-[0.12em] text-muted-foreground">Advanced Contract</span>
              <span className="mt-0.5 block text-xs text-muted-foreground">Only edit this if the preview is wrong or the contract needs a specific runtime policy.</span>
            </span>
            <span className="text-xs font-medium text-primary">{advancedTransformOpen ? "Hide" : "Show"}</span>
          </button>
          {advancedTransformOpen ? (
            <div className="border-t border-border/70 p-3">
              <div className="grid gap-4 md:grid-cols-2">
                <FormField label="Mapping Type">
                  <FormSelect
                    value={form.mappingType}
                    onChange={(event) => {
                      const mappingType = event.target.value;
                      onChange({
                        ...form,
                        mappingType,
                        mappingKind: mappingType === "exact" ? "direct" : mappingType === "enum" ? "enum" : "transform",
                      });
                    }}
                  >
                    <option value="exact">exact</option>
                    <option value="transform">transform</option>
                    <option value="enum">enum</option>
                    <option value="composite">composite</option>
                  </FormSelect>
                </FormField>
                <FormField label="Mapping Kind">
                  <FormSelect value={form.mappingKind} onChange={(event) => onChange({ ...form, mappingKind: event.target.value })}>
                    <option value="direct">direct</option>
                    <option value="transform">transform</option>
                    <option value="enum">enum</option>
                    <option value="composite">composite</option>
                  </FormSelect>
                </FormField>
              </div>
              <div className="mt-4 grid gap-4 md:grid-cols-2">
                <FormField label="Transform Spec">
                  <FormTextarea
                    value={form.transformSpec}
                    onChange={(event) => onChange({ ...form, transformSpec: event.target.value })}
                    placeholder='{"kind":"date_parse","input_format":"yyyyMMddHHmmss","output_format":"ISO_DATETIME","empty_policy":"null","invalid_policy":"reject"}'
                    className="min-h-28 font-mono text-xs"
                  />
                  <div className="mt-2 rounded-lg border border-border/60 bg-background/60 px-3 py-2 text-[11px] leading-5 text-muted-foreground">
                    <div className="font-medium text-foreground">Examples</div>
                    <div><span className="font-mono">date_parse</span>: <span className="font-mono">{'{"kind":"date_parse","input_format":"yyyyMMdd","output_format":"ISO_DATE"}'}</span></div>
                    <div><span className="font-mono">number_parse</span>: <span className="font-mono">{'{"kind":"number_parse","thousands_separator":","}'}</span></div>
                  </div>
                </FormField>
                <FormField label="Enum Mapping">
                  <FormTextarea
                    value={form.enumMapping}
                    onChange={(event) => onChange({ ...form, enumMapping: event.target.value })}
                    placeholder='{"Y":true,"N":false}'
                    className="min-h-28 font-mono text-xs"
                  />
                  <div className="mt-2 rounded-lg border border-border/60 bg-background/60 px-3 py-2 text-[11px] leading-5 text-muted-foreground">
                    <div className="font-medium text-foreground">Examples</div>
                    <div><span className="font-mono">boolean</span>: <span className="font-mono">{'{"Y":true,"N":false}'}</span></div>
                    <div><span className="font-mono">code</span>: <span className="font-mono">{'{"1":"notice_date","2":"close_date"}'}</span></div>
                  </div>
                </FormField>
              </div>
            </div>
          ) : null}
        </div>
      </div>
      ) : null}

      <FormField label="Notes">
        <FormTextarea
          value={form.notes}
          onChange={(event) => onChange({ ...form, notes: event.target.value })}
          placeholder="optional rationale or mapping note"
          className="min-h-20"
        />
      </FormField>

      {afterNotes}

      {quickCreateOpen ? (
        <div className="fixed inset-0 z-[90]">
          <button type="button" aria-label="Close semantic type dialog" className="absolute inset-0 bg-black/25" onClick={() => setQuickCreateOpen(false)} />
          <div className="fixed left-1/2 top-1/2 w-[min(92vw,560px)] -translate-x-1/2 -translate-y-1/2 rounded-2xl border border-border/80 bg-background p-5 shadow-2xl">
            <div className="mb-4 flex items-start justify-between gap-4 border-b border-border/70 pb-3">
              <div>
                <h4 className="text-sm font-semibold text-foreground">Create Semantic Type</h4>
                <p className="mt-1 text-xs text-muted-foreground">Create the missing semantic type here and continue the mapping.</p>
              </div>
              <Button type="button" variant="ghost" size="sm" onClick={() => setQuickCreateOpen(false)}>
                Close
              </Button>
            </div>

            {quickCreateError ? (
              <div className="mb-4 rounded-lg border border-rose-500/20 bg-rose-500/5 px-3 py-2 text-sm text-rose-700">
                {quickCreateError}
              </div>
            ) : null}

            <div className="space-y-4">
              <FormGrid>
                <FormField label="Name">
                  <Input value={semanticTypeDraft.name} onChange={(event) => setSemanticTypeDraft({ ...semanticTypeDraft, name: event.target.value })} />
                </FormField>
                <FormField label="Datatype">
                  <Input value={semanticTypeDraft.datatype} onChange={(event) => setSemanticTypeDraft({ ...semanticTypeDraft, datatype: event.target.value })} />
                </FormField>
              </FormGrid>
              <FormField label="Description">
                <FormTextarea value={semanticTypeDraft.description} onChange={(event) => setSemanticTypeDraft({ ...semanticTypeDraft, description: event.target.value })} />
              </FormField>
              <div className="flex justify-end gap-2 border-t border-border/70 pt-3">
                <Button type="button" variant="outline" onClick={() => setQuickCreateOpen(false)}>
                  Cancel
                </Button>
                <Button type="button" onClick={() => void handleSemanticTypeCreate()} disabled={quickCreateBusy || !semanticTypeDraft.name.trim()}>
                  Create Semantic Type
                </Button>
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
}
