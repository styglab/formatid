"use client";

import type { FormEvent } from "react";
import type { SemanticType } from "@/types/semantic";
import { Input } from "@/components/ui/input";
import { FormField, FormGrid, FormSelect, FormShell, FormTextarea } from "@/components/semantic/forms/form-shell";

export type SemanticTypeFormState = {
  name: string;
  description: string;
  datatype: string;
  entityKind: string;
  parentEntityId: string;
  aliases: string;
  owners: string;
  status: string;
};

export const semanticTypeFormDefaults: SemanticTypeFormState = {
  name: "",
  description: "",
  datatype: "string",
  entityKind: "entity",
  parentEntityId: "",
  aliases: "",
  owners: "",
  status: "draft"
};

type SemanticTypeFormProps = {
  title: string;
  description: string;
  form: SemanticTypeFormState;
  entities: SemanticType[];
  onChange: (next: SemanticTypeFormState) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onCancel: () => void;
  submitLabel: string;
  submitting?: boolean;
};

export function SemanticTypeForm(props: SemanticTypeFormProps) {
  const { form, onChange, entities } = props;
  return (
    <FormShell {...props}>
      <FormGrid>
        <FormField label="Name">
          <Input value={form.name} onChange={(event) => onChange({ ...form, name: event.target.value })} required />
        </FormField>
        <FormField label="Datatype">
          <Input value={form.datatype} onChange={(event) => onChange({ ...form, datatype: event.target.value })} />
        </FormField>
        <FormField label="Entity Kind">
          <FormSelect value={form.entityKind} onChange={(event) => onChange({ ...form, entityKind: event.target.value })}>
            <option value="entity">entity</option>
            <option value="attribute">attribute</option>
          </FormSelect>
        </FormField>
        <FormField label="Status">
          <FormSelect value={form.status} onChange={(event) => onChange({ ...form, status: event.target.value })}>
            <option value="draft">draft</option>
            <option value="review">review</option>
            <option value="approved">approved</option>
            <option value="published">published</option>
          </FormSelect>
        </FormField>
      </FormGrid>
      {form.entityKind === "attribute" ? (
        <FormField label="Parent Entity">
          <FormSelect value={form.parentEntityId} onChange={(event) => onChange({ ...form, parentEntityId: event.target.value })}>
            <option value="">select entity</option>
            {entities.map((item) => {
              const display = item.draft_snapshot || item;
              return (
                <option key={item.id} value={item.id}>
                  {display.name}
                </option>
              );
            })}
          </FormSelect>
        </FormField>
      ) : null}
      <FormField label="Description">
        <FormTextarea value={form.description} onChange={(event) => onChange({ ...form, description: event.target.value })} />
      </FormField>
      <FormGrid>
        <FormField label="Aliases">
          <Input value={form.aliases} onChange={(event) => onChange({ ...form, aliases: event.target.value })} placeholder="comma,separated" />
        </FormField>
        <FormField label="Owners">
          <Input value={form.owners} onChange={(event) => onChange({ ...form, owners: event.target.value })} placeholder="comma,separated" />
        </FormField>
      </FormGrid>
    </FormShell>
  );
}
