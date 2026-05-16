# Semantic Engine

`core/semantic` provides app-neutral helpers for building AI-ready semantic
objects and documents.

Apps should provide a small semantic spec and keep only domain-specific rules in
app code.

Recommended pattern:

```txt
core/semantic
  builder.py      # spec + record -> semantic object/document

apps/<app>/semantic
  spec.py         # entity, attribute, relationship, document mapping
  semantic.py     # thin app-specific wrapper and custom rules only
```

Use the common builder for:

- entity id, label, and attributes
- simple relationships from one record
- semantic document text

Keep app-specific code for:

- nested relationship expansion
- domain inference tags
- eligibility or compliance rules
