from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from xml.etree import ElementTree
from zipfile import ZipFile


@dataclass
class LoadedSource:
    source_id: str
    source_name: str
    source_type: str
    filename: str
    media_type: str
    reference_uri: str
    stored_path: str
    content_text: str
    content_json: dict[str, Any] | list[Any] | None


@dataclass
class DiscoveredAsset:
    name: str
    asset_type: str
    locator: str
    description: str
    metadata: dict[str, Any] = field(default_factory=dict)
    access_paths: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class DiscoveredField:
    scope: str
    raw_name: str
    field_path: str
    data_type: str
    is_required: bool
    description: str = ""
    evidence: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class StructureOperationDraft:
    operation_key: str
    fields: list[DiscoveredField] = field(default_factory=list)


def load_source_payload(source: dict[str, Any]) -> LoadedSource:
    config = source.get("config") if isinstance(source.get("config"), dict) else {}
    upload = config.get("upload") if isinstance(config.get("upload"), dict) else {}
    stored_path = str(upload.get("stored_path") or "")
    reference_uri = str(config.get("reference_uri") or "")
    filename = str(upload.get("filename") or Path(reference_uri).name or f"{source.get('id')}.txt")
    media_type = str(upload.get("media_type") or "application/octet-stream")
    content_text = ""
    content_json: dict[str, Any] | list[Any] | None = None

    file_path = Path(stored_path) if stored_path else Path(reference_uri) if reference_uri.startswith("/") else None
    extracted_text = str(upload.get("extracted_text") or "")
    if file_path and file_path.exists():
        raw = file_path.read_bytes()
        content_text = extract_text_from_bytes(file_path.name, raw)
        try:
            content_json = json.loads(content_text)
        except json.JSONDecodeError:
            content_json = None
    else:
        preview = str(upload.get("preview") or "")
        content_text = extracted_text or preview

    return LoadedSource(
        source_id=str(source.get("id") or ""),
        source_name=str(source.get("name") or source.get("id") or ""),
        source_type=str(source.get("source_type") or "other"),
        filename=filename,
        media_type=media_type,
        reference_uri=reference_uri,
        stored_path=stored_path,
        content_text=content_text,
        content_json=content_json,
    )


def determine_ingestion_strategy(loaded: LoadedSource) -> str:
    suffix = Path(loaded.filename).suffix.lower()
    text = loaded.content_text[:2000].lower()
    if suffix in {".json", ".csv", ".tsv"}:
        return "structured"
    if suffix in {".yaml", ".yml"}:
        return "semi_structured"
    if loaded.source_type in {"api", "table", "file"} and ("openapi" in text or "swagger" in text or "paths:" in text):
        return "semi_structured"
    return "document_heavy"


def parse_csv_rows(content_text: str) -> list[list[str]]:
    if not content_text.strip():
        return []
    sample = content_text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample)
    except csv.Error:
        dialect = csv.excel
    reader = csv.reader(io.StringIO(content_text), dialect=dialect)
    return [row for row in reader]


def extract_text_from_bytes(filename: str, raw: bytes) -> str:
    suffix = Path(filename).suffix.lower()
    if suffix == ".docx":
        extracted = _extract_docx_text_from_bytes(raw)
        if extracted.strip():
            return extracted
    return raw.decode("utf-8", errors="replace")


def _extract_docx_text_from_bytes(raw: bytes) -> str:
    try:
        with ZipFile(io.BytesIO(raw)) as archive:
            xml_bytes = archive.read("word/document.xml")
    except Exception:
        return ""
    root = ElementTree.fromstring(xml_bytes)
    namespace = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    body = root.find("w:body", namespace)
    if body is None:
        return ""
    lines: list[str] = []
    for paragraph in body.findall(".//w:p", namespace):
        texts = [node.text or "" for node in paragraph.findall(".//w:t", namespace)]
        line = "".join(texts).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)
