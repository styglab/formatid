from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Any
from xml.etree import ElementTree
from zipfile import ZipFile

from services.context_platform.internal.ingestion.parsers.common import LoadedSource


_WORD_NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}


@dataclass
class DocumentElement:
    kind: str
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DocumentChunk:
    chunk_id: str
    chunk_type: str
    heading: str
    text: str
    evidence_refs: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


def extract_document_elements(loaded: LoadedSource) -> list[DocumentElement]:
    suffix = Path(loaded.filename).suffix.lower()
    if suffix == ".docx":
        elements = _extract_docx_elements(Path(loaded.stored_path or loaded.reference_uri))
        if elements:
            return elements
    return _extract_text_elements(loaded.content_text)


def extract_document_chunks(loaded: LoadedSource) -> tuple[list[DocumentChunk], str]:
    suffix = Path(loaded.filename).suffix.lower()
    path = Path(loaded.stored_path or loaded.reference_uri)
    if suffix in {".docx", ".pdf", ".html", ".htm"} and path.exists():
        docling_chunks = _extract_docling_chunks(path)
        if docling_chunks:
            return docling_chunks, "docling"
    return build_document_chunks(extract_document_elements(loaded)), "fallback"


def build_document_chunks(elements: list[DocumentElement]) -> list[DocumentChunk]:
    chunks: list[DocumentChunk] = []
    current_heading = ""
    paragraph_buffer: list[str] = []
    paragraph_evidence: list[dict[str, Any]] = []
    chunk_index = 0

    def flush_paragraphs() -> None:
        nonlocal chunk_index, paragraph_buffer, paragraph_evidence
        text = "\n".join(item for item in paragraph_buffer if item.strip()).strip()
        if not text:
            paragraph_buffer = []
            paragraph_evidence = []
            return
        chunks.append(
            DocumentChunk(
                chunk_id=f"chunk_{chunk_index:03d}",
                chunk_type="narrative",
                heading=current_heading,
                text=text,
                evidence_refs=list(paragraph_evidence),
                metadata={},
            )
        )
        chunk_index += 1
        paragraph_buffer = []
        paragraph_evidence = []

    for index, element in enumerate(elements):
        if not element.text.strip():
            continue
        if element.kind == "heading":
            flush_paragraphs()
            current_heading = element.text.strip()
            chunks.append(
                DocumentChunk(
                    chunk_id=f"chunk_{chunk_index:03d}",
                    chunk_type="heading",
                    heading=current_heading,
                    text=element.text.strip(),
                    evidence_refs=[{"kind": "heading", "element_index": index, "text": element.text[:200]}],
                    metadata={},
                )
            )
            chunk_index += 1
            continue
        if element.kind == "table":
            flush_paragraphs()
            chunks.append(
                DocumentChunk(
                    chunk_id=f"chunk_{chunk_index:03d}",
                    chunk_type="table",
                    heading=current_heading,
                    text=element.text.strip(),
                    evidence_refs=[{"kind": "table", "element_index": index, "text": element.text[:400]}],
                    metadata={},
                )
            )
            chunk_index += 1
            continue
        paragraph_buffer.append(element.text.strip())
        paragraph_evidence.append({"kind": element.kind, "element_index": index, "text": element.text[:200]})
    flush_paragraphs()
    return chunks


def summarize_chunks(chunks: list[DocumentChunk], *, limit: int = 12) -> list[dict[str, Any]]:
    return [
        {
            "chunk_id": chunk.chunk_id,
            "chunk_type": chunk.chunk_type,
            "heading": chunk.heading,
            "preview": chunk.text[:240],
            "evidence_refs": chunk.evidence_refs[:3],
            "metadata": chunk.metadata,
        }
        for chunk in chunks[:limit]
    ]


def _extract_docling_chunks(path: Path) -> list[DocumentChunk]:
    try:
        from docling.datamodel.base_models import InputFormat
        from docling.datamodel.pipeline_options import PdfPipelineOptions
        from docling.document_converter import DocumentConverter
        from docling.document_converter import PdfFormatOption
        from docling_core.transforms.chunker import HierarchicalChunker
    except Exception:
        return []

    try:
        converter = DocumentConverter()
        if path.suffix.lower() == ".pdf":
            options = PdfPipelineOptions()
            options.do_ocr = False
            options.force_backend_text = True
            converter = DocumentConverter(
                format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=options)}
            )
        result = converter.convert(str(path))
        doc = result.document
        chunker = HierarchicalChunker()
        chunks: list[DocumentChunk] = []
        for index, chunk in enumerate(chunker.chunk(doc)):
            chunk_text = getattr(chunk, "text", "") or ""
            contextualized = ""
            try:
                contextualized = chunker.contextualize(chunk)
            except Exception:
                contextualized = ""
            text = (contextualized or chunk_text or "").strip()
            if not text:
                continue
            headings = _docling_headings(chunk)
            chunk_type = _infer_docling_chunk_type(text)
            chunks.append(
                DocumentChunk(
                    chunk_id=f"docling_chunk_{index:03d}",
                    chunk_type=chunk_type,
                    heading=" > ".join(headings),
                    text=text,
                    evidence_refs=[
                        {
                            "kind": "docling_chunk",
                            "chunk_index": index,
                            "headings": headings,
                            "preview": text[:300],
                        }
                    ],
                    metadata={
                        "source": "docling",
                        "headings": headings,
                        "raw_text_preview": chunk_text[:300],
                    },
                )
            )
        if chunks:
            return chunks
        markdown = doc.export_to_markdown()
        if markdown.strip():
            return build_document_chunks(_extract_text_elements(markdown))
        return chunks
    except Exception:
        return []


def _docling_headings(chunk: Any) -> list[str]:
    meta = getattr(chunk, "meta", None)
    headings = getattr(meta, "headings", None) if meta is not None else None
    if isinstance(headings, list):
        return [str(item).strip() for item in headings if str(item).strip()]
    if headings:
        return [str(headings).strip()]
    return []


def _infer_docling_chunk_type(text: str) -> str:
    stripped = text.strip()
    if not stripped:
        return "narrative"
    if stripped.startswith("|"):
        return "table"
    first_line = stripped.splitlines()[0]
    if _looks_like_heading(first_line):
        return "heading"
    return "narrative"


def _extract_docx_elements(path: Path) -> list[DocumentElement]:
    if not path.exists():
        return []
    try:
        with ZipFile(path) as archive:
            xml_bytes = archive.read("word/document.xml")
    except Exception:
        return []
    root = ElementTree.fromstring(xml_bytes)
    body = root.find("w:body", _WORD_NS)
    if body is None:
        return []
    elements: list[DocumentElement] = []
    for child in body:
        tag = child.tag.rsplit("}", 1)[-1]
        if tag == "p":
            text = _paragraph_text(child)
            if not text.strip():
                continue
            style = _paragraph_style(child)
            kind = "heading" if style.startswith("Heading") or _looks_like_heading(text) else "paragraph"
            elements.append(DocumentElement(kind=kind, text=text.strip(), metadata={"style": style}))
        elif tag == "tbl":
            table_text = _table_text(child)
            if table_text.strip():
                elements.append(DocumentElement(kind="table", text=table_text.strip()))
    return elements


def _extract_text_elements(text: str) -> list[DocumentElement]:
    elements: list[DocumentElement] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        kind = "heading" if _looks_like_heading(stripped) else "paragraph"
        elements.append(DocumentElement(kind=kind, text=stripped))
    return elements


def _paragraph_text(node: ElementTree.Element) -> str:
    texts = [child.text or "" for child in node.findall(".//w:t", _WORD_NS)]
    return "".join(texts)


def _paragraph_style(node: ElementTree.Element) -> str:
    style = node.find("./w:pPr/w:pStyle", _WORD_NS)
    if style is None:
        return ""
    return str(style.attrib.get(f"{{{_WORD_NS['w']}}}val") or "")


def _table_text(node: ElementTree.Element) -> str:
    rows: list[str] = []
    for row in node.findall("./w:tr", _WORD_NS):
        cells = []
        for cell in row.findall("./w:tc", _WORD_NS):
            texts = [child.text or "" for child in cell.findall(".//w:t", _WORD_NS)]
            cell_text = " ".join(part.strip() for part in texts if part.strip()).strip()
            cells.append(cell_text)
        cleaned = [cell for cell in cells if cell]
        if cleaned:
            rows.append(" | ".join(cleaned))
    return "\n".join(rows)


def _looks_like_heading(text: str) -> bool:
    stripped = text.strip()
    if len(stripped) > 80:
        return False
    if re.match(r"^\d+(\.\d+)*\s+\S+", stripped):
        return True
    keywords = ("api", "openapi", "요청", "응답", "파라미터", "parameter", "response", "request", "endpoint")
    lowered = stripped.lower()
    return any(keyword in lowered for keyword in keywords) and len(stripped.split()) <= 8
