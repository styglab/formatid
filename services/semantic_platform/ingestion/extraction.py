from __future__ import annotations

import re
import zipfile
from pathlib import Path
from xml.etree import ElementTree


WORD_NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}


def extract_text(path: str | Path) -> str:
    source = Path(path)
    suffix = source.suffix.lower()
    if suffix in {".txt", ".md", ".json", ".yaml", ".yml", ".csv"}:
        return source.read_text(encoding="utf-8", errors="ignore")
    if suffix == ".docx":
        return _extract_docx(source)
    if suffix == ".zip":
        return _extract_zip(source)
    return source.read_text(encoding="utf-8", errors="ignore")


def _extract_docx(path: Path) -> str:
    with zipfile.ZipFile(path) as archive:
        names = [name for name in archive.namelist() if name.startswith("word/") and name.endswith(".xml")]
        chunks = []
        for name in sorted(names):
            if name not in {"word/document.xml"} and not name.startswith("word/header") and not name.startswith("word/footer"):
                continue
            root = ElementTree.fromstring(archive.read(name))
            chunks.append(_docx_xml_text(root))
        return "\n".join(chunks)


def _docx_xml_text(root: ElementTree.Element) -> str:
    body = root.find("w:body", WORD_NS)
    if body is None:
        return _node_text(root)
    chunks = []
    for child in body:
        if child.tag.endswith("}p"):
            text = _node_text(child)
            if text:
                chunks.append(text)
        elif child.tag.endswith("}tbl"):
            for row in child.findall(".//w:tr", WORD_NS):
                cells = []
                for cell in row.findall("./w:tc", WORD_NS):
                    paragraphs = [_node_text(paragraph) for paragraph in cell.findall("./w:p", WORD_NS)]
                    text = " ".join(value for value in paragraphs if value.strip()) or _node_text(cell)
                    value = re.sub(r"\s+", " ", text).strip()
                    cells.append(value)
                if any(cells):
                    chunks.append("| " + " | ".join(cells) + " |")
    return "\n".join(chunks)


def _node_text(node: ElementTree.Element) -> str:
    values = [
        text_node.text or ""
        for text_node in node.iter()
        if text_node.tag.endswith("}t") or text_node.tag == "t"
    ]
    return "".join(values)


def _extract_zip(path: Path) -> str:
    chunks = []
    with zipfile.ZipFile(path) as archive:
        for name in sorted(archive.namelist()):
            if name.endswith("/"):
                continue
            suffix = Path(name).suffix.lower()
            if suffix not in {".txt", ".md", ".xml", ".json", ".yaml", ".yml"}:
                continue
            data = archive.read(name)
            chunks.append(f"\n\n# ZIP_ENTRY {name}\n{data.decode('utf-8', errors='ignore')}")
    return "\n".join(chunks)


def compact_text(text: str, limit: int = 40000) -> str:
    normalized = re.sub(r"\r\n?", "\n", text)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized[:limit]
