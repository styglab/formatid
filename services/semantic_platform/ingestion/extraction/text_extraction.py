from __future__ import annotations

import io
import re
import zipfile
from html import unescape
from pathlib import Path
from xml.etree import ElementTree

def _extract_docx_text(path: Path) -> str:
    return _extract_docx_bytes(path.read_bytes())


def _extract_docx_bytes(raw: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        xml = archive.read("word/document.xml")
    root = ElementTree.fromstring(xml)
    namespace = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    parts = [node.text or "" for node in root.findall(".//w:t", namespace)]
    return unescape("\n".join(part for part in parts if part.strip()))


def _extract_zip_text(path: Path) -> str:
    sections = []
    with zipfile.ZipFile(path) as archive:
        for name in archive.namelist():
            if name.endswith("/"):
                continue
            suffix = Path(name).suffix.lower()
            if suffix not in {".docx", ".txt", ".md", ".json", ".xml", ".html", ".htm", ".csv", ".xlsx"}:
                continue
            raw = archive.read(name)
            try:
                if suffix == ".docx":
                    content = _extract_docx_bytes(raw)
                elif suffix == ".xlsx":
                    content = _extract_xlsx_bytes(raw)
                else:
                    content = _decode_text_bytes(raw)
            except (KeyError, ValueError, zipfile.BadZipFile, ElementTree.ParseError):
                content = ""
            if not content.strip():
                continue
            sections.append(f"\n\n--- ZIP_ENTRY: {name} ---\n{content.strip()}")
    return "\n".join(sections)


def _extract_xlsx_bytes(raw: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        shared_strings = _extract_xlsx_shared_strings(archive)
        parts = []
        for name in sorted(archive.namelist()):
            if not re.fullmatch(r"xl/worksheets/sheet\d+\.xml", name):
                continue
            root = ElementTree.fromstring(archive.read(name))
            values = []
            for cell in root.findall(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}c"):
                value = cell.find("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v")
                if value is None or value.text is None:
                    inline = cell.find(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")
                    if inline is not None and inline.text:
                        values.append(inline.text)
                    continue
                if cell.attrib.get("t") == "s":
                    try:
                        values.append(shared_strings[int(value.text)])
                    except (ValueError, IndexError):
                        values.append(value.text)
                else:
                    values.append(value.text)
            if values:
                parts.append(f"[{name}]\n" + "\n".join(values))
        return "\n\n".join(parts)


def _extract_xlsx_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    try:
        raw = archive.read("xl/sharedStrings.xml")
    except KeyError:
        return []
    root = ElementTree.fromstring(raw)
    namespace = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    values = []
    for item in root.findall(f".//{namespace}si"):
        texts = [node.text or "" for node in item.findall(f".//{namespace}t")]
        values.append("".join(texts))
    return values


def _decode_text_bytes(raw: bytes) -> str:
    for encoding in ("utf-8", "cp949", "euc-kr"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")

