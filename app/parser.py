from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

from docx import Document


DOC_ORDER = ["Klage", "Klageerwiderung", "Replik", "Duplik", "Stellungnahme"]
HEADING_RE = re.compile(r"^(?:[IVXLC]+\.?|\d+(?:\.\d+)*\.?|[a-zA-Z]\)|\(\d+\))\s+")


@dataclass
class ParsedParagraph:
    para_index: int
    text: str
    style: str
    is_heading: bool
    hierarchy_path: str
    continuation_group: str


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def table_to_json(table) -> tuple[str, str]:
    rows = []
    html_rows = []
    for row in table.rows:
        cells = [cell.text.strip() for cell in row.cells]
        rows.append(cells)
        html_rows.append("<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
    return json.dumps(rows, ensure_ascii=False), "<table>" + "".join(html_rows) + "</table>"


def is_heading(text: str, style_name: str) -> bool:
    t = text.strip()
    if not t:
        return False
    if style_name.lower().startswith("heading"):
        return True
    if len(t) <= 90 and (HEADING_RE.match(t) or t.isupper()):
        return True
    return False


def should_ocr_normalize(paragraphs: list[str]) -> bool:
    if not paragraphs:
        return False
    short = [p for p in paragraphs if 0 < len(p.strip()) <= 35]
    return (len(short) / max(len(paragraphs), 1)) > 0.55


def normalize_ocr_lines(paragraphs: list[str]) -> list[str]:
    result = []
    buffer = ""
    for p in paragraphs:
        text = p.strip()
        if not text:
            if buffer:
                result.append(buffer.strip())
                buffer = ""
            continue
        if buffer and buffer.endswith("-"):
            buffer = buffer[:-1] + text
        elif buffer and len(buffer) < 180 and not HEADING_RE.match(text):
            buffer += " " + text
        else:
            if buffer:
                result.append(buffer.strip())
            buffer = text
    if buffer:
        result.append(buffer.strip())
    return result


def parse_docx(path: Path) -> tuple[list[str], list[tuple[str, str]]]:
    doc = Document(path)
    paragraphs = [p.text for p in doc.paragraphs if p.text and p.text.strip()]
    tables = [table_to_json(t) for t in doc.tables]
    return paragraphs, tables


def build_hierarchy(paragraphs: list[str], styles: list[str]) -> list[ParsedParagraph]:
    stack: list[str] = []
    parsed: list[ParsedParagraph] = []
    group = 0
    for i, (text, style) in enumerate(zip(paragraphs, styles)):
        head = is_heading(text, style)
        if head:
            group += 1
            token = text.strip()[:70]
            stack = stack[:3]
            stack.append(token)
        hierarchy = " > ".join(stack) if stack else "ROOT"
        parsed.append(
            ParsedParagraph(
                para_index=i,
                text=text.strip(),
                style=style,
                is_heading=head,
                hierarchy_path=hierarchy,
                continuation_group=f"g{group or 1}",
            )
        )
    return parsed
