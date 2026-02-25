from __future__ import annotations

import json
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any

DOC_ORDER = ["Klage", "Klageerwiderung", "Replik", "Duplik", "Stellungnahme"]
DOC_ORDER_MAP = {name: idx for idx, name in enumerate(DOC_ORDER)}
NON_RESPONSE_LINK_TYPES = {"SUPPORTS"}


@dataclass
class MatrixCache:
    signature: tuple[int, int, int, str, float]
    payload: dict[str, Any]


_cache: MatrixCache | None = None


def _doc_sort_key(doc_type: str, date_value: str):
    return (DOC_ORDER_MAP.get(doc_type, 999), date_value or "")


def _short_title(value: str, limit: int = 90) -> str:
    value = (value or "").strip()
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"


def _json_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        return [str(x) for x in json.loads(raw) if x]
    except json.JSONDecodeError:
        return []


def _build_signature(conn, our_side: str, min_confidence: float):
    docs = conn.execute("SELECT COUNT(*) AS c, COALESCE(SUM(id), 0) AS sid FROM documents").fetchone()
    links = conn.execute(
        """SELECT COUNT(*) AS c,
                  COALESCE(SUM(id), 0) AS sid,
                  COALESCE(SUM(CASE status WHEN 'confirmed' THEN 7 WHEN 'proposed' THEN 3 ELSE 1 END), 0) AS status_sig
           FROM links"""
    ).fetchone()
    return (docs["c"], docs["sid"], links["c"] + links["sid"] + links["status_sig"], our_side, round(min_confidence, 3))


def build_matrix_payload(
    conn,
    min_confidence: float = 0.7,
    include_unanswered_only: bool = False,
    include_our_gaps_only: bool = False,
    issue_filter: str | None = None,
    link_type_filter: str | None = None,
    status_filter: str | None = None,
    our_side: str = "PLAINTIFF",
) -> dict[str, Any]:
    global _cache

    signature = _build_signature(conn, our_side=our_side, min_confidence=min_confidence)
    if _cache and _cache.signature == signature:
        base = _cache.payload
    else:
        args_rows = conn.execute(
            """SELECT a.id, a.side, a.title, d.doc_type, d.date, d.doc_id,
                      GROUP_CONCAT(DISTINCT sba.issues_json) AS issues_blob,
                      GROUP_CONCAT(DISTINCT sba.role) AS roles_blob
               FROM arguments a
               JOIN documents d ON d.id = a.document_id
               LEFT JOIN semantic_block_arguments sba_map ON sba_map.argument_id = a.id
               LEFT JOIN semantic_block_analysis sba ON sba.block_id = sba_map.block_id
               GROUP BY a.id
               ORDER BY d.date, a.id"""
        ).fetchall()

        links_rows = [
            dict(r)
            for r in conn.execute(
                "SELECT id, from_argument_id, to_argument_id, link_type, confidence, rationale_short, status FROM links"
            ).fetchall()
        ]

        arguments: dict[int, dict[str, Any]] = {}
        for row in args_rows:
            issues: set[str] = set()
            for blob in (row["issues_blob"] or "").split(","):
                issues.update(_json_list(blob))

            roles = [r for r in (row["roles_blob"] or "").split(",") if r]
            arguments[row["id"]] = {
                "id": row["id"],
                "side": row["side"],
                "title": row["title"],
                "short_title": _short_title(row["title"]),
                "doc_type": row["doc_type"],
                "date": row["date"],
                "doc_id": row["doc_id"],
                "issues": sorted(issues) or ["Ohne Issue"],
                "roles": sorted(set(roles)),
                "out_links": [],
                "in_links": [],
                "badges": [],
            }

        valid_response_links = []
        for link in links_rows:
            src = arguments.get(link["from_argument_id"])
            dst = arguments.get(link["to_argument_id"])
            if not src or not dst:
                continue

            if link["status"] != "rejected" and link["confidence"] >= min_confidence:
                src["out_links"].append({**link, "target_title": dst["short_title"], "target_side": dst["side"]})
                dst["in_links"].append({**link, "source_title": src["short_title"], "source_side": src["side"]})

            if link["status"] == "rejected" or link["confidence"] < min_confidence or link["link_type"] in NON_RESPONSE_LINK_TYPES:
                continue
            valid_response_links.append(link)

        links_by_argument: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for link in valid_response_links:
            links_by_argument[link["from_argument_id"]].append(link)
            links_by_argument[link["to_argument_id"]].append(link)

        placeholders = []
        for arg in arguments.values():
            has_later_opponent_response = False
            for link in links_by_argument.get(arg["id"], []):
                other_id = link["to_argument_id"] if link["from_argument_id"] == arg["id"] else link["from_argument_id"]
                other = arguments.get(other_id)
                if not other or other["side"] == arg["side"]:
                    continue
                if _doc_sort_key(other["doc_type"], other["date"]) > _doc_sort_key(arg["doc_type"], arg["date"]):
                    has_later_opponent_response = True
                    break

            if has_later_opponent_response:
                continue

            if arg["side"] == our_side:
                arg["badges"].append("Unbeantwortet durch Gegner")
                placeholders.append(
                    {
                        "id": f"gap-opp-{arg['id']}",
                        "type": "GAP_OPPONENT",
                        "side": "DEFENDANT" if our_side == "PLAINTIFF" else "PLAINTIFF",
                        "issues": arg["issues"],
                        "doc_type": arg["doc_type"],
                        "date": arg["date"],
                        "text": f"⚠ Keine Antwort im Schriftsatz nach {arg['doc_type']} gefunden",
                        "source_argument_id": arg["id"],
                    }
                )
            else:
                arg["badges"].append("Noch nicht beantwortet von uns")

        base = {
            "arguments": arguments,
            "placeholders": placeholders,
            "links": links_rows,
            "meta": {
                "min_confidence": min_confidence,
                "issues": sorted({issue for arg in arguments.values() for issue in arg["issues"]}),
                "link_types": sorted({r["link_type"] for r in links_rows}),
                "statuses": sorted({r["status"] for r in links_rows}),
            },
        }
        _cache = MatrixCache(signature=signature, payload=base)

    issues_map: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: {"plaintiff": [], "defendant": []})
    links_view = []

    for link in base["links"]:
        if link_type_filter and link["link_type"] != link_type_filter:
            continue
        if status_filter and link["status"] != status_filter:
            continue
        if link["confidence"] < min_confidence:
            continue
        if link["status"] == "rejected":
            continue
        links_view.append(link)

    for arg in base["arguments"].values():
        if issue_filter and issue_filter not in arg["issues"]:
            continue
        if include_unanswered_only and "Unbeantwortet durch Gegner" not in arg["badges"]:
            continue
        if include_our_gaps_only and "Noch nicht beantwortet von uns" not in arg["badges"]:
            continue
        if status_filter:
            if not any(l["status"] == status_filter for l in arg["out_links"] + arg["in_links"]):
                continue
        if link_type_filter:
            if not any(l["link_type"] == link_type_filter for l in arg["out_links"] + arg["in_links"]):
                continue

        for issue in arg["issues"]:
            if issue_filter and issue != issue_filter:
                continue
            bucket = "plaintiff" if arg["side"] == "PLAINTIFF" else "defendant"
            issues_map[issue][bucket].append(arg)

    for gap in base["placeholders"]:
        for issue in gap["issues"]:
            if issue_filter and issue != issue_filter:
                continue
            if include_our_gaps_only:
                continue
            bucket = "plaintiff" if gap["side"] == "PLAINTIFF" else "defendant"
            issues_map[issue][bucket].append(gap)

    rows = []
    for issue, pair in issues_map.items():
        pair["plaintiff"].sort(key=lambda x: _doc_sort_key(x.get("doc_type", ""), x.get("date", "")))
        pair["defendant"].sort(key=lambda x: _doc_sort_key(x.get("doc_type", ""), x.get("date", "")))
        rows.append({"issue": issue, **pair})

    rows.sort(key=lambda x: x["issue"])
    return {"rows": rows, "links": links_view, "meta": base["meta"]}


def build_thread_payload(conn, argument_id: int, min_confidence: float = 0.7) -> dict[str, Any]:
    args_rows = conn.execute(
        """SELECT a.id, a.side, a.title, d.doc_type, d.date, d.doc_id
           FROM arguments a JOIN documents d ON d.id = a.document_id"""
    ).fetchall()
    links_rows = conn.execute(
        "SELECT id, from_argument_id, to_argument_id, link_type, confidence, rationale_short, status FROM links"
    ).fetchall()
    args = {r["id"]: dict(r) for r in args_rows}
    if argument_id not in args:
        return {"sequence": []}

    graph = defaultdict(list)
    valid = []
    for r in links_rows:
        if r["status"] == "rejected" or r["confidence"] < min_confidence:
            continue
        d = dict(r)
        valid.append(d)
        graph[d["from_argument_id"]].append(d)
        graph[d["to_argument_id"]].append(d)

    visited = set()
    q = deque([argument_id])
    component = set()
    while q:
        cur = q.popleft()
        if cur in visited:
            continue
        visited.add(cur)
        component.add(cur)
        for link in graph.get(cur, []):
            nxt = link["to_argument_id"] if link["from_argument_id"] == cur else link["from_argument_id"]
            if nxt not in visited:
                q.append(nxt)

    ordered = sorted([args[i] for i in component], key=lambda x: (_doc_sort_key(x["doc_type"], x["date"]), x["id"]))
    links_component = [l for l in valid if l["from_argument_id"] in component and l["to_argument_id"] in component]

    sequence = []
    for idx, item in enumerate(ordered):
        sequence.append({"type": "argument", **item})
        future_links = []
        for link in links_component:
            if item["id"] not in (link["from_argument_id"], link["to_argument_id"]):
                continue
            other_id = link["to_argument_id"] if link["from_argument_id"] == item["id"] else link["from_argument_id"]
            other = args.get(other_id)
            if not other:
                continue
            if _doc_sort_key(other["doc_type"], other["date"]) > _doc_sort_key(item["doc_type"], item["date"]):
                future_links.append(link)

        if future_links:
            for link in future_links:
                sequence.append(
                    {
                        "type": "link",
                        "link_type": link["link_type"],
                        "rationale_short": link["rationale_short"],
                        "confidence": link["confidence"],
                    }
                )
        elif idx < len(ordered) - 1:
            sequence.append({"type": "gap", "text": "⚠ GAP: Keine weitere Erwiderung"})

    return {"sequence": sequence, "root_argument_id": argument_id}
