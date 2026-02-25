from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import logging
from pathlib import Path

from docx import Document

from app.config import AppConfig
from app.db import db_conn
from app.llm import LLMClient
from app.parser import DOC_ORDER, build_hierarchy, file_hash, normalize_ocr_lines, should_ocr_normalize, table_to_json

logger = logging.getLogger("pipeline")


class Pipeline:
    def __init__(self, config: AppConfig):
        self.config = config
        self.llm = LLMClient(config)

    def run_all(self):
        self.import_documents()
        self.normalize_documents()
        asyncio.run(self.analyze_paragraphs())
        self.build_arguments()
        asyncio.run(self.propose_links())

    def retry_failed(self):
        asyncio.run(self.analyze_paragraphs(only_failed=True))
        asyncio.run(self.propose_links(only_failed=True))

    def import_documents(self):
        manifest = Path("data/manifest.csv")
        inbox = Path("data/inbox")
        rows = list(csv.DictReader(manifest.read_text(encoding="utf-8-sig").splitlines()))
        rows.sort(key=lambda r: (DOC_ORDER.index(r["doc_type"]), r["date"]))
        with db_conn(self.config.db_path) as conn:
            for row in rows:
                p = inbox / row["filename"]
                if not p.exists():
                    logger.error("Missing file %s", p)
                    continue
                ch = file_hash(p)
                conn.execute(
                    """INSERT INTO documents(doc_id,side,doc_type,date,filename,content_hash)
                    VALUES(?,?,?,?,?,?)
                    ON CONFLICT(doc_id) DO UPDATE SET side=excluded.side, doc_type=excluded.doc_type, date=excluded.date,
                    filename=excluded.filename, content_hash=excluded.content_hash""",
                    (row["doc_id"], row["side"], row["doc_type"], row["date"], row["filename"], ch),
                )
                doc_id = conn.execute("SELECT id FROM documents WHERE doc_id=?", (row["doc_id"],)).fetchone()[0]
                self._ingest_doc(conn, doc_id, p)

    def _ingest_doc(self, conn, document_id: int, path: Path):
        doc = Document(path)
        paras, styles = [], []
        for p in doc.paragraphs:
            txt = (p.text or "").strip()
            if not txt:
                continue
            paras.append(txt)
            styles.append(getattr(p.style, "name", "Normal"))
        parsed = build_hierarchy(paras, styles)
        for para in parsed:
            h = hashlib.sha256(f"{document_id}:{para.para_index}:{para.text}".encode()).hexdigest()
            conn.execute(
                """INSERT INTO paragraphs(document_id,para_index,text,style,is_heading,hierarchy_path,continuation_group,content_hash)
                VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(content_hash) DO UPDATE SET
                text=excluded.text, hierarchy_path=excluded.hierarchy_path, continuation_group=excluded.continuation_group""",
                (
                    document_id,
                    para.para_index,
                    para.text,
                    para.style,
                    int(para.is_heading),
                    para.hierarchy_path,
                    para.continuation_group,
                    h,
                ),
            )
            pid = conn.execute("SELECT id FROM paragraphs WHERE content_hash=?", (h,)).fetchone()[0]
            conn.execute("INSERT OR IGNORE INTO paragraph_jobs(paragraph_id,status) VALUES(?, 'PENDING')", (pid,))
        for idx, table in enumerate(doc.tables):
            cells_json, render_html = table_to_json(table)
            conn.execute(
                """INSERT INTO table_blocks(document_id,block_index,cells_json,render_html)
                VALUES(?,?,?,?)""",
                (document_id, idx, cells_json, render_html),
            )

    def normalize_documents(self):
        with db_conn(self.config.db_path) as conn:
            docs = conn.execute("SELECT id FROM documents").fetchall()
            for d in docs:
                rows = conn.execute("SELECT id,text FROM paragraphs WHERE document_id=? ORDER BY para_index", (d[0],)).fetchall()
                texts = [r["text"] for r in rows]
                trig = should_ocr_normalize(texts)
                if trig:
                    normalized = normalize_ocr_lines(texts)
                    for i, t in enumerate(normalized):
                        if i < len(rows):
                            conn.execute("UPDATE paragraphs SET text=? WHERE id=?", (t, rows[i]["id"]))
                conn.execute("UPDATE documents SET ocr_normalized=? WHERE id=?", (1 if trig else 0, d[0]))

    async def analyze_paragraphs(self, only_failed: bool = False):
        with db_conn(self.config.db_path) as conn:
            query = """SELECT p.id,p.text,p.hierarchy_path,pj.attempts
            FROM paragraphs p JOIN paragraph_jobs pj ON p.id=pj.paragraph_id
            WHERE pj.status IN ({})""".format("'FAILED'" if only_failed else "'PENDING','FAILED'")
            jobs = conn.execute(query).fetchall()
        for job in jobs:
            pid, text, hierarchy, attempts = job
            with db_conn(self.config.db_path) as conn:
                conn.execute("UPDATE paragraph_jobs SET status='RUNNING',attempts=attempts+1,updated_at=CURRENT_TIMESTAMP WHERE paragraph_id=?", (pid,))
            try:
                prev_next = self._neighbor_context(pid)
                result = await self.llm.analyze_paragraph(text, f"{hierarchy}\n{prev_next}")
                with db_conn(self.config.db_path) as conn:
                    canonical_issues = [self._canonicalize_issue(conn, x) for x in result.issues]
                    conn.execute(
                        """INSERT INTO paragraph_analysis(paragraph_id,keywords_json,issues_json,role,summary_3_sentences,
                        continuation_of_previous,continuation_reason,citations_norms_json,citations_cases_json,
                        citations_contract_json,citations_exhibits_json)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(paragraph_id) DO UPDATE SET keywords_json=excluded.keywords_json,issues_json=excluded.issues_json,
                        role=excluded.role,summary_3_sentences=excluded.summary_3_sentences,
                        continuation_of_previous=excluded.continuation_of_previous,continuation_reason=excluded.continuation_reason,
                        citations_norms_json=excluded.citations_norms_json,citations_cases_json=excluded.citations_cases_json,
                        citations_contract_json=excluded.citations_contract_json,citations_exhibits_json=excluded.citations_exhibits_json""",
                        (
                            pid,
                            json.dumps(result.keywords, ensure_ascii=False),
                            json.dumps(canonical_issues, ensure_ascii=False),
                            result.role.value,
                            result.summary_3_sentences,
                            int(result.continuation_of_previous),
                            result.continuation_reason,
                            json.dumps(result.citations_norms, ensure_ascii=False),
                            json.dumps(result.citations_cases, ensure_ascii=False),
                            json.dumps(result.citations_contract, ensure_ascii=False),
                            json.dumps(result.citations_exhibits, ensure_ascii=False),
                        ),
                    )
                    conn.execute("UPDATE paragraph_jobs SET status='DONE',last_error=NULL,updated_at=CURRENT_TIMESTAMP WHERE paragraph_id=?", (pid,))
            except Exception as exc:
                with db_conn(self.config.db_path) as conn:
                    status = "FAILED" if attempts + 1 >= 3 else "PENDING"
                    conn.execute("UPDATE paragraph_jobs SET status=?,last_error=?,updated_at=CURRENT_TIMESTAMP WHERE paragraph_id=?", (status, str(exc)[:500], pid))

    def _neighbor_context(self, pid: int) -> str:
        with db_conn(self.config.db_path) as conn:
            row = conn.execute("SELECT document_id,para_index FROM paragraphs WHERE id=?", (pid,)).fetchone()
            prev = conn.execute("SELECT text FROM paragraphs WHERE document_id=? AND para_index=?", (row[0], row[1] - 1)).fetchone()
            nxt = conn.execute("SELECT text FROM paragraphs WHERE document_id=? AND para_index=?", (row[0], row[1] + 1)).fetchone()
            return f"Vorher: {(prev['text'] if prev else '')}\nNachher: {(nxt['text'] if nxt else '')}"

    def _canonicalize_issue(self, conn, issue: str) -> str:
        norm = issue.strip().lower()
        rows = conn.execute("SELECT canonical,synonyms_json FROM issue_vocab").fetchall()
        for r in rows:
            canonical = r["canonical"]
            syns = json.loads(r["synonyms_json"])
            if norm == canonical.lower() or norm in [s.lower() for s in syns]:
                return canonical
            if canonical.lower() in norm or norm in canonical.lower():
                return canonical
        canonical = issue.strip()
        conn.execute("INSERT OR IGNORE INTO issue_vocab(canonical,synonyms_json) VALUES(?,?)", (canonical, json.dumps([], ensure_ascii=False)))
        return canonical

    def build_arguments(self):
        with db_conn(self.config.db_path) as conn:
            conn.execute("DELETE FROM paragraph_arguments")
            conn.execute("DELETE FROM arguments")
            rows = conn.execute(
                """SELECT p.id,p.document_id,d.side,p.hierarchy_path,p.continuation_group,p.text
                FROM paragraphs p JOIN documents d ON p.document_id=d.id
                ORDER BY p.document_id,p.para_index"""
            ).fetchall()
            cache = {}
            for r in rows:
                key = (r["document_id"], r["side"], r["hierarchy_path"], r["continuation_group"])
                if key not in cache:
                    title = f"{r['hierarchy_path']} / {r['continuation_group']}"
                    conn.execute("INSERT INTO arguments(document_id,side,title,hierarchy_path) VALUES(?,?,?,?)", (r["document_id"], r["side"], title, r["hierarchy_path"]))
                    cache[key] = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                conn.execute("INSERT OR IGNORE INTO paragraph_arguments(paragraph_id,argument_id) VALUES(?,?)", (r["id"], cache[key]))

    async def propose_links(self, only_failed: bool = False):
        with db_conn(self.config.db_path) as conn:
            plaintiff = conn.execute("SELECT a.id,a.title FROM arguments a WHERE side='PLAINTIFF'").fetchall()
            defendant = conn.execute("SELECT a.id,a.title FROM arguments a WHERE side='DEFENDANT'").fetchall()
            for p in plaintiff:
                for d in defendant:
                    conn.execute("INSERT OR IGNORE INTO links_jobs(from_argument_id,to_argument_id,status) VALUES(?,?,'PENDING')", (p[0], d[0]))
            status_filter = "('FAILED')" if only_failed else "('PENDING','FAILED')"
            jobs = conn.execute(f"SELECT id,from_argument_id,to_argument_id,attempts FROM links_jobs WHERE status IN {status_filter}").fetchall()
        for j in jobs:
            with db_conn(self.config.db_path) as conn:
                conn.execute("UPDATE links_jobs SET status='RUNNING',attempts=attempts+1 WHERE id=?", (j["id"],))
                p = conn.execute("SELECT title FROM arguments WHERE id=?", (j["from_argument_id"],)).fetchone()
                d = conn.execute("SELECT title FROM arguments WHERE id=?", (j["to_argument_id"],)).fetchone()
            try:
                proposal = await self.llm.classify_link(p[0], d[0])
                with db_conn(self.config.db_path) as conn:
                    conn.execute(
                        """INSERT INTO links(from_argument_id,to_argument_id,link_type,confidence,rationale_short,status)
                        VALUES(?,?,?,?,?,'proposed')""",
                        (j["from_argument_id"], j["to_argument_id"], proposal.link_type.value, proposal.confidence, proposal.rationale_short),
                    )
                    conn.execute("UPDATE links_jobs SET status='DONE',last_error=NULL WHERE id=?", (j["id"],))
            except Exception as exc:
                with db_conn(self.config.db_path) as conn:
                    status = "FAILED" if j["attempts"] + 1 >= 3 else "PENDING"
                    conn.execute("UPDATE links_jobs SET status=?,last_error=? WHERE id=?", (status, str(exc)[:500], j["id"]))
