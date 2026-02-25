from __future__ import annotations

import json
import logging
import threading
import time
import uuid
import webbrowser
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.config import load_config
from app.db import db_conn, init_db
from app.matrix_view import build_matrix_payload, build_thread_payload
from app.pipeline import Pipeline

config = load_config()
init_db(config.db_path)
pipeline = Pipeline(config)

app = FastAPI(title="Nebenkosten Viewer")
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")
logger = logging.getLogger("app.main")


job_lock = threading.Lock()
job_state = {
    "running": False,
    "job_id": None,
    "phase": "idle",
    "docs_total": 0,
    "docs_done": 0,
    "blocks_total": 0,
    "blocks_done": 0,
    "llm_done": 0,
    "failed": 0,
    "last_error": None,
    "started_at": None,
}


def _set_job_state(**updates):
    with job_lock:
        job_state.update(updates)


def _snapshot_job_state() -> dict:
    with job_lock:
        return dict(job_state)


def _refresh_totals() -> None:
    with db_conn(config.db_path) as conn:
        docs_total = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        blocks_total = conn.execute(
            """SELECT COUNT(*) FROM semantic_blocks
            WHERE block_type IN ('BODY','BODY_WITH_QUOTE','QUOTE_BLOCK')"""
        ).fetchone()[0]
    _set_job_state(docs_total=docs_total, docs_done=docs_total, blocks_total=blocks_total)


def _progress_callback(event_type: str, payload: dict):
    if event_type == "block_done":
        blocks_done = payload.get("blocks_done", 0)
        failed = payload.get("failed", 0)
        _set_job_state(blocks_done=blocks_done, llm_done=blocks_done, failed=failed)
    if event_type == "block_log":
        logger.info(
            "RUN ALL progress: %s/%s blocks processed (%s failed)",
            payload.get("blocks_done", 0),
            payload.get("blocks_total", 0),
            payload.get("failed", 0),
        )


def run_pipeline_job() -> None:
    try:
        _set_job_state(phase="importing")
        pipeline.import_documents()
        _refresh_totals()

        _set_job_state(phase="normalizing")
        pipeline.normalize_documents()

        _set_job_state(phase="analyzing")
        import asyncio

        asyncio.run(pipeline.analyze_semantic_blocks(progress_callback=_progress_callback))

        _set_job_state(phase="building_arguments")
        pipeline.build_arguments()

        _set_job_state(phase="linking")
        asyncio.run(pipeline.propose_links())

        _set_job_state(phase="done")
    except Exception as exc:
        logger.exception("RUN ALL job failed")
        _set_job_state(phase="error", last_error=repr(exc))
    finally:
        _set_job_state(running=False)


class LinkUpdate(BaseModel):
    status: str


class LinkCreate(BaseModel):
    from_argument_id: int
    to_argument_id: int
    link_type: str
    confidence: float
    rationale_short: str


@app.get("/", response_class=HTMLResponse)
def index():
    return (Path(__file__).parent / "templates" / "index.html").read_text(encoding="utf-8")


@app.post("/api/run-all")
def run_all():
    with job_lock:
        if job_state["running"]:
            return JSONResponse(
                status_code=409,
                content={
                    "started": False,
                    "running": True,
                    "job_id": job_state["job_id"],
                    "error": "already running",
                },
            )
        job_state.update(
            {
                "running": True,
                "job_id": str(uuid.uuid4()),
                "phase": "starting",
                "docs_total": 0,
                "docs_done": 0,
                "blocks_total": 0,
                "blocks_done": 0,
                "llm_done": 0,
                "failed": 0,
                "last_error": None,
                "started_at": int(time.time()),
            }
        )
        current_job_id = job_state["job_id"]

    threading.Thread(target=run_pipeline_job, daemon=True).start()
    return {"started": True, "job_id": current_job_id, "running": True}


@app.get("/api/status")
def status():
    return _snapshot_job_state()


@app.post("/api/retry-failed")
def retry_failed():
    pipeline.retry_failed()
    return {"ok": True}


@app.get("/api/paragraphs")
def get_paragraphs(doc_id: str | None = None, side: str | None = None, issue: str | None = None, role: str | None = None, q: str | None = None, include_rubrum: bool = False, include_noise: bool = False):
    with db_conn(config.db_path) as conn:
        sql = """SELECT d.doc_id,d.side,d.raw_paragraph_count,d.semantic_block_count,d.removed_lines_count,d.kept_account_headings_count,sb.id,sb.block_type,sb.text_original,sb.intro_text,sb.quote_text,sb.hierarchy_path,sba.role,sba.summary_3_sentences,sba.keywords_json,sba.issues_json
        FROM semantic_blocks sb JOIN documents d ON sb.document_id=d.id
        LEFT JOIN semantic_block_analysis sba ON sba.block_id=sb.id WHERE 1=1"""
        params = []
        if doc_id:
            sql += " AND d.doc_id=?"
            params.append(doc_id)
        if side:
            sql += " AND d.side=?"
            params.append(side)
        if role:
            sql += " AND sba.role=?"
            params.append(role)
        if not include_rubrum:
            sql += " AND sb.block_type != 'RUBRUM_META'"
        rows = conn.execute(sql + " ORDER BY d.date,sb.block_index", tuple(params)).fetchall()
        out = []
        for r in rows:
            issues_json = r["issues_json"] or "[]"
            if issue and issue not in json.loads(issues_json):
                continue
            text_value = r["text_original"] or ""
            if q and q.lower() not in text_value.lower() and q.lower() not in (r["summary_3_sentences"] or "").lower():
                continue
            out.append(
                {
                    "doc_id": r["doc_id"],
                    "side": r["side"],
                    "id": r["id"],
                    "block_type": r["block_type"],
                    "text": text_value,
                    "intro_text": r["intro_text"],
                    "quote_text": r["quote_text"],
                    "hierarchy_path": r["hierarchy_path"],
                    "role": r["role"],
                    "summary": r["summary_3_sentences"],
                    "keywords": json.loads(r["keywords_json"] or "[]"),
                    "issues": json.loads(issues_json),
                    "raw_paragraph_count": r["raw_paragraph_count"],
                    "semantic_block_count": r["semantic_block_count"],
                    "removed_lines_count": r["removed_lines_count"],
                    "kept_account_headings_count": r["kept_account_headings_count"],
                }
            )
        return out


@app.get("/api/removed-lines")
def get_removed_lines(doc_id: str | None = None, reason: str | None = None, q: str | None = None):
    with db_conn(config.db_path) as conn:
        sql = "SELECT rl.*, d.side FROM removed_lines rl JOIN documents d ON rl.doc_id=d.doc_id WHERE 1=1"
        params = []
        if doc_id:
            sql += " AND rl.doc_id=?"
            params.append(doc_id)
        if reason:
            sql += " AND rl.reason=?"
            params.append(reason)
        if q:
            sql += " AND rl.text LIKE ?"
            params.append(f"%{q}%")
        rows = conn.execute(sql + " ORDER BY rl.created_at DESC, rl.id DESC", tuple(params)).fetchall()
        return [dict(r) for r in rows]


@app.get("/api/outline")
def outline():
    with db_conn(config.db_path) as conn:
        args = [dict(r) for r in conn.execute("SELECT * FROM arguments ORDER BY document_id,id").fetchall()]
        maps = [dict(r) for r in conn.execute("SELECT * FROM semantic_block_arguments").fetchall()]
        return {"arguments": args, "mapping": maps}


@app.get("/api/matrix")
def matrix(link_type: str | None = None, status: str | None = None, min_conf: float = 0.0):
    with db_conn(config.db_path) as conn:
        rows = conn.execute(
            """SELECT l.*,ap.title as from_title,ad.title as to_title
            FROM links l JOIN arguments ap ON l.from_argument_id=ap.id JOIN arguments ad ON l.to_argument_id=ad.id"""
        ).fetchall()
        out = []
        for r in rows:
            if link_type and r["link_type"] != link_type:
                continue
            if status and r["status"] != status:
                continue
            if r["confidence"] < min_conf:
                continue
            out.append(dict(r))
        return out


@app.get("/api/matrix-view")
def matrix_view(
    issue: str | None = None,
    link_type: str | None = None,
    status: str | None = None,
    min_conf: float = 0.7,
    unanswered_only: bool = False,
    our_gaps_only: bool = False,
    our_side: str = "PLAINTIFF",
):
    with db_conn(config.db_path) as conn:
        return build_matrix_payload(
            conn,
            min_confidence=min_conf,
            include_unanswered_only=unanswered_only,
            include_our_gaps_only=our_gaps_only,
            issue_filter=issue,
            link_type_filter=link_type,
            status_filter=status,
            our_side=our_side,
        )


@app.get("/api/thread/{argument_id}")
def thread(argument_id: int, min_conf: float = 0.7):
    with db_conn(config.db_path) as conn:
        return build_thread_payload(conn, argument_id=argument_id, min_confidence=min_conf)


@app.get("/api/tables")
def tables():
    with db_conn(config.db_path) as conn:
        rows = conn.execute(
            """SELECT d.doc_id,t.block_index,t.render_html FROM table_blocks t
            JOIN documents d ON t.document_id=d.id ORDER BY d.date,t.block_index"""
        ).fetchall()
        return [dict(r) for r in rows]


@app.patch("/api/links/{link_id}")
def update_link(link_id: int, payload: LinkUpdate):
    with db_conn(config.db_path) as conn:
        conn.execute("UPDATE links SET status=? WHERE id=?", (payload.status, link_id))
    return {"ok": True}


@app.delete("/api/links/{link_id}")
def delete_link(link_id: int):
    with db_conn(config.db_path) as conn:
        conn.execute("DELETE FROM links WHERE id=?", (link_id,))
    return {"ok": True}


@app.post("/api/links")
def create_link(payload: LinkCreate):
    with db_conn(config.db_path) as conn:
        conn.execute(
            """INSERT INTO links(from_argument_id,to_argument_id,link_type,confidence,rationale_short,status)
            VALUES(?,?,?,?,?, 'proposed')""",
            (payload.from_argument_id, payload.to_argument_id, payload.link_type, payload.confidence, payload.rationale_short),
        )
    return {"ok": True}


def open_browser():
    webbrowser.open(f"http://{config.host}:{config.port}")


if __name__ == "__main__":
    import uvicorn

    open_browser()
    uvicorn.run("app.main:app", host=config.host, port=config.port, reload=False)
