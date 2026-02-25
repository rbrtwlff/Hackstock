from __future__ import annotations

import json
import webbrowser
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.config import load_config
from app.db import db_conn, init_db
from app.pipeline import Pipeline

config = load_config()
init_db(config.db_path)
pipeline = Pipeline(config)

app = FastAPI(title="Nebenkosten Viewer")
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")


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
    pipeline.run_all()
    return {"ok": True}


@app.post("/api/retry-failed")
def retry_failed():
    pipeline.retry_failed()
    return {"ok": True}


@app.get("/api/paragraphs")
def get_paragraphs(doc_id: str | None = None, side: str | None = None, issue: str | None = None, role: str | None = None, q: str | None = None):
    with db_conn(config.db_path) as conn:
        sql = """SELECT d.doc_id,d.side,p.id,p.text,p.hierarchy_path,pa.role,pa.summary_3_sentences,pa.keywords_json,pa.issues_json
        FROM paragraphs p JOIN documents d ON p.document_id=d.id
        LEFT JOIN paragraph_analysis pa ON pa.paragraph_id=p.id WHERE 1=1"""
        params = []
        if doc_id:
            sql += " AND d.doc_id=?"
            params.append(doc_id)
        if side:
            sql += " AND d.side=?"
            params.append(side)
        if role:
            sql += " AND pa.role=?"
            params.append(role)
        rows = conn.execute(sql + " ORDER BY d.date,p.para_index", tuple(params)).fetchall()
        out = []
        for r in rows:
            issues_json = r["issues_json"] or "[]"
            if issue and issue not in json.loads(issues_json):
                continue
            if q and q.lower() not in (r["text"] or "").lower() and q.lower() not in (r["summary_3_sentences"] or "").lower():
                continue
            out.append(
                {
                    "doc_id": r["doc_id"],
                    "side": r["side"],
                    "id": r["id"],
                    "text": r["text"],
                    "hierarchy_path": r["hierarchy_path"],
                    "role": r["role"],
                    "summary": r["summary_3_sentences"],
                    "keywords": json.loads(r["keywords_json"] or "[]"),
                    "issues": json.loads(issues_json),
                }
            )
        return out


@app.get("/api/outline")
def outline():
    with db_conn(config.db_path) as conn:
        args = [dict(r) for r in conn.execute("SELECT * FROM arguments ORDER BY document_id,id").fetchall()]
        maps = [dict(r) for r in conn.execute("SELECT * FROM paragraph_arguments").fetchall()]
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
