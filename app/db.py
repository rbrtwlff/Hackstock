from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path


SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY,
    doc_id TEXT UNIQUE NOT NULL,
    side TEXT NOT NULL,
    doc_type TEXT NOT NULL,
    date TEXT NOT NULL,
    filename TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    ocr_normalized INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS table_blocks (
    id INTEGER PRIMARY KEY,
    document_id INTEGER NOT NULL,
    block_index INTEGER NOT NULL,
    cells_json TEXT NOT NULL,
    render_html TEXT NOT NULL,
    FOREIGN KEY(document_id) REFERENCES documents(id)
);
CREATE TABLE IF NOT EXISTS paragraphs (
    id INTEGER PRIMARY KEY,
    document_id INTEGER NOT NULL,
    para_index INTEGER NOT NULL,
    text TEXT NOT NULL,
    style TEXT,
    is_heading INTEGER DEFAULT 0,
    hierarchy_path TEXT,
    continuation_group TEXT,
    content_hash TEXT NOT NULL UNIQUE,
    FOREIGN KEY(document_id) REFERENCES documents(id)
);
CREATE TABLE IF NOT EXISTS paragraph_analysis (
    paragraph_id INTEGER PRIMARY KEY,
    keywords_json TEXT,
    issues_json TEXT,
    role TEXT,
    summary_3_sentences TEXT,
    continuation_of_previous INTEGER,
    continuation_reason TEXT,
    citations_norms_json TEXT,
    citations_cases_json TEXT,
    citations_contract_json TEXT,
    citations_exhibits_json TEXT,
    FOREIGN KEY(paragraph_id) REFERENCES paragraphs(id)
);
CREATE TABLE IF NOT EXISTS issue_vocab (
    id INTEGER PRIMARY KEY,
    canonical TEXT UNIQUE NOT NULL,
    synonyms_json TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS arguments (
    id INTEGER PRIMARY KEY,
    document_id INTEGER NOT NULL,
    side TEXT NOT NULL,
    title TEXT NOT NULL,
    parent_id INTEGER,
    hierarchy_path TEXT,
    FOREIGN KEY(document_id) REFERENCES documents(id)
);
CREATE TABLE IF NOT EXISTS paragraph_arguments (
    paragraph_id INTEGER NOT NULL,
    argument_id INTEGER NOT NULL,
    PRIMARY KEY (paragraph_id, argument_id),
    FOREIGN KEY(paragraph_id) REFERENCES paragraphs(id),
    FOREIGN KEY(argument_id) REFERENCES arguments(id)
);
CREATE TABLE IF NOT EXISTS links (
    id INTEGER PRIMARY KEY,
    from_argument_id INTEGER,
    to_argument_id INTEGER,
    from_paragraph_id INTEGER,
    to_paragraph_id INTEGER,
    link_type TEXT NOT NULL,
    confidence REAL NOT NULL,
    rationale_short TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'proposed'
);
CREATE TABLE IF NOT EXISTS paragraph_jobs (
    paragraph_id INTEGER PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS links_jobs (
    id INTEGER PRIMARY KEY,
    from_argument_id INTEGER NOT NULL,
    to_argument_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(from_argument_id, to_argument_id)
);
"""


@contextmanager
def db_conn(path: str):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with db_conn(path) as conn:
        conn.executescript(SCHEMA)
