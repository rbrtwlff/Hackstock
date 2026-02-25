from app.db import db_conn, init_db


def test_upsert_documents(tmp_path):
    db = tmp_path / "case.sqlite"
    init_db(str(db))
    with db_conn(str(db)) as conn:
        conn.execute("INSERT INTO documents(doc_id,side,doc_type,date,filename,content_hash) VALUES(?,?,?,?,?,?)", ("D1", "PLAINTIFF", "Klage", "2024-01-01", "a.docx", "h1"))
        conn.execute("""INSERT INTO documents(doc_id,side,doc_type,date,filename,content_hash)
            VALUES(?,?,?,?,?,?) ON CONFLICT(doc_id) DO UPDATE SET filename=excluded.filename""", ("D1", "PLAINTIFF", "Klage", "2024-01-01", "b.docx", "h2"))
        row = conn.execute("SELECT filename FROM documents WHERE doc_id='D1'").fetchone()
        assert row[0] == "b.docx"


def test_removed_lines_table_and_document_counts_exist(tmp_path):
    db = tmp_path / "case.sqlite"
    init_db(str(db))
    with db_conn(str(db)) as conn:
        doc_cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
        assert "removed_lines_count" in doc_cols
        assert "kept_account_headings_count" in doc_cols
        row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='removed_lines'").fetchone()
        assert row is not None
