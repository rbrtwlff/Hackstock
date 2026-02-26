import time

from fastapi.testclient import TestClient

from app.main import app, job_lock, job_state



def test_run_all_returns_fast_and_sets_running(monkeypatch):
    def fake_job():
        time.sleep(0.5)
        with job_lock:
            job_state["phase"] = "done"
            job_state["running"] = False

    with job_lock:
        job_state.update(
            {
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
                "last_llm_status_code": None,
                "last_llm_response_excerpt": None,
                "consecutive_llm_failures": 0,
                "started_at": None,
            }
        )

    monkeypatch.setattr("app.main.run_pipeline_job", fake_job)

    client = TestClient(app)
    start = time.perf_counter()
    response = client.post("/api/run-all")
    elapsed = time.perf_counter() - start

    assert response.status_code == 200
    assert elapsed < 0.2

    status_response = client.get("/api/status")
    assert status_response.status_code == 200
    assert status_response.json()["running"] is True
