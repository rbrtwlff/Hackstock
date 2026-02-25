@echo off
cd /d %~dp0
if not exist .venv (
  py -3.11 -m venv .venv
)
call .venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python -c "from app.main import pipeline; pipeline.run_all()"
start http://127.0.0.1:8000
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
