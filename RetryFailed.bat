@echo off
cd /d %~dp0
call .venv\Scripts\activate
python -c "from app.main import pipeline; pipeline.retry_failed()"
