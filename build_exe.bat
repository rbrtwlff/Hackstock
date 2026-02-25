@echo off
cd /d %~dp0
call .venv\Scripts\activate
pip install pyinstaller
pyinstaller --onefile -n NebenkostenViewer run_server.py
