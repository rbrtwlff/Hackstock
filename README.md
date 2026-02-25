# Hackstock Viewer

Viewer ohne Build-Toolchain: statisches HTML/CSS/JS über FastAPI ausgeliefert.

## Start

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Dann im Browser `http://127.0.0.1:8000` öffnen.
