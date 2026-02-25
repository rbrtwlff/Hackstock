# Nebenkosten-Analyse Viewer (Windows lokal, ohne Docker)

Dieses Projekt verarbeitet DOCX-Schriftsätze (Gewerberaummiete/Nebenkosten) lokal auf Windows:

1. Import aus `data/inbox` per `data/manifest.csv`
2. OCR-Normalisierung (konservativ)
3. Absatzanalyse mit Kimi (OpenAI-kompatibles API-Format)
4. Argumentbaum + Kläger/Beklagter-Linkmatrix
5. Viewer im Browser (Tabs: Search, Outline, Matrix, Tables)

## 1) Voraussetzungen

- Windows 10/11
- Python 3.11 installiert (`py -3.11 --version`)
- Internet für Kimi API

## 2) Einmalige Einrichtung

1. Öffnen Sie den Projektordner.
2. Doppelklick auf `Setup.bat`.
3. Tragen Sie API-Key in `config.yaml` ein (`api_key`) oder setzen Sie Umgebungsvariable `MOONSHOT_API_KEY`.

## 3) Daten vorbereiten

- DOCX-Dateien nach `data/inbox/` kopieren.
- `data/manifest.csv` pflegen mit:
  - `doc_id`
  - `side` (`PLAINTIFF` oder `DEFENDANT`)
  - `doc_type` (`Klage`, `Klageerwiderung`, `Replik`, `Duplik`, `Stellungnahme`)
  - `date` (`YYYY-MM-DD`)
  - `filename` (Dateiname in `data/inbox`)

## 4) RUN ALL starten

Doppelklick auf `Start.bat`.

Das Skript macht automatisch:
- Konfiguration laden (`config.yaml`, `manifest.csv`)
- Pipeline ausführen: `import -> normalize -> analyze paragraphs -> build arguments -> propose links`
- FastAPI-Server lokal starten auf `127.0.0.1:8000`
- Browser öffnen

## 5) Retry failed

- Doppelklick `RetryFailed.bat`
- oder im Viewer Button **Retry Failed**

Nur Jobs mit `FAILED` werden erneut versucht.

## 6) Viewer-Funktionen

- **Search**: Volltext + Summary durchsuchen, inkl. Rolle/Issues
- **Outline**: Argumentbaum + Absatz-Mapping
- **Matrix**: Links Kläger/Beklagter inkl. Status-Änderung/Löschen
- **Tables**: DOCX-Tabellen als separater Block

## 7) Robustheit / Verhalten

- SQLite (`case.sqlite`) speichert dauerhaft Zustand + Jobs.
- Job-Status: `PENDING`, `RUNNING`, `DONE`, `FAILED`.
- Retries mit Exponential Backoff (max. 3).
- Bei ungültigem LLM-JSON: 1 Repair-Call, dann Schema-Validierung.
- Bei Abbruch können Sie erneut `Start.bat` ausführen (restart-safe, idempotente Upserts).
- Server bindet standardmäßig an `127.0.0.1` (lokal-only).

## 8) Optional: EXE bauen

- Doppelklick `build_exe.bat`
- Ergebnis im `dist`-Ordner (`NebenkostenViewer.exe`)

## 9) Tests

```bash
pytest
```

## 10) Hinweise

- Standard-Logging ist zurückhaltend (`errors+metadata`), keine Volltexte im Log per Default.
- Für große Absätze werden Tokenbudget und Textkappung angewendet.
