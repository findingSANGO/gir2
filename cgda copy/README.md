# Citizen’s Grievance Data Analytics (CGDA) – Full Stack MVP

CGDA is a **read-only analytics portal** for a Municipal Corporation. It **does not manage grievances** and **does not change workflows**. It sits above an existing grievance system and focuses on:

- Executive analytics (RAG indicators)
- Issue intelligence (category, ward heat, trends)
- Citizen feedback analytics (1-star drivers + explainable AI summary)
- Closure time analytics (delay buckets + insights)
- Predictive view (rule-based spike/risk signals)

## Tech Stack

- **Frontend**: React (Vite), Tailwind CSS, Recharts
- **Backend**: FastAPI (Python), REST APIs, JWT auth
- **AI**: Google Gemini API (prompt-based), with an offline heuristic fallback for local/demo runs
- **Data**: CSV upload + SQLite (designed to swap to upstream API later)
- **DevOps**: Docker + docker-compose, `.env` configuration

## Local Run (Docker)

1) Copy environment file:

- Create a `.env` file at the repo root (`cgda/.env`) based on `.env.example`.
- **Optional**: set `GEMINI_API_KEY` for Gemini-powered analysis. If empty, the backend will use an explainable heuristic fallback.

2) Start:

```bash
cd cgda
docker-compose -f deployment/docker-compose.yml up --build
```

3) Open:

- Frontend: `http://localhost:3000`
- Backend: `http://localhost:8000` (health: `/healthz`)

## Clean rebuild (if localhost is down / ports not responding)

```bash
cd cgda
docker compose -f deployment/docker-compose.yml down -v --remove-orphans
docker compose -f deployment/docker-compose.yml build --no-cache
docker compose -f deployment/docker-compose.yml up -d
```

Then verify:

```bash
curl http://localhost:8000/healthz
```

## Demo Credentials (MVP)

- **Commissioner**: `commissioner / commissioner123`
- **Admin**: `admin / admin123`

> Change credentials via `.env` in production environments.

## Data Flow (As Implemented)

CGDA supports two ingestion paths:

### A) NMMC/IES Excel/CSV → AI Enrichment → Analytics-ready dataset (Recommended)

1) **Place your original NMMC/IES export** (CSV or XLSX) into `data/raw/` (**raw is never modified**)
2) Open the UI: **Upload & Enrich**
3) Click **Run Enrichment**
4) Backend writes a preprocessing artifact:
   - `data/processed/preprocessed_latest.csv` (this is the ONLY input used for downstream steps)
5) Backend processes in **batches of 10**, with retries/backoff and **free-tier Gemini fallback**
6) Enriched outputs are written to:
   - `data/processed/grievances_enriched.csv` (always regenerated safely)
   - SQLite checkpoints: `enrichment_checkpoints` (resume support)
   - SQLite runs: `enrichment_runs` (status/progress)
7) Dashboards read AI labels via the existing `grievances_structured` table (synced from checkpoints)

PII safeguards:
- Only **Complaint Subject + Complaint Description** are sent to Gemini.
- **Mobile No / User Names** are never included in prompts.

### B) Legacy CSV upload (MVP demo)

1) Upload CSV dump (raw file stored under `data/raw/`)
2) Raw rows stored into SQLite (`grievances_raw`)
3) Text sent to Gemini → structured labels stored (`grievances_structured`)
4) Analytics computed and served via REST endpoints to dashboards

## NMMC/IES Input Format (Exact Columns)

The NMMC/IES Excel/CSV must have these headers (exact):

- `Grievance Id`
- `Created Date`
- `Reported by User Name`
- `Mobile No.`
- `Complaint Location`
- `Complaint Subject`
- `Complaint Description`
- `Current Status`
- `Current Department Name`
- `Ward Name`
- `Current User Name`
- `Closing Remark`

## Legacy CSV Format

Required columns (case-insensitive):

- `grievance_id`
- `grievance_text`

Supported optional columns:

- `lodged_date`, `closed_date` (formats: `YYYY-MM-DD`, `DD-MM-YYYY`, `DD/MM/YYYY`)
- `ward`
- `department`
- `citizen_feedback_rating`

## API (Basic)

### Auth
- `POST /api/auth/login`

Body:
```json
{ "username": "admin", "password": "admin123" }
```

### Grievances
- `POST /api/grievances/upload` (multipart form-data: `file=...`, query: `process_now=true|false`)
- `POST /api/grievances/process?limit=500`
- `GET /api/grievances?limit=50&offset=0`

### Analytics
- `GET /api/analytics/retrospective`
- `GET /api/analytics/inferential`
- `GET /api/analytics/feedback`
- `GET /api/analytics/closure`
- `GET /api/analytics/predictive`

### NMMC/IES Enrichment (Drop file into `data/raw/`)
- `GET /api/data/latest`
- `POST /api/data/ingest` → returns `{ run_id }`
- `GET /api/data/runs`
- `GET /api/data/runs/{run_id}`
- `GET /api/data/enriched/download` → downloads `grievances_enriched.csv`
- `GET /api/data/preprocessed/download` → downloads `preprocessed_latest.csv`
- `GET /api/data/results?limit=50&offset=0` → UI table rows with AI subcategories

## Where to see subcategories (localhost)

- **Main results view (per record)**: `http://localhost:3000/upload-enrich` → section **“Main results (Subcategories)”**
- **Overview analytics (subcategories)**: `http://localhost:3000/` → **Subcategory distribution** chart + **Top subcategories** table

## Auto-preload (process first 100 records)

On localhost startup, the backend will automatically:
- detect the newest file in `data/raw/`
- write `data/processed/preprocessed_latest.csv`
- enrich up to **100** rows in background (or fewer if the file has <100)

Environment toggles:
- `AUTO_PRELOAD_ON_STARTUP=true|false` (default: true)
- `AUTO_PRELOAD_LIMIT=100` (default: 100)

## Notes on Explainability (Government-safe)

- AI outputs are stored as **simple labels** (category, sub-issue, sentiment, severity, repeat likelihood, delay risk) plus a **plain-language rationale**.
- Prompts are stored in `backend/prompts/` and are intentionally simple and editable.
- Wherever AI-derived labels/insights are shown, the UI displays **Powered by caseA** (with tiny **CaseA.ai**).


