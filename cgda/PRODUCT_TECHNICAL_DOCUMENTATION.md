## CGDA (Citizen’s Grievance Data Analytics) — Product & Technical Documentation

### Document control
- **Product**: CGDA Portal (Municipal Governance Analytics)
- **Audience**: Commissioner / Admin stakeholders, Data/IT team, Developers, DevOps
- **Scope**: Local-first deployment (Docker), Excel/CSV preprocessing + AI enrichment + analytics dashboards
- **Non-goals**: No production-grade cloud deployment, no DNS/Zero Trust setup, no real-time AI in UI for analytics

---

## 1) Product overview (what CGDA is)

CGDA is a **read-only analytics portal** for a Municipal Corporation grievance system (NMMC / IES). It does not create/close/update grievances. It provides:

- **Executive Overview**: totals, trend charts, top categories/sub-topics, status breakdown (where available)
- **Issue Intelligence (Tier 1)**: sub-topic analytics (overall, by ward, by department, trends)
- **Predictive Analytics (trend-based)**: rising issues, at-risk wards, chronic issues, with optional AI explanations
- **Operational tabs**: Upload & Enrich (run pipeline), Evidence, Citizen Feedback, Closure Analytics (as present in UI)

Wherever AI-derived labels/insights are displayed, the UI shows **“Powered by caseA”**.

---

## 2) Core user journeys

### A) Quick-start demo workflow (recommended)
- Admin places the newest grievance export in `data/raw/` (CSV or XLSX).
- Backend preprocesses and stores query-ready records into SQLite.
- Admin runs enrichment to populate AI labels (category/sub-topic/confidence) into SQLite checkpoints and processed tables.
- Leadership uses dashboards with date filters and “GO” to query instantly (no AI calls during filtering).

### B) Remote demo workflow (temporary)
- Run reverse proxy on `localhost:8080`.
- Expose via Cloudflare Quick Tunnel and share one HTTPS URL.

---

## 3) System architecture

### High-level components
- **Frontend**: React (Vite) + Tailwind + Recharts
- **Backend**: FastAPI (Python)
- **DB**: SQLite (local)
- **AI Provider**: Google Gemini (via HTTP API) — batch classification only

### Runtime topology (local)
- Frontend container (Nginx serving built assets) on `:3000`
- Backend container (Uvicorn) on `:8000`
- Optional demo Nginx reverse proxy container on `:8080` (tunnel origin)

### Demo HTTPS (single URL)
Public HTTPS URL
→ Cloudflare Tunnel (Quick Tunnel)
→ local Nginx reverse proxy on `localhost:8080`
→ `/` routes to frontend (3000)
→ `/api/*` routes to backend (8000)

---

## 4) Data model & datasets

### 4.1 Raw input format (NMMC/IES Excel/CSV)
The incoming file must contain **exactly** these headers:

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

### 4.2 Immutability (non-negotiable)
- **Raw input is never modified.**
- All downstream artifacts are written to `data/processed/` and/or SQLite tables.

### 4.3 Derived (preprocessed) dataset
Preprocessing normalizes the raw file into a dataset suitable for fast analytics and consistent enrichment:

- Converts `Created Date` into a real datetime
- Creates derived fields:
  - `created_date` (YYYY-MM-DD)
  - `created_month` (YYYY-MM)
  - `created_week` (ISO week: YYYY-Www)
- Cleans text fields for safe processing and analytics

### 4.4 AI-enriched dataset (labels)
AI enrichment adds these conceptual fields (stored in checkpoints and synced to processed tables):

- `ai_category` (closed-world category list)
- `ai_subtopic` (2–4 word standardized label)
- `ai_confidence` (High | Medium | Low)
- `ai_model`, timestamps, and error fields (in checkpoint/run tracking)

---

## 5) Pipeline: Excel/CSV → Preprocess → AI Enrichment → Analytics

### 5.1 Directory structure
- `cgda/data/raw/`: raw Excel/CSV exports (immutable)
- `cgda/data/processed/`: generated artifacts (CSV downloads, preprocessed file)
- `cgda/data/runs/`: run logs/status artifacts (if enabled by pipeline)

### 5.2 Stage 1 — Raw detection
Backend detects the newest `.xlsx` or `.csv` in `data/raw/`.

### 5.3 Stage 2 — Preprocessing (idempotent)
Endpoint triggers preprocessing and stores results into SQLite:

- Target table: `grievances_processed`
- Tracking table: `preprocess_runs`
- Idempotency: if raw filename + mtime unchanged and last run completed, backend can return previous run.

### 5.4 Stage 3 — Enrichment (batch, resumable)
Enrichment uses a checkpointing approach:

- Stable key per record: **`grievance_key = str(Grievance Id).strip()`**
- AI input text: **Subject + newline + Description**
- PII minimization: **Never send Mobile No or User Name** to Gemini
- Hashing: `ai_input_hash` allows “skip if unchanged”

Resume behavior:
- If checkpoint exists with same `ai_input_hash` and no prior error: **skip**
- If input hash changed or prior error exists: **reprocess**

Batch behavior:
- Processes in configurable batch sizes (default ~10 per request)
- Retries and fallback model handling
- Individual record failures are captured in `ai_error` and processing continues

### 5.5 Outputs (always produced)
- A downloadable enriched CSV under `data/processed/` (used for exports)
- SQLite tables updated for dashboards (so UI uses stored AI labels)

---

## 6) AI classification design

### 6.1 Prompting strategy (two-step labeling)
1) **Category**: choose **one** from a closed list:
   - Solid Waste Management
   - Roads & Footpaths
   - Water Supply
   - Sewerage & Drainage
   - Street Lighting
   - Public Health & Sanitation
   - Encroachment & Illegal Construction
   - Property Tax & Revenue
   - Parks & Public Spaces
   - Traffic & Transport
   - Noise / Nuisance
   - Animal Control
   - Other Civic Issues

2) **Sub-Topic**: short standardized noun phrase:
- 2–4 words preferred
- Title Case
- No locations, no names, no “complaint/request/regarding”
- If unclear: `General Civic Issue` with Low confidence

Prompt files:
- `backend/prompts/category_prompt.txt`
- `backend/prompts/subtopic_prompt.txt`
- `backend/prompts/predictive_explain_prompt.txt` (for explanation only)

### 6.2 Model configuration
Configured via `.env` (no code changes needed):
- `GEMINI_API_KEY` (required for Gemini)
- `GEMINI_MODEL_PRIMARY` (default `gemini-3-pro`)
- `GEMINI_MODEL_FALLBACK` (default `gemini-3-flash`)
- `GEMINI_TEMPERATURE` (default `0.1`)
- `GEMINI_MAX_OUTPUT_TOKENS` (default `256`)

### 6.3 Safety & validation
- Enforced JSON-only outputs
- Category validation: invalid → `Other Civic Issues`
- Sub-topic validation: empty/invalid → `General Civic Issue`
- No PII fields included in AI prompts

### 6.4 Token usage tracking (cost observability)
Run summaries can store `usageMetadata`-derived token counts:
- Prompt tokens
- Output tokens
- Total tokens
- Model breakdown

Note: Convert token usage to currency using Google’s published Gemini pricing for your selected model/version.

---

## 7) Analytics design (fast, SQL-based, filterable)

### 7.1 Non-negotiable rule
**Date filtering + dashboards must not call Gemini.**  
All analytics endpoints read from stored results (SQLite).

### 7.2 Filter contract (frontend)
Filters are edited as “draft” and only applied on **GO**:
- `start_date` (YYYY-MM-DD)
- `end_date` (YYYY-MM-DD)
- optional: `wards` (multi-select)
- optional: `department`
- optional: `category` / `ai_category` (depending on endpoint contract)

### 7.3 Processed analytics table
All date-range analytics query `grievances_processed`, which includes:
- grievance_id, created_at/date/month/week
- ward_name, department_name, status
- subject/description/closing_remark (text)
- ai_category, ai_subtopic, ai_confidence

Indexing strategy (SQLite):
- `created_date`, `ward_name`, `department_name`, `ai_category`, `ai_subtopic`

### 7.4 Issue Intelligence metrics
- Top sub-topics overall (Top 10)
- Top sub-topics by ward (Top 5)
- Top sub-topics by department (Top 10)
- Monthly trend per sub-topic
- One-of-a-kind complaints (sub-topics with count == 1)

### 7.5 Predictive analytics (trend-based, not forecasting)
Rule-based signals using SQL aggregation:
- Rising sub-topics: recent window vs previous window
- At-risk wards: volume + diversity + repetition density
- Chronic issues: appears in top N across multiple periods

Gemini is allowed only for `/predictive/explain` to generate short narrative explanations of already-computed metrics.

---

## 8) API surface (overview)

### Auth
- `POST /api/auth/login` → JWT token

### Data pipeline
- `GET /api/data/latest` (latest raw file detection, if present)
- `POST /api/data/preprocess` (preprocess latest raw into `grievances_processed`)
- `GET /api/data/preprocess/status` (last preprocess run status)
- `POST /api/data/ingest` (start enrichment run; background)
- `GET /api/data/runs` / `GET /api/data/runs/{run_id}` (run status/progress)
- `GET /api/data/enriched/download` (download enriched CSV)

### Analytics (SQL-only; no Gemini during filtering)
- `GET /api/analytics/executive-overview`
- `GET /api/analytics/top-subtopics`
- `GET /api/analytics/top-subtopics/by-ward`
- `GET /api/analytics/top-subtopics/by-department`
- `GET /api/analytics/subtopic-trend`
- `GET /api/analytics/one-of-a-kind` (unique subtopics)
- `GET /api/analytics/dimensions_processed` (filter dropdowns)

### Predictive (Gemini only for explain)
- `GET /api/analytics/predictive/*` (SQL-based signals)
- `POST /api/analytics/predictive/explain` (Gemini narrative only)

---

## 9) Frontend architecture & UX rules

### 9.1 Pages and routing
Key pages:
- Executive Overview
- Issue Intelligence
- Predictive Analytics
- Upload & Enrich
- Evidence / Feedback / Closure (as present)

### 9.2 UI design system
Frontend uses a small design system for consistency:
- `components/ui/Card`
- `components/ui/Button`
- `components/ui/Badge`
- `components/ui/Skeleton`

### 9.3 Chart system
Charts are built on Recharts with:
- premium tooltips
- rounded bars
- subtle grid styling
- consistent deterministic color mapping
- skeleton loading and empty states

### 9.4 Branding rule
Show **Powered by caseA** only on:
- AI categories/sub-topics charts
- AI-generated insights (predictive explain)

Do not show the badge on pure raw counts that are not AI-derived.

---

## 10) Security & privacy

### 10.1 PII minimization
For AI calls:
- Only `Complaint Subject` + `Complaint Description` are sent
- Do not send: Mobile numbers, user names, location fields

### 10.2 Auth model
- JWT-based login
- Role-based access enforced by backend routes

### 10.3 Storage
- SQLite stores processed records and AI checkpoint outputs locally
- `.env` contains API keys; do not commit secrets

---

## 11) Local run & operations (Docker)

### 11.1 Run stack
From `cgda/`:

```bash
docker-compose -f deployment/docker-compose.yml up --build
```

Open:
- Frontend: `http://localhost:3000`
- Backend: `http://localhost:8000` (health: `/healthz`)

### 11.2 Clean rebuild

```bash
docker compose -f deployment/docker-compose.yml down -v --remove-orphans
docker compose -f deployment/docker-compose.yml build --no-cache
docker compose -f deployment/docker-compose.yml up -d
```

---

## 12) Temporary public demo URL (Cloudflare Quick Tunnel)

### 12.1 Start local reverse proxy (demo only)
From repo root (where `nginx.conf` exists):

```bash
docker run --rm -p 8080:8080 \
  -v "$PWD/nginx.conf":/etc/nginx/nginx.conf:ro nginx:alpine
```

### 12.2 Start tunnel

```bash
cloudflared tunnel --url http://localhost:8080
```

Share the `https://*.trycloudflare.com` URL and keep the tunnel terminal running.

---

## 13) Troubleshooting playbook

### “Dashboards show ‘Unknown’ / ‘General Civic Issue’ everywhere”
Likely causes:
- Enrichment not run for the full dataset
- Checkpoints exist from a quota failure and resume is skipping them

Fix:
- Run enrichment with force reprocess for errored/unknown rows (admin endpoint parameter if enabled)
- Verify `GEMINI_API_KEY` and quota status

### “Backend returns 429 / quota exceeded”
- Confirm Gemini quota/billing
- Reduce batch size or increase backoff
- Use fallback model

### “Remote users see API errors”
- Ensure frontend uses relative `/api/*` behind the tunnel
- Ensure reverse proxy routes `/api/` to backend

### “SQLite is locked”
- Avoid multiple concurrent writers
- Prefer a single enrichment run at a time

---

## 14) Appendix — Key repo paths

- Frontend:
  - `frontend/src/pages/*`
  - `frontend/src/components/*`
  - `frontend/src/components/ui/*`
  - `frontend/src/services/api.js`
- Backend:
  - `backend/main.py`
  - `backend/routes/*`
  - `backend/services/*`
  - `backend/prompts/*`
- Data:
  - `data/raw/`
  - `data/processed/`
  - `data/runs/`


