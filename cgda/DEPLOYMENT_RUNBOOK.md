# CGDA Deployment Runbook (Data + IES Integration)

## Why your deployment guy “can’t see the data”

There are **two common causes**:

1) **Data is not in git**
- The app uses a local `data/` folder (`cgda/data/...`) for:
  - raw inputs (`raw2/`, `raw3/`, `raw4/`)
  - the SQLite DB (`processed/cgda.db`)
  - exports (`preprocess/`, `stage_data/`, `ai_outputs/`)
- In real deployments, this folder is usually **not committed** (too large / contains sensitive data).
- If the server has no `data/` folder, the backend will start with an empty DB and dashboards will look empty.

2) **Frontend calling the wrong API**
- The frontend bakes `VITE_API_BASE_URL` at build time.
- If it’s set to `http://localhost:8000` on a server build, any user visiting your app will call **their own laptop’s** `localhost:8000` and see “no data”.

## Required deploy inputs

You must provide these **outside git** (recommended):

- `cgda/data/processed/cgda.db` (fastest: includes processed + AI fields already)
  - also include `cgda.db-wal` and `cgda.db-shm` if present
- OR: at minimum provide the raw inputs so the backend can regenerate:
  - `cgda/data/raw2/Close Grievances (1).csv`
  - `cgda/data/raw3/extra_grievances.csv` (optional)
  - `cgda/data/raw4/NMMC - Grievance Data - 07.01.2026.xlsx` (optional)

## Docker compose expectations (data volume)

`deployment/docker-compose.yml` mounts:

- `../data:/app/data`

So on the server, the folder **must exist at**:

- `<repo>/cgda/data/`

## Server steps (copy data + start)

1) Copy data folder to the server (example using scp):

```bash
scp -r cgda/data user@server:/opt/cgda/cgda/
```

2) Build/start (from the server):

```bash
cd /opt/cgda/cgda
docker compose -f deployment/docker-compose.yml up -d --build
```

## Set the correct API base URL (critical)

### Option A (simple, separate origins)

- Frontend: `https://cgda.example.com`
- Backend: `https://cgda-api.example.com`

Set on the server before building frontend:

```bash
export VITE_API_BASE_URL="https://cgda-api.example.com"
export CORS_ORIGINS="https://cgda.example.com"
docker compose -f deployment/docker-compose.yml up -d --build
```

### Option B (recommended for IES integration: same-origin via reverse proxy)

Put CGDA behind IES as a sub-route (example):

- UI: `https://ies.example.com/cgda/`
- API: `https://ies.example.com/cgda/api/`

Then set:

- `VITE_API_BASE_URL="/cgda/api"`
- `CORS_ORIGINS="https://ies.example.com"` (or omit CORS if truly same-origin)

You’ll also need IES’s reverse proxy to route:

- `/cgda/` → CGDA frontend container
- `/cgda/api/` → CGDA backend container

## IES integration options

### Option 1: IFrame embed (fastest)

- Host CGDA at its own URL and embed it in IES.
- Requires allowing iframe embedding (headers) and aligning auth approach (SSO later).

### Option 2: Reverse proxy under IES domain (best “native” feel)

- IES serves CGDA at a sub-path.
- Requires proxy config + setting `VITE_API_BASE_URL` to a relative path as above.

## Next info I need from your deployment guy

- What URL is he opening for CGDA? (domain + path)
- Does the server have `cgda/data/processed/cgda.db` present?
- Does the browser console show failed API calls to `localhost:8000`?


