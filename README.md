# NYC Noise Map

This workspace is organized into two applications and one cheap aggregate layer:

- `311_backend/`: FastAPI API, PostgreSQL sync/streaming jobs, Dataproc/Spark jobs, schema, and datasets
- `frontend-nyc-map/`: Next.js map frontend
- `311_backend/dataproc_jobs/`: offline analytics jobs that build precomputed aggregates for the API

## Architecture

1. Socrata or Kafka loads raw 311 noise complaints into PostgreSQL
2. A lightweight Postgres aggregate rebuild job refreshes derived tables
3. FastAPI serves raw points and precomputed rollups to the frontend
4. Optional Dataproc/Spark jobs remain available for future scale

## Local development

Backend:

```bash
cd 311_backend
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
psql -d "$POSTGRES_DB" -U "$POSTGRES_USER" -f schema.sql
python sync_socrata_to_postgres.py
uvicorn 311_backend:app --reload
```

Frontend:

```bash
cd frontend-nyc-map
echo 'NEXT_PUBLIC_API_BASE=http://127.0.0.1:8000' > .env.local
npm install
npm run dev
```

## Aggregate rebuild job

```bash
cd 311_backend
python rebuild_aggregates.py
```

This writes to:

- `complaint_grid_rollups`
- `complaint_time_rollups`

The live API will use those tables automatically when `USE_PRECOMPUTED_ROLLUPS=true`.

Optional Spark jobs remain in `311_backend/dataproc_jobs/` if you later need Dataproc.
