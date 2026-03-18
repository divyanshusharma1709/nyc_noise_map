# Deployment

## Recommended low-cost production setup

Use Railway for everything in production:

- Backend service rooted at `311_backend/`
- Frontend service rooted at `frontend-nyc-map/`
- Railway Postgres
- One Railway cron job that runs sync plus aggregate rebuild

## Railway backend

Service root: `311_backend/`

Environment:

```bash
POSTGRES_HOST
POSTGRES_PORT
POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD
CORS_ORIGINS
USE_PRECOMPUTED_ROLLUPS=true
```

The backend now also accepts Railway-style `PG*` variables automatically.

## Railway frontend

Service root: `frontend-nyc-map/`

Environment:

```bash
NEXT_PUBLIC_API_BASE=https://YOUR_BACKEND_DOMAIN
```

## Railway cron job

Use a separate Railway service rooted at `311_backend/`.

Start command:

```bash
sh -c "python sync_socrata_to_postgres.py && python rebuild_aggregates.py"
```

Cron schedule:

```bash
0 9 * * *
```

Recommended env:

```bash
SYNC_INTERVAL_DAYS=5
SOCRATA_LOOKBACK_DAYS=7
USE_PRECOMPUTED_ROLLUPS=true
```

## Optional Dataproc

If you outgrow the Railway-only architecture, you can still submit the Spark jobs in `311_backend/dataproc_jobs/` to Google Dataproc Serverless. That is optional and no longer required for production.
