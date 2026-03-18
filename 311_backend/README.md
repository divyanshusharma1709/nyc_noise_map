# NYC 311 noise backend

This backend now has two layers:

- Online serving layer: FastAPI + PostgreSQL for raw complaints and API responses
- Offline analytics layer: cheap Postgres aggregate rebuilds for production, with optional Spark/Dataproc jobs for future scale

## Active services

- `311_backend.py`: FastAPI app for raw complaints, precomputed grid layers, summary stats, timeseries, sync status, and aggregate-job status
- `sync_socrata_to_postgres.py`: short-lived direct sync job for low-cost scheduled refreshes
- `consumer_to_postgres.py`: Kafka consumer that upserts complaints into PostgreSQL in batches
- `stream_from_csv.py`: Kafka producer for CSV snapshots or fresh Socrata rows
- `schema.sql`: raw and derived table schema
- `rebuild_aggregates.py`: rebuilds precomputed grid layers and time rollups directly in PostgreSQL
- `dataproc_jobs/build_grid_layers.py`: optional Spark job for precomputed grid layers
- `dataproc_jobs/build_time_rollups.py`: optional Spark job for hourly, daily, and weekly complaint rollups

## Environment

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=nyc311
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=your_password
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=noise-complaints
export SOCRATA_APP_TOKEN=your_optional_token
export CORS_ORIGINS=http://localhost:3000
```

Optional:

```bash
export USE_PRECOMPUTED_ROLLUPS=true
export POSTGRES_JDBC_URL=jdbc:postgresql://localhost:5432/nyc311
export POSTGRES_JDBC_JAR=/absolute/path/to/postgresql-42.6.0.jar
```

## Bootstrap

Create the schema:

```bash
psql -d "$POSTGRES_DB" -U "$POSTGRES_USER" -f schema.sql
```

Sync recent noise complaints into PostgreSQL:

```bash
python3 sync_socrata_to_postgres.py
```

Run the API:

```bash
uvicorn 311_backend:app --reload
```

## Cheap production aggregate rebuild

For Railway Hobby, use PostgreSQL itself to rebuild precomputed aggregates:

```bash
python3 rebuild_aggregates.py
```

This refreshes:

- `complaint_grid_rollups`
- `complaint_time_rollups`

The API will use these tables when `USE_PRECOMPUTED_ROLLUPS=true`.

## Optional Dataproc batch layer

The Spark jobs in `dataproc_jobs/` are still available if you later outgrow the cheap Postgres-only approach.

## Recommended serving pattern

- `GET /complaints`
  Raw recent points from `noise_complaints`
- `GET /grid-summary?zoom_level=12`
  Reads from `complaint_grid_rollups` when available, otherwise falls back to live SQL aggregation
- `GET /stats/overview`
  Reads high-level counters from raw complaints
- `GET /stats/timeseries?bucket_type=day`
  Reads precomputed Dataproc time rollups
- `GET /sync/status`
  Sync job status
- `GET /aggregate/status`
  Dataproc job status

## Railway sync schedule

Use a separate Railway cron service that runs both refresh steps in sequence:

```bash
python3 sync_socrata_to_postgres.py && python3 rebuild_aggregates.py
```

Recommended cron:

```bash
0 9 * * *
```

Recommended env:

```bash
export SYNC_INTERVAL_DAYS=5
export SOCRATA_LOOKBACK_DAYS=7
export USE_PRECOMPUTED_ROLLUPS=true
```
