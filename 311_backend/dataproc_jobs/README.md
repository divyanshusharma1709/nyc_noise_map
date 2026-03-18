# Dataproc Jobs

These Spark jobs are the offline analytics layer for the NYC noise map.

They are intended to run on Dataproc or any Spark environment and write
derived aggregate tables back to PostgreSQL for the FastAPI app to serve.

## Jobs

- `build_grid_layers.py`
  Builds precomputed map grid layers for zoom levels 10, 12, and 14.
- `build_time_rollups.py`
  Builds hourly, daily, and weekly complaint rollups by borough and complaint type.

## Environment

```bash
export POSTGRES_JDBC_URL=jdbc:postgresql://HOST:5432/nyc311
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=your_password
export POSTGRES_JDBC_JAR=/path/to/postgresql-42.6.0.jar
```

Optional:

```bash
export DATAPROC_INPUT_PATH=gs://bucket/path/to/noise_complaints.csv
```

If `DATAPROC_INPUT_PATH` is set, the jobs read from object storage.
Otherwise they read the raw `noise_complaints` table from PostgreSQL.
