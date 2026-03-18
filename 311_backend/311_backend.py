import os
from contextlib import contextmanager
from datetime import datetime
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from psycopg2 import OperationalError
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool


def env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    return value.strip() if isinstance(value, str) else value


DATABASE_CONFIG = {
    "host": env("POSTGRES_HOST") or env("PGHOST") or "localhost",
    "port": int(env("POSTGRES_PORT") or env("PGPORT") or "5432"),
    "dbname": env("POSTGRES_DB") or env("PGDATABASE") or "nyc311",
    "user": env("POSTGRES_USER") or env("PGUSER") or "postgres",
    "password": env("POSTGRES_PASSWORD") or env("PGPASSWORD") or "",
}

APP_CONFIG = {
    "cors_origins": [origin.strip() for origin in env("CORS_ORIGINS", "*").split(",") if origin.strip()],
    "default_limit": int(env("DEFAULT_LIMIT", "500") or "500"),
    "max_limit": int(env("MAX_LIMIT", "5000") or "5000"),
    "grid_size_degrees": float(env("GRID_SIZE_DEGREES", "0.0045") or "0.0045"),
    "use_precomputed_rollups": env("USE_PRECOMPUTED_ROLLUPS", "true").lower() == "true",
}

GRID_SIZE_BY_ZOOM = {
    10: 0.0200,
    12: 0.0100,
    14: 0.0045,
}

app = FastAPI(title="NYC 311 Noise Backend", version="1.1.0")
pool: SimpleConnectionPool | None = None

app.add_middleware(
    CORSMiddleware,
    allow_origins=APP_CONFIG["cors_origins"],
    allow_credentials="*" not in APP_CONFIG["cors_origins"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@contextmanager
def get_db_cursor():
    global pool
    if pool is None:
        pool = SimpleConnectionPool(minconn=1, maxconn=10, **DATABASE_CONFIG)
    connection = pool.getconn()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            yield connection, cursor
            connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        pool.putconn(connection)


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid datetime: {value}") from exc


def bounded_limit(limit: int) -> int:
    return min(max(limit, 1), APP_CONFIG["max_limit"])


def pagination_limit(limit: Annotated[int, Query(ge=1, le=5000)] = APP_CONFIG["default_limit"]) -> int:
    return bounded_limit(limit)


def choose_rollup_zoom(zoom_level: int | None) -> int:
    if zoom_level is None:
        return 14
    if zoom_level <= 10:
        return 10
    if zoom_level <= 12:
        return 12
    return 14


def build_complaints_query(
    start_date: datetime | None,
    end_date: datetime | None,
    borough: str | None,
    complaint_type: str | None,
    descriptor: str | None,
    min_lat: float | None,
    max_lat: float | None,
    min_lon: float | None,
    max_lon: float | None,
    lat_column: str = "latitude",
    lon_column: str = "longitude",
) -> tuple[str, list]:
    clauses = [f"{lat_column} IS NOT NULL", f"{lon_column} IS NOT NULL"]
    params: list = []

    if start_date:
        clauses.append("created_date >= %s")
        params.append(start_date)
    if end_date:
        clauses.append("created_date <= %s")
        params.append(end_date)
    if borough:
        clauses.append("borough = %s")
        params.append(borough.upper())
    if complaint_type:
        clauses.append("complaint_type = %s")
        params.append(complaint_type)
    if descriptor:
        clauses.append("descriptor = %s")
        params.append(descriptor)
    if min_lat is not None:
        clauses.append(f"{lat_column} >= %s")
        params.append(min_lat)
    if max_lat is not None:
        clauses.append(f"{lat_column} <= %s")
        params.append(max_lat)
    if min_lon is not None:
        clauses.append(f"{lon_column} >= %s")
        params.append(min_lon)
    if max_lon is not None:
        clauses.append(f"{lon_column} <= %s")
        params.append(max_lon)

    return " AND ".join(clauses), params


def query_precomputed_grid_summary(
    cursor,
    zoom_level: int,
    borough: str | None,
    complaint_type: str | None,
    min_lat: float | None,
    max_lat: float | None,
    min_lon: float | None,
    max_lon: float | None,
    min_count: int,
):
    clauses = ["zoom_level = %s", "complaint_count >= %s"]
    params: list = [choose_rollup_zoom(zoom_level), min_count]

    if borough:
        clauses.append("borough = %s")
        params.append(borough.upper())
    if complaint_type:
        clauses.append("complaint_type = %s")
        params.append(complaint_type)
    if min_lat is not None:
        clauses.append("center_lat >= %s")
        params.append(min_lat)
    if max_lat is not None:
        clauses.append("center_lat <= %s")
        params.append(max_lat)
    if min_lon is not None:
        clauses.append("center_lon >= %s")
        params.append(min_lon)
    if max_lon is not None:
        clauses.append("center_lon <= %s")
        params.append(max_lon)

    cursor.execute(
        f"""
        SELECT center_lat AS lat,
               center_lon AS lon,
               complaint_count AS count,
               lat_bucket,
               lon_bucket,
               zoom_level,
               latest_created_date
        FROM complaint_grid_rollups
        WHERE {' AND '.join(clauses)}
        ORDER BY complaint_count DESC
        """,
        params,
    )
    return cursor.fetchall()


def query_live_grid_summary(
    cursor,
    start_date: datetime | None,
    end_date: datetime | None,
    borough: str | None,
    complaint_type: str | None,
    min_lat: float | None,
    max_lat: float | None,
    min_lon: float | None,
    max_lon: float | None,
    min_count: int,
):
    where_clause, params = build_complaints_query(
        start_date,
        end_date,
        borough,
        complaint_type,
        None,
        min_lat,
        max_lat,
        min_lon,
        max_lon,
    )

    query = f"""
        WITH bucketed AS (
            SELECT
                FLOOR(latitude / %s) * %s AS lat_bucket,
                FLOOR(longitude / %s) * %s AS lon_bucket,
                latitude,
                longitude
            FROM noise_complaints
            WHERE {where_clause}
        )
        SELECT
            AVG(latitude) AS lat,
            AVG(longitude) AS lon,
            COUNT(*) AS count,
            lat_bucket,
            lon_bucket,
            NULL::INTEGER AS zoom_level,
            NULL::TIMESTAMP AS latest_created_date
        FROM bucketed
        GROUP BY lat_bucket, lon_bucket
        HAVING COUNT(*) >= %s
        ORDER BY count DESC
    """

    grid_params = [
        APP_CONFIG["grid_size_degrees"],
        APP_CONFIG["grid_size_degrees"],
        APP_CONFIG["grid_size_degrees"],
        APP_CONFIG["grid_size_degrees"],
        *params,
        min_count,
    ]
    cursor.execute(query, grid_params)
    return cursor.fetchall()


@app.get("/health")
def health():
    try:
        with get_db_cursor() as (_, cursor):
            cursor.execute("SELECT COUNT(*) AS complaint_count, MAX(created_date) AS latest_created_date FROM noise_complaints")
            row = cursor.fetchone()
        return {
            "status": "ok",
            "database": DATABASE_CONFIG["dbname"],
            "complaint_count": row["complaint_count"],
            "latest_created_date": row["latest_created_date"],
        }
    except OperationalError as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}") from exc


@app.get("/complaints")
def get_complaints(
    limit: int = Depends(pagination_limit),
    offset: Annotated[int, Query(ge=0)] = 0,
    start_date: str | None = None,
    end_date: str | None = None,
    borough: str | None = None,
    complaint_type: str | None = None,
    descriptor: str | None = None,
    min_lat: float | None = None,
    max_lat: float | None = None,
    min_lon: float | None = None,
    max_lon: float | None = None,
):
    where_clause, params = build_complaints_query(
        parse_iso_datetime(start_date),
        parse_iso_datetime(end_date),
        borough,
        complaint_type,
        descriptor,
        min_lat,
        max_lat,
        min_lon,
        max_lon,
    )
    params.extend([limit, offset])

    query = f"""
        SELECT unique_key, created_date, closed_date, complaint_type, descriptor,
               agency, status, borough, city, incident_zip, incident_address,
               latitude AS lat, longitude AS lon
        FROM noise_complaints
        WHERE {where_clause}
        ORDER BY created_date DESC
        LIMIT %s OFFSET %s
    """

    with get_db_cursor() as (_, cursor):
        cursor.execute(query, params)
        return cursor.fetchall()


@app.get("/grid-summary")
def get_grid_summary(
    min_count: Annotated[int, Query(ge=1)] = 1,
    zoom_level: Annotated[int | None, Query(ge=1, le=22)] = None,
    start_date: str | None = None,
    end_date: str | None = None,
    borough: str | None = None,
    complaint_type: str | None = None,
    min_lat: float | None = None,
    max_lat: float | None = None,
    min_lon: float | None = None,
    max_lon: float | None = None,
):
    start = parse_iso_datetime(start_date)
    end = parse_iso_datetime(end_date)

    with get_db_cursor() as (_, cursor):
        if APP_CONFIG["use_precomputed_rollups"] and not start and not end:
            try:
                return query_precomputed_grid_summary(
                    cursor,
                    zoom_level or 14,
                    borough,
                    complaint_type,
                    min_lat,
                    max_lat,
                    min_lon,
                    max_lon,
                    min_count,
                )
            except Exception:
                return query_live_grid_summary(
                    cursor,
                    start,
                    end,
                    borough,
                    complaint_type,
                    min_lat,
                    max_lat,
                    min_lon,
                    max_lon,
                    min_count,
                )

        return query_live_grid_summary(
            cursor,
            start,
            end,
            borough,
            complaint_type,
            min_lat,
            max_lat,
            min_lon,
            max_lon,
            min_count,
        )


@app.get("/stats/overview")
def get_stats_overview(
    start_date: str | None = None,
    end_date: str | None = None,
    borough: str | None = None,
):
    where_clause, params = build_complaints_query(
        parse_iso_datetime(start_date),
        parse_iso_datetime(end_date),
        borough,
        None,
        None,
        None,
        None,
        None,
        None,
    )

    with get_db_cursor() as (_, cursor):
        cursor.execute(
            f"""
            SELECT
                COUNT(*) AS total_complaints,
                COUNT(DISTINCT complaint_type) AS complaint_types,
                COUNT(DISTINCT borough) AS boroughs,
                MIN(created_date) AS earliest_created_date,
                MAX(created_date) AS latest_created_date
            FROM noise_complaints
            WHERE {where_clause}
            """,
            params,
        )
        overview = cursor.fetchone()

        cursor.execute(
            f"""
            SELECT complaint_type, COUNT(*) AS count
            FROM noise_complaints
            WHERE {where_clause}
            GROUP BY complaint_type
            ORDER BY count DESC
            LIMIT 10
            """,
            params,
        )
        top_complaint_types = cursor.fetchall()

        cursor.execute(
            f"""
            SELECT borough, COUNT(*) AS count
            FROM noise_complaints
            WHERE {where_clause}
            GROUP BY borough
            ORDER BY count DESC
            """,
            params,
        )
        counts_by_borough = cursor.fetchall()

    return {
        "overview": overview,
        "top_complaint_types": top_complaint_types,
        "counts_by_borough": counts_by_borough,
    }


@app.get("/stats/timeseries")
def get_stats_timeseries(
    bucket_type: Annotated[str, Query(pattern="^(hour|day|week)$")] = "day",
    borough: str | None = None,
    complaint_type: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 90,
):
    with get_db_cursor() as (_, cursor):
        clauses = ["bucket_type = %s"]
        params: list = [bucket_type]
        if borough:
            clauses.append("borough = %s")
            params.append(borough.upper())
        if complaint_type:
            clauses.append("complaint_type = %s")
            params.append(complaint_type)
        params.append(limit)
        cursor.execute(
            f"""
            SELECT bucket_type, bucket_start, borough, complaint_type, complaint_count
            FROM complaint_time_rollups
            WHERE {' AND '.join(clauses)}
            ORDER BY bucket_start DESC
            LIMIT %s
            """,
            params,
        )
        return cursor.fetchall()


@app.get("/stream/status")
def stream_status():
    with get_db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT consumer_name, topic_name, partition_id, last_offset,
                   last_created_date, last_message_at, updated_at
            FROM stream_offsets
            ORDER BY consumer_name, partition_id
            """
        )
        return cursor.fetchall()


@app.get("/sync/status")
def sync_status():
    with get_db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT sync_name, last_started_at, last_finished_at, last_successful_at,
                   last_status, last_error, rows_processed, updated_at
            FROM sync_runs
            ORDER BY sync_name
            """
        )
        return cursor.fetchall()


@app.get("/aggregate/status")
def aggregate_status():
    with get_db_cursor() as (_, cursor):
        cursor.execute(
            """
            SELECT job_name, last_started_at, last_finished_at, last_successful_at,
                   last_status, last_error, updated_at
            FROM aggregate_job_runs
            ORDER BY job_name
            """
        )
        return cursor.fetchall()


@app.on_event("shutdown")
def shutdown():
    global pool
    if pool is not None:
        pool.closeall()
        pool = None
