import os
from contextlib import contextmanager

import psycopg2


GRID_DEFINITIONS = {
    10: 0.0200,
    12: 0.0100,
    14: 0.0045,
}
JOB_NAME = "postgres_aggregate_rebuild"


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


@contextmanager
def get_connection():
    connection = psycopg2.connect(**DATABASE_CONFIG)
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def mark_job_started(cursor):
    cursor.execute(
        """
        INSERT INTO aggregate_job_runs (job_name, last_started_at, last_status, last_error, updated_at)
        VALUES (%s, NOW(), %s, NULL, NOW())
        ON CONFLICT (job_name) DO UPDATE SET
            last_started_at = NOW(),
            last_status = EXCLUDED.last_status,
            last_error = NULL,
            updated_at = NOW()
        """,
        (JOB_NAME, "running"),
    )


def mark_job_finished(cursor, status: str, error: str | None = None):
    cursor.execute(
        """
        UPDATE aggregate_job_runs
        SET last_finished_at = NOW(),
            last_successful_at = CASE WHEN %s = 'success' THEN NOW() ELSE last_successful_at END,
            last_status = %s,
            last_error = %s,
            updated_at = NOW()
        WHERE job_name = %s
        """,
        (status, status, error, JOB_NAME),
    )


def rebuild_grid_rollups(cursor):
    cursor.execute("TRUNCATE complaint_grid_rollups")
    for zoom_level, grid_size in GRID_DEFINITIONS.items():
        cursor.execute(
            """
            INSERT INTO complaint_grid_rollups (
                zoom_level,
                grid_size_degrees,
                borough,
                complaint_type,
                lat_bucket,
                lon_bucket,
                center_lat,
                center_lon,
                complaint_count,
                earliest_created_date,
                latest_created_date,
                refreshed_at
            )
            SELECT
                %s AS zoom_level,
                %s AS grid_size_degrees,
                borough,
                complaint_type,
                FLOOR(latitude / %s) * %s AS lat_bucket,
                FLOOR(longitude / %s) * %s AS lon_bucket,
                AVG(latitude) AS center_lat,
                AVG(longitude) AS center_lon,
                COUNT(*) AS complaint_count,
                MIN(created_date) AS earliest_created_date,
                MAX(created_date) AS latest_created_date,
                NOW() AS refreshed_at
            FROM noise_complaints
            WHERE latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND complaint_type ILIKE 'Noise%%'
            GROUP BY borough, complaint_type, FLOOR(latitude / %s) * %s, FLOOR(longitude / %s) * %s
            """,
            (
                zoom_level,
                grid_size,
                grid_size,
                grid_size,
                grid_size,
                grid_size,
                grid_size,
                grid_size,
                grid_size,
                grid_size,
            ),
        )


def rebuild_time_rollups(cursor):
    cursor.execute("TRUNCATE complaint_time_rollups")
    for bucket_type in ("hour", "day", "week"):
        cursor.execute(
            """
            INSERT INTO complaint_time_rollups (
                bucket_type,
                bucket_start,
                borough,
                complaint_type,
                complaint_count,
                refreshed_at
            )
            SELECT
                %s AS bucket_type,
                date_trunc(%s, created_date) AS bucket_start,
                borough,
                complaint_type,
                COUNT(*) AS complaint_count,
                NOW() AS refreshed_at
            FROM noise_complaints
            WHERE created_date IS NOT NULL
              AND complaint_type ILIKE 'Noise%%'
            GROUP BY date_trunc(%s, created_date), borough, complaint_type
            """,
            (bucket_type, bucket_type, bucket_type),
        )


def main():
    with get_connection() as connection:
        with connection.cursor() as cursor:
            mark_job_started(cursor)

    try:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                rebuild_grid_rollups(cursor)
                rebuild_time_rollups(cursor)
                mark_job_finished(cursor, "success")
        print("Rebuilt complaint_grid_rollups and complaint_time_rollups.")
    except Exception as exc:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                mark_job_finished(cursor, "failed", str(exc)[:1000])
        raise


if __name__ == "__main__":
    main()
