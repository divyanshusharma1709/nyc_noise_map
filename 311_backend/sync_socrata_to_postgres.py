import os
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_batch
from sodapy import Socrata


SYNC_NAME = "socrata_noise_sync"


def env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    return value.strip() if isinstance(value, str) else value


DATABASE_CONFIG = {
    "dbname": env("POSTGRES_DB", "nyc311"),
    "user": env("POSTGRES_USER", "postgres"),
    "password": env("POSTGRES_PASSWORD", ""),
    "host": env("POSTGRES_HOST", "localhost"),
    "port": env("POSTGRES_PORT", "5432"),
}

BATCH_SIZE = int(env("UPSERT_BATCH_SIZE", "500") or "500")
SYNC_INTERVAL_DAYS = int(env("SYNC_INTERVAL_DAYS", "5") or "5")
LOOKBACK_DAYS = int(env("SOCRATA_LOOKBACK_DAYS", "7") or "7")


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def normalize_record(record: dict) -> tuple | None:
    complaint_type = (record.get("complaint_type") or "").strip()
    latitude = record.get("latitude")
    longitude = record.get("longitude")
    unique_key = record.get("unique_key")

    if "Noise" not in complaint_type or not unique_key or latitude in (None, "") or longitude in (None, ""):
        return None

    return (
        str(unique_key),
        parse_datetime(record.get("created_date")),
        parse_datetime(record.get("closed_date")),
        record.get("agency"),
        record.get("agency_name"),
        complaint_type,
        record.get("descriptor"),
        record.get("status"),
        record.get("borough"),
        record.get("city"),
        record.get("incident_zip"),
        record.get("incident_address"),
        record.get("street_name"),
        record.get("cross_street_1"),
        record.get("cross_street_2"),
        record.get("location_type"),
        float(latitude),
        float(longitude),
        "socrata",
    )


def get_last_successful_sync(cursor) -> datetime | None:
    cursor.execute(
        """
        SELECT last_successful_at
        FROM sync_runs
        WHERE sync_name = %s
        """,
        (SYNC_NAME,),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def mark_sync_started(cursor):
    cursor.execute(
        """
        INSERT INTO sync_runs (sync_name, last_started_at, last_status, updated_at)
        VALUES (%s, NOW(), %s, NOW())
        ON CONFLICT (sync_name) DO UPDATE SET
            last_started_at = NOW(),
            last_status = EXCLUDED.last_status,
            last_error = NULL,
            updated_at = NOW()
        """,
        (SYNC_NAME, "running"),
    )


def mark_sync_finished(cursor, status: str, rows_processed: int, error: str | None = None):
    cursor.execute(
        """
        UPDATE sync_runs
        SET last_finished_at = NOW(),
            last_successful_at = CASE WHEN %s = 'success' THEN NOW() ELSE last_successful_at END,
            last_status = %s,
            last_error = %s,
            rows_processed = %s,
            updated_at = NOW()
        WHERE sync_name = %s
        """,
        (status, status, error, rows_processed, SYNC_NAME),
    )


def fetch_records() -> list[dict]:
    client = Socrata(
        env("SOCRATA_DOMAIN", "data.cityofnewyork.us"),
        env("SOCRATA_APP_TOKEN"),
        timeout=30,
    )
    dataset = env("SOCRATA_DATASET", "erm2-nwe9")
    since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    where = (
        "complaint_type like 'Noise%' "
        "AND latitude IS NOT NULL "
        "AND longitude IS NOT NULL "
        f"AND created_date >= '{since.strftime('%Y-%m-%dT%H:%M:%S')}'"
    )

    limit = int(env("SOCRATA_LIMIT", "50000") or "50000")
    offset = 0
    all_rows: list[dict] = []

    while True:
        rows = client.get(
            dataset,
            where=where,
            order="created_date DESC",
            limit=limit,
            offset=offset,
        )
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < limit:
            break
        offset += limit

    return all_rows


def upsert_records(cursor, rows: list[dict]) -> int:
    normalized = [record for row in rows if (record := normalize_record(row))]
    if not normalized:
        return 0

    execute_batch(
        cursor,
        """
        INSERT INTO noise_complaints (
            unique_key, created_date, closed_date, agency, agency_name,
            complaint_type, descriptor, status, borough, city, incident_zip,
            incident_address, street_name, cross_street_1, cross_street_2,
            location_type, latitude, longitude, source
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (unique_key) DO UPDATE SET
            created_date = EXCLUDED.created_date,
            closed_date = EXCLUDED.closed_date,
            agency = EXCLUDED.agency,
            agency_name = EXCLUDED.agency_name,
            complaint_type = EXCLUDED.complaint_type,
            descriptor = EXCLUDED.descriptor,
            status = EXCLUDED.status,
            borough = EXCLUDED.borough,
            city = EXCLUDED.city,
            incident_zip = EXCLUDED.incident_zip,
            incident_address = EXCLUDED.incident_address,
            street_name = EXCLUDED.street_name,
            cross_street_1 = EXCLUDED.cross_street_1,
            cross_street_2 = EXCLUDED.cross_street_2,
            location_type = EXCLUDED.location_type,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            source = EXCLUDED.source,
            ingested_at = NOW()
        """,
        normalized,
        page_size=min(len(normalized), BATCH_SIZE),
    )
    return len(normalized)


def should_run(last_successful_at: datetime | None) -> bool:
    if last_successful_at is None:
        return True
    threshold = datetime.now() - timedelta(days=SYNC_INTERVAL_DAYS)
    return last_successful_at <= threshold


def main():
    connection = psycopg2.connect(**DATABASE_CONFIG)
    rows_processed = 0

    try:
        with connection:
            with connection.cursor() as cursor:
                last_successful_at = get_last_successful_sync(cursor)
                if not should_run(last_successful_at):
                    print(
                        "Skipping sync. "
                        f"Last successful sync was at {last_successful_at.isoformat()} "
                        f"and interval is {SYNC_INTERVAL_DAYS} days."
                    )
                    return

                mark_sync_started(cursor)

        rows = fetch_records()

        with connection:
            with connection.cursor() as cursor:
                rows_processed = upsert_records(cursor, rows)
                mark_sync_finished(cursor, "success", rows_processed)

        print(f"Synced {rows_processed} noise complaint rows from Socrata.")
    except Exception as exc:
        with connection:
            with connection.cursor() as cursor:
                mark_sync_finished(cursor, "failed", rows_processed, str(exc)[:1000])
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    main()
