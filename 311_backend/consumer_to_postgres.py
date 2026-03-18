import json
import logging
import os
from datetime import datetime

import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import execute_batch


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("consumer_to_postgres")


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

TOPIC = env("KAFKA_TOPIC", "noise-complaints")
BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GROUP_ID = env("KAFKA_GROUP_ID", "noise-complaints-postgres")
BATCH_SIZE = int(env("CONSUMER_BATCH_SIZE", "500") or "500")


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def normalize_record(record: dict) -> tuple:
    return (
        str(record["unique_key"]),
        parse_datetime(record.get("created_date")),
        parse_datetime(record.get("closed_date")),
        record.get("agency"),
        record.get("agency_name"),
        record.get("complaint_type"),
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
        float(record["latitude"]) if record.get("latitude") not in (None, "") else None,
        float(record["longitude"]) if record.get("longitude") not in (None, "") else None,
        record.get("source", "kafka"),
    )


def write_batch(cursor, batch: list[tuple], offsets: list[tuple]):
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
        batch,
        page_size=min(len(batch), BATCH_SIZE),
    )

    execute_batch(
        cursor,
        """
        INSERT INTO stream_offsets (
            consumer_name, topic_name, partition_id, last_offset,
            last_created_date, last_message_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (consumer_name, topic_name, partition_id) DO UPDATE SET
            last_offset = EXCLUDED.last_offset,
            last_created_date = EXCLUDED.last_created_date,
            last_message_at = NOW(),
            updated_at = NOW()
        """,
        offsets,
        page_size=min(len(offsets), BATCH_SIZE),
    )


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset=env("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        enable_auto_commit=False,
        group_id=GROUP_ID,
    )

    connection = psycopg2.connect(**DATABASE_CONFIG)
    cursor = connection.cursor()
    batch: list[tuple] = []
    offsets: list[tuple] = []

    logger.info("Consuming topic %s with group %s", TOPIC, GROUP_ID)

    try:
        for message in consumer:
            record = message.value
            try:
                batch.append(normalize_record(record))
                offsets.append(
                    (
                        GROUP_ID,
                        message.topic,
                        message.partition,
                        message.offset,
                        parse_datetime(record.get("created_date")),
                    )
                )
            except Exception as exc:
                logger.warning("Skipping malformed record %s due to %s", record, exc)
                continue

            if len(batch) >= BATCH_SIZE:
                write_batch(cursor, batch, offsets)
                connection.commit()
                consumer.commit()
                logger.info("Committed %s records through offset %s", len(batch), offsets[-1][3])
                batch.clear()
                offsets.clear()
    finally:
        if batch:
            write_batch(cursor, batch, offsets)
            connection.commit()
            consumer.commit()
            logger.info("Committed final %s records", len(batch))
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()
