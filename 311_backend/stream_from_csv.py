import csv
import json
import logging
import os
import time
from pathlib import Path

from kafka import KafkaProducer
from sodapy import Socrata


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("stream_from_csv")


def env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    return value.strip() if isinstance(value, str) else value


TOPIC = env("KAFKA_TOPIC", "noise-complaints")
BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
MODE = env("STREAM_MODE", "csv")
CSV_PATH = Path(env("STREAM_CSV_PATH", "ny_noise_data.csv"))
SLEEP_SECONDS = float(env("STREAM_SLEEP_SECONDS", "0.01") or "0.01")
LIVE_LIMIT = int(env("SOCRATA_LIMIT", "5000") or "5000")


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        linger_ms=int(env("KAFKA_LINGER_MS", "250") or "250"),
        batch_size=int(env("KAFKA_BATCH_SIZE", "16384") or "16384"),
    )


def normalize_record(row: dict, source: str) -> dict | None:
    complaint_type = (row.get("complaint_type") or "").strip()
    latitude = row.get("latitude")
    longitude = row.get("longitude")
    unique_key = row.get("unique_key")

    if "Noise" not in complaint_type or not unique_key or latitude in (None, "") or longitude in (None, ""):
        return None

    return {
        "unique_key": str(unique_key),
        "created_date": row.get("created_date"),
        "closed_date": row.get("closed_date"),
        "agency": row.get("agency"),
        "agency_name": row.get("agency_name"),
        "complaint_type": complaint_type,
        "descriptor": row.get("descriptor"),
        "status": row.get("status"),
        "borough": row.get("borough"),
        "city": row.get("city"),
        "incident_zip": row.get("incident_zip"),
        "incident_address": row.get("incident_address"),
        "street_name": row.get("street_name"),
        "cross_street_1": row.get("cross_street_1"),
        "cross_street_2": row.get("cross_street_2"),
        "location_type": row.get("location_type"),
        "latitude": latitude,
        "longitude": longitude,
        "source": source,
    }


def publish_records(records, producer: KafkaProducer, source: str):
    sent = 0
    for row in records:
        payload = normalize_record(row, source)
        if not payload:
            continue
        producer.send(TOPIC, payload)
        sent += 1
        if SLEEP_SECONDS > 0:
            time.sleep(SLEEP_SECONDS)
    producer.flush()
    logger.info("Published %s records to %s from %s", sent, TOPIC, source)


def stream_csv(producer: KafkaProducer):
    with CSV_PATH.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        publish_records(reader, producer, "csv")


def stream_socrata(producer: KafkaProducer):
    client = Socrata(
        env("SOCRATA_DOMAIN", "data.cityofnewyork.us"),
        env("SOCRATA_APP_TOKEN"),
        timeout=30,
    )
    where = env(
        "SOCRATA_WHERE",
        "complaint_type like 'Noise%' AND latitude IS NOT NULL AND longitude IS NOT NULL",
    )
    rows = client.get(
        env("SOCRATA_DATASET", "erm2-nwe9"),
        limit=LIVE_LIMIT,
        order="created_date DESC",
        where=where,
    )
    publish_records(rows, producer, "socrata")


def main():
    producer = create_producer()
    logger.info("Streaming mode=%s topic=%s", MODE, TOPIC)

    if MODE == "socrata":
        stream_socrata(producer)
    else:
        stream_csv(producer)


if __name__ == "__main__":
    main()
