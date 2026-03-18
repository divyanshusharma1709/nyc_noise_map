CREATE TABLE IF NOT EXISTS noise_complaints (
    unique_key TEXT PRIMARY KEY,
    created_date TIMESTAMP NULL,
    closed_date TIMESTAMP NULL,
    agency TEXT NULL,
    agency_name TEXT NULL,
    complaint_type TEXT NULL,
    descriptor TEXT NULL,
    status TEXT NULL,
    borough TEXT NULL,
    city TEXT NULL,
    incident_zip TEXT NULL,
    incident_address TEXT NULL,
    street_name TEXT NULL,
    cross_street_1 TEXT NULL,
    cross_street_2 TEXT NULL,
    location_type TEXT NULL,
    latitude DOUBLE PRECISION NULL,
    longitude DOUBLE PRECISION NULL,
    source TEXT NOT NULL DEFAULT 'unknown',
    ingested_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_noise_complaints_created_date
    ON noise_complaints (created_date DESC);

CREATE INDEX IF NOT EXISTS idx_noise_complaints_complaint_type
    ON noise_complaints (complaint_type);

CREATE INDEX IF NOT EXISTS idx_noise_complaints_borough
    ON noise_complaints (borough);

CREATE INDEX IF NOT EXISTS idx_noise_complaints_lat_lon
    ON noise_complaints (latitude, longitude);

CREATE TABLE IF NOT EXISTS stream_offsets (
    consumer_name TEXT NOT NULL,
    topic_name TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    last_offset BIGINT NOT NULL,
    last_created_date TIMESTAMP NULL,
    last_message_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_name, topic_name, partition_id)
);

CREATE TABLE IF NOT EXISTS sync_runs (
    sync_name TEXT PRIMARY KEY,
    last_started_at TIMESTAMP NULL,
    last_finished_at TIMESTAMP NULL,
    last_successful_at TIMESTAMP NULL,
    last_status TEXT NULL,
    last_error TEXT NULL,
    rows_processed INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aggregate_job_runs (
    job_name TEXT PRIMARY KEY,
    last_started_at TIMESTAMP NULL,
    last_finished_at TIMESTAMP NULL,
    last_successful_at TIMESTAMP NULL,
    last_status TEXT NULL,
    last_error TEXT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS complaint_grid_rollups (
    zoom_level INTEGER NOT NULL,
    grid_size_degrees DOUBLE PRECISION NOT NULL,
    borough TEXT NULL,
    complaint_type TEXT NULL,
    lat_bucket DOUBLE PRECISION NOT NULL,
    lon_bucket DOUBLE PRECISION NOT NULL,
    center_lat DOUBLE PRECISION NOT NULL,
    center_lon DOUBLE PRECISION NOT NULL,
    complaint_count BIGINT NOT NULL,
    earliest_created_date TIMESTAMP NULL,
    latest_created_date TIMESTAMP NULL,
    refreshed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (zoom_level, borough, complaint_type, lat_bucket, lon_bucket)
);

CREATE INDEX IF NOT EXISTS idx_complaint_grid_rollups_lookup
    ON complaint_grid_rollups (zoom_level, borough, complaint_type, complaint_count DESC);

CREATE TABLE IF NOT EXISTS complaint_time_rollups (
    bucket_type TEXT NOT NULL,
    bucket_start TIMESTAMP NOT NULL,
    borough TEXT NULL,
    complaint_type TEXT NULL,
    complaint_count BIGINT NOT NULL,
    refreshed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (bucket_type, bucket_start, borough, complaint_type)
);

CREATE INDEX IF NOT EXISTS idx_complaint_time_rollups_lookup
    ON complaint_time_rollups (bucket_type, bucket_start DESC, borough, complaint_type);
