#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${GCP_DEPS_BUCKET:?Set GCP_DEPS_BUCKET}"
: "${POSTGRES_JDBC_GCS_URI:?Set POSTGRES_JDBC_GCS_URI to a gs:// path for postgresql-42.6.0.jar}"
: "${POSTGRES_JDBC_URL:?Set POSTGRES_JDBC_URL}"
: "${POSTGRES_USER:?Set POSTGRES_USER}"
: "${POSTGRES_PASSWORD:?Set POSTGRES_PASSWORD}"

COMMON_ARGS=(
  --project="${GCP_PROJECT_ID}"
  --region="${GCP_REGION}"
  --version=2.3
  --deps-bucket="${GCP_DEPS_BUCKET}"
  --jars="${POSTGRES_JDBC_GCS_URI}"
  --properties="spark.executorEnv.POSTGRES_JDBC_URL=${POSTGRES_JDBC_URL},spark.executorEnv.POSTGRES_USER=${POSTGRES_USER},spark.executorEnv.POSTGRES_PASSWORD=${POSTGRES_PASSWORD},spark.executorEnv.POSTGRES_JDBC_DRIVER=org.postgresql.Driver,spark.yarn.appMasterEnv.POSTGRES_JDBC_URL=${POSTGRES_JDBC_URL},spark.yarn.appMasterEnv.POSTGRES_USER=${POSTGRES_USER},spark.yarn.appMasterEnv.POSTGRES_PASSWORD=${POSTGRES_PASSWORD},spark.yarn.appMasterEnv.POSTGRES_JDBC_DRIVER=org.postgresql.Driver"
)

gcloud dataproc batches submit pyspark dataproc_jobs/build_grid_layers.py "${COMMON_ARGS[@]}"
gcloud dataproc batches submit pyspark dataproc_jobs/build_time_rollups.py "${COMMON_ARGS[@]}"
