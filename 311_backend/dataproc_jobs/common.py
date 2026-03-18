import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lower, trim


GRID_DEFINITIONS = {
    10: 0.0200,
    12: 0.0100,
    14: 0.0045,
}


def env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    return value.strip() if isinstance(value, str) else value


def create_spark(app_name: str) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    jdbc_jar = env("POSTGRES_JDBC_JAR")
    if jdbc_jar:
        builder = builder.config("spark.jars", jdbc_jar)
    return builder.getOrCreate()


def read_noise_complaints(spark: SparkSession) -> DataFrame:
    input_path = env("DATAPROC_INPUT_PATH")
    if input_path:
        source_df = spark.read.option("header", True).csv(input_path, inferSchema=True)
    else:
        jdbc_url = env("POSTGRES_JDBC_URL", "jdbc:postgresql://localhost:5432/nyc311")
        source_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "noise_complaints")
            .option("user", env("POSTGRES_USER", "postgres"))
            .option("password", env("POSTGRES_PASSWORD", ""))
            .option("driver", env("POSTGRES_JDBC_DRIVER", "org.postgresql.Driver"))
            .load()
        )

    return (
        source_df.filter(col("latitude").isNotNull() & col("longitude").isNotNull())
        .filter(lower(trim(col("complaint_type"))).contains("noise"))
    )


def write_jdbc(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    jdbc_url = env("POSTGRES_JDBC_URL", "jdbc:postgresql://localhost:5432/nyc311")
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", env("POSTGRES_USER", "postgres"))
        .option("password", env("POSTGRES_PASSWORD", ""))
        .option("driver", env("POSTGRES_JDBC_DRIVER", "org.postgresql.Driver"))
        .mode(mode)
        .save()
    )
