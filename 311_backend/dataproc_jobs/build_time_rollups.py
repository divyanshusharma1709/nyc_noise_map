from pyspark.sql.functions import col, count, date_trunc, lit

from common import create_spark, read_noise_complaints, write_jdbc


def build_bucket(df, bucket_type: str):
    bucketed = df.withColumn("bucket_start", date_trunc(bucket_type, col("created_date")))
    return bucketed.groupBy("bucket_start", "borough", "complaint_type").agg(
        lit(bucket_type).alias("bucket_type"),
        count("*").alias("complaint_count"),
    )


def main():
    spark = create_spark("NYC311BuildTimeRollups")
    try:
        complaints = read_noise_complaints(spark).select(
            "created_date",
            "borough",
            "complaint_type",
        )

        hourly = build_bucket(complaints, "hour")
        daily = build_bucket(complaints, "day")
        weekly = build_bucket(complaints, "week")

        output = hourly.unionByName(daily).unionByName(weekly)
        write_jdbc(output, "complaint_time_rollups")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
