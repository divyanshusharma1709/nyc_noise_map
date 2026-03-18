from pyspark.sql.functions import avg, col, count, floor, lit, max as spark_max, min as spark_min

from common import GRID_DEFINITIONS, create_spark, read_noise_complaints, write_jdbc


def build_rollup_for_zoom(df, zoom_level: int, grid_size: float):
    bucketed = (
        df.withColumn("lat_bucket", floor(col("latitude") / lit(grid_size)) * lit(grid_size))
        .withColumn("lon_bucket", floor(col("longitude") / lit(grid_size)) * lit(grid_size))
    )

    return bucketed.groupBy("borough", "complaint_type", "lat_bucket", "lon_bucket").agg(
        lit(zoom_level).alias("zoom_level"),
        lit(grid_size).alias("grid_size_degrees"),
        avg("latitude").alias("center_lat"),
        avg("longitude").alias("center_lon"),
        count("*").alias("complaint_count"),
        spark_min("created_date").alias("earliest_created_date"),
        spark_max("created_date").alias("latest_created_date"),
    )


def main():
    spark = create_spark("NYC311BuildGridLayers")
    try:
        complaints = read_noise_complaints(spark).select(
            "borough",
            "complaint_type",
            "created_date",
            "latitude",
            "longitude",
        )

        rollups = [
            build_rollup_for_zoom(complaints, zoom_level, grid_size)
            for zoom_level, grid_size in GRID_DEFINITIONS.items()
        ]

        output = rollups[0]
        for rollup in rollups[1:]:
            output = output.unionByName(rollup)

        write_jdbc(output, "complaint_grid_rollups")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
