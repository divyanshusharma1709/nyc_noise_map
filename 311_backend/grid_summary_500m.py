from pyspark.sql import SparkSession
from pyspark.sql.functions import floor, col, count, avg

# Create Spark session
spark = SparkSession.builder.appName("NYC311 Grid Aggregation").getOrCreate()

# Read cleaned CSV from GCS
df = spark.read.csv("gs://nyc311_database/cleaned_nyc311.csv", header=True, inferSchema=True)

# Convert lat/lon to 500m grid cells using ~0.0045 degree bins
grid_size_deg = 0.0045  # ~500 meters

df = df.withColumn("lat_bucket", floor(col("latitude") / grid_size_deg) * grid_size_deg)
df = df.withColumn("lon_bucket", floor(col("longitude") / grid_size_deg) * grid_size_deg)

# Aggregate complaint counts per grid cell
grid_summary = df.groupBy("lat_bucket", "lon_bucket").agg(
    count("*").alias("complaint_count"),
    avg("latitude").alias("center_lat"),
    avg("longitude").alias("center_lon")
)

# Show sample output
grid_summary.show(10)

# Save to GCS
grid_summary.write.csv("gs://nyc311_database/results/grid_summary_500m/", header=True, mode="overwrite")

# Optional: Write to PostgreSQL (requires appropriate connector jar and network access)
grid_summary.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://10.142.0.2:5432/nyc311") \
    .option("dbtable", "grid_summary_500m") \
    .option("user", "postgres") \
    .option("password", "Mishi@1709") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

spark.stop()
