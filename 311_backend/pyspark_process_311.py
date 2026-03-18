from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("NYC311 Noise Complaints").getOrCreate()

# Read cleaned CSV from GCS
df = spark.read.csv("gs://nyc311_database/cleaned_nyc311.csv", header=True, inferSchema=True)

# Show basic info
print("Schema:")
df.printSchema()

print("Sample Rows:")
df.show(5)

# Group by complaint type
complaint_counts = df.groupBy("complaint_type").count().orderBy("count", ascending=False)
complaint_counts.show(10)

complaint_counts \
    .repartition(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("gs://nyc311_database/results/complaint_summary")
# Save results back to GCS

spark.stop()
