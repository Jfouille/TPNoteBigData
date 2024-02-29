# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, to_timestamp

# Create a Spark session
spark = SparkSession.builder.appName("TP Final").config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").getOrCreate()

# Read Kafka messages from the 'spark' topic
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "spark").load()

# Select the 'value' column containing Mastodon API JSON data
df = df.selectExpr("CAST(value AS STRING)")

# Get tags and created_at from the JSON
df = df.selectExpr("get_json_object(value, '$.tags') as tags", "get_json_object(value, '$.created_at') as created_at")

# Keep only rows with "ia" tag
df = df.filter(df.tags.contains("ia"))

# Convert created_at to timestamp type
df = df.withColumn("created_at", to_timestamp(df.created_at, "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"))

# Define a 6-hour time window with a 30-minute overlap
window_spec = window(df.created_at, "6 hours", "30 minutes")

# Add columns for the start and end of the window
df = df.withColumn("window_start", window_spec.start) \
       .withColumn("window_end", window_spec.end)

# Write the result to a CSV file
query = df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "./output") \
    .option("checkpointLocation", "./checkpoint") \
    .start()

# Wait for the query to terminate
query.awaitTermination()
