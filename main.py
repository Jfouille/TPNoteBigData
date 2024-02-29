# Importer les modules nécessaires
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import window, count

# Créer une session Spark
spark = SparkSession.builder.appName("TP Final").config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").getOrCreate()

# Lire les messages Kafka du topic spark
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "spark").option("failOnDataLoss", "false").load()

# Sélectionner la colonne value qui contient les données JSON de l'API Mastodon
df = df.selectExpr("CAST(value AS STRING)")

# get tags and created_at from the json
df = df.selectExpr("get_json_object(value, '$.tags') as tags", "get_json_object(value, '$.created_at') as created_at")

#  Keeping only rows with "ia" tag
df = df.filter(df.tags.contains("ia"))

#df.writeStream.outputMode("append").format("console").start().awaitTermination()
df.writeStream \
  .format("csv") \
  .trigger(processingTime="10 seconds") \
  .option("checkpointLocation", "checkpoint/") \
  .option("path", "output_path/") \
  .outputMode("append")\
  .start()\
  .awaitTermination()

