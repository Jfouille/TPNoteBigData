from pyspark.sql import SparkSession
from pyspark.sql.functions import window, to_timestamp, count
import time

spark = SparkSession.builder.appName("TP Final").config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").getOrCreate()

# Lecture des messages Kafka du topic spark
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "spark").option("failOnDataLoss", "false").load()

# On récupère la colonne value qui contient les données JSON de l'API Mastodon
df = df.selectExpr("CAST(value AS STRING)")

# On récupère les colonnes qui nous intérèssent
# Ici tags et created_at
df = df.selectExpr("get_json_object(value, '$.tags') as tags", "get_json_object(value, '$.created_at') as created_at")

# On fait un filtre pour garder que les tags contenant "ia"
df = df.filter(df.tags.contains("ia"))

# On converti le timestamp qui est en string ver un "vrai" type timestamp 2024-02-29T09:31:19.000Z
df = df.withColumn("created_at", to_timestamp(df.created_at, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

# On créer une fenêtre de 6 heures avec un chevauchement de 30 minutes
window_spec = window(df.created_at, "6 hours", "30 minutes")

# On ajoute les colonnes pour le début et la fin de la fenêtre
# Puis on compte effectue un comptage en regroupant avec la window définie
df = df.withColumn("window_start", window_spec.start) \
       .withColumn("window_end", window_spec.end)

dfWithWatermark = df.withWatermark("window_start", "6 hours")

df_count = dfWithWatermark.groupBy("window_start", "window_end").agg(count("*").alias("count"))


# Avant question bonus
# df_count.writeStream \
#   .format("csv") \
#   .trigger(processingTime="10 seconds") \
#   .option("checkpointLocation", "checkpoint/") \
#   .option("path", "output_path/") \
#   .outputMode("append")\
#   .start()\
#   .awaitTermination()


# Question bonus
# On commence le flux de streaming pendant une durée donnée
query = df_count.writeStream \
  .format("memory") \
  .trigger(processingTime="10 seconds") \
  .queryName("temp") \
  .outputMode("complete")\
  .start()

time.sleep(60)
query.stop()

# Ce qui nous permet de d'écrire toutes les données d'un coup d'un un unique fichier
data = spark.table("temp")
data.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save("./output")
