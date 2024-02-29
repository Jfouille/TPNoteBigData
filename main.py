# Importer les modules nécessaires
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, to_timestamp

spark = SparkSession.builder.appName("TP Final").config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").getOrCreate()

# Lire les messages Kafka du topic spark
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "spark").load()

# Sélectionner la colonne value qui contient les données JSON de l'API Mastodon
df = df.selectExpr("CAST(value AS STRING)")

# get tags and created_at from the json
df = df.selectExpr("get_json_object(value, '$.tags') as tags", "get_json_object(value, '$.created_at') as created_at")

#  Keeping only rows with "ia" tag
df = df.filter(df.tags.contains("ia"))


# Convertir created_at en type timestamp                      2024-02-29T09:31:19.000Z
df = df.withColumn("created_at", to_timestamp(df.created_at, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

# Définir une fenêtre temporelle de 6 heures avec un chevauchement de 30 minutes
window_spec = window(df.created_at, "6 hours")#, "30 minutes")

# Ajouter les colonnes pour le début et la fin de la fenêtre
df = df.withColumn("window_start", window_spec.start) \
       .withColumn("window_end", window_spec.end)

# Compter le nombre de threads dans chaque fenêtre
df = df.groupBy("window_start", "window_end").count()

dfWithWatermark = df.withWatermark("window_start", "6 hours")

# Écrire le résultat dans un fichier CSV
query = dfWithWatermark.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "./output") \
    .option("checkpointLocation", "./checkpoint") \
    .start()

# Attendre l'arrêt du query
query.awaitTermination()


# df.writeStream.outputMode("append").format("console").start().awaitTermination()
