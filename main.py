# Importer les modules nécessaires
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import window, count

# Créer une session Spark
spark = SparkSession.builder.appName("TP Final").config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").getOrCreate()

# Lire les messages Kafka du topic spark
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "spark").load()

# Sélectionner la colonne value qui contient les données JSON de l'API Mastodon
df = df.selectExpr("CAST(value AS STRING)")

# Convertir les données JSON en colonnes Spark
df = df.selectExpr("from_json(value, 'tags STRING, id STRING, content STRING, created_at TIMESTAMP') as data").select("data.*")

# Filtrer les messages qui contiennent le hashtag #IA
#df = df.filter(df.tag == "#IA")


df.writeStream.outputMode("append").format("console").start().awaitTermination()
