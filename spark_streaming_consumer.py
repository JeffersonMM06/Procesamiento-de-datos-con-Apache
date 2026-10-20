from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import logging

def main():
    # 1. Crear la sesión de Spark configurada para streaming
    spark = SparkSession.builder \
        .appName("KafkaSparkStreamingSiniestros") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("--- Sesión de Spark Streaming iniciada ---")

    # 2. Definir el esquema de los datos de entrada que envía el productor de Kafka
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),  # O TimestampType() si viene formateado
        StructField("locality", StringType(), True),
        StructField("accident_class", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ])

    # 3. Configurar el lector de streaming para consumir desde Kafka
    kafka_server = "localhost:9092"
    topic_name = "siniestros_colombia"  # Debe coincidir con el tópico del productor

    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    # 4. Parsear los datos de Kafka (convertir el campo 'value' binario a String y luego aplicar el esquema JSON)
    parsed_df = df_kafka.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Convertir el campo de tiempo a tipo Timestamp para poder usar ventanas de tiempo
    parsed_df = parsed_df.withColumn("timestamp", col("event_time").cast(TimestampType()))

    # 5. Calcular estadísticas en tiempo real por ventana de tiempo (ej. por minuto y localidad)
    windowed_stats = parsed_df \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("locality")
        ) \
        .agg(count("event_id").alias("total_siniestros"))

    # 6. Escribir los resultados en la consola en tiempo real
    query = windowed_stats \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    print(f"--- Escuchando eventos en tiempo real desde el tópico '{topic_name}' ---")
    query.awaitTermination()

if __name__ == "__main__":
    main()