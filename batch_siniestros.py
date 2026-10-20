from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, when, year, month

def main():
    # 1. Crear la sesión de Spark para procesamiento batch
    spark = SparkSession.builder \
        .appName("BatchAnalysisSiniestrosViales") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("--- Sesión de Spark Batch iniciada correctamente ---")

    # 2. Cargar el conjunto de datos (Asegúrate de tener tu archivo CSV en una carpeta data/)
    # Reemplaza 'data/siniestros.csv' con la ruta real de tu archivo descargado
    file_path = "data/siniestros.csv"
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)

    print(f"Datos cargados exitosamente. Total de registros iniciales: {df.count()}")

    # 3. Análisis Exploratorio Inicial (EDA): Ver esquema y primeras filas
    print("Esquema original del dataset:")
    df.printSchema()
    
    print("Primeras 5 filas del dataset:")
    df.show(5, truncate=False)

    # 4. Limpieza y Transformación de Datos
    # - Filtrar registros nulos en columnas críticas (ej: ID, fecha o localidad)
    # - Rellenar o eliminar valores vacíos según convenga
    df_cleaned = df.dropna(subset=["event_id", "event_time", "locality"])

    # Transformaciones adicionales (ejemplo: extraer año y mes si la fecha viene en texto)
    # df_cleaned = df_cleaned.withColumn("anio", year(col("event_time")))

    print(f"Registros después de la limpieza: {df_cleaned.count()}")

    # 5. Análisis Exploratorio Avanzado (Agregaciones y EDA)
    print("--- Análisis 1: Conteo de siniestros por Localidad ---")
    localidad_stats = df_cleaned.groupBy("locality") \
        .agg(count("event_id").alias("total_siniestros")) \
        .orderBy(desc("total_siniestros"))
    
    localidad_stats.show(10, truncate=False)

    print("--- Análisis 2: Conteo de siniestros por Gravedad o Clase de Accidentes ---")
    if "accident_class" in df_cleaned.columns:
        clase_stats = df_cleaned.groupBy("accident_class") \
            .agg(count("event_id").alias("total")) \
            .orderBy(desc("total"))
        clase_stats.show(10, truncate=False)

    # 6. Opcional: Guardar los resultados procesados en un archivo CSV o Parquet resumido
    # localidad_stats.write.mode("overwrite").option("header", "true").csv("data/resultado_localidades")

    # Detener la sesión de Spark al finalizar
    spark.stop()
    print("--- Procesamiento Batch finalizado con éxito ---")

if __name__ == "__main__":
    main()