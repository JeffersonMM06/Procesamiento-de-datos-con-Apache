# Procesamiento de Datos con Apache (Batch y Streaming)

Este repositorio contiene la solución correspondiente a la Tarea 3 sobre procesamiento de datos en tiempo real y por lotes (batch) utilizando Apache Kafka y PySpark.



El proyecto abarca el análisis y procesamiento de eventos de tráfico y siniestros viales a través de dos enfoques:

1. **Procesamiento Batch (`batch_siniestros.py`):** Realiza la lectura, transformación y análisis histórico de los datos de siniestros viales.
2. **Productor de Eventos Kafka (`kafka_traffic_producer.py`):** Simula el envío de métricas de tráfico en tiempo real hacia un tópico de Apache Kafka.
3. **Consumidor de Streaming PySpark (`spark_streaming_consumer.py`):** Consume el flujo de eventos emitidos por Kafka y calcula métricas agregadas por ventanas de tiempo (1 minuto) y por localidad.



##  Requisitos e Instalación

Para ejecutar este proyecto de manera local o en un entorno virtualizado, se requiere:

* Python 3.x
* Apache Spark (PySpark)
* Apache Kafka
* Biblioteca `kafka-python` (`pip install kafka-python`)



##  Instrucciones de Ejecución

### 1. Ejecución del Procesamiento Batch
Para ejecutar el análisis histórico batch:
```bash
python batch_siniestros.py
