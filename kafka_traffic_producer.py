import json
import time
import pandas as pd
from kafka import KafkaProducer

def main():
    # 1. Configurar el servidor de Kafka y el Tópico
    bootstrap_servers = 'localhost:9092'
    topic_name = 'siniestros_colombia'  # O el nombre de tu tópico

    print(f"Conectando al productor de Kafka en {bootstrap_servers}...")
    
    # Inicializar el productor serializando los mensajes a JSON
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # 2. Cargar el dataset de referencia (puedes usar pandas para leer el CSV local)
    file_path = "data/siniestros.csv"  # Ruta de tu archivo CSV
    print(f"Leyendo datos desde {file_path}...")
    
    df = pd.read_csv(file_path)
    print(f"Total de registros listos para transmitir: {len(df)}")

    # 3. Simular la llegada de datos en tiempo real (Streaming)
    print(f"--- Iniciando transmisión hacia el tópico '{topic_name}' ---")
    try:
        # Iteramos sobre las filas del DataFrame simulando eventos en tiempo real
        for index, row in df.iterrows():
            # Construir el diccionario con las columnas clave del siniestro
            evento = {
                "event_id": str(index + 1),
                "event_time": str(row.get("fecha", "2026-10-11T00:00:00")),
                "locality": str(row.get("localidad", "Desconocida")),
                "accident_class": str(row.get("clase_accidente", "Colisión")),
                "severity": str(row.get("gravedad", "Heridos")),
                "latitude": float(row.get("latitud", 4.6097)),
                "longitude": float(row.get("longitud", -74.0817))
            }

            # Enviar el mensaje al tópico de Kafka
            producer.send(topic_name, value=evento)
            print(f"Evento enviado -> Localidad: {evento['locality']} | Gravedad: {evento['severity']}")

            # Pausa de 1 a 2 segundos para simular el flujo en tiempo real
            time.sleep(1.5)

    except KeyboardInterrupt:
        print("\nTransmisión detenida manualmente por el usuario.")
    finally:
        # Asegurar el cierre del productor al terminar
        producer.close()
        print("Productor de Kafka cerrado correctamente.")

if __name__ == "__main__":
    main()