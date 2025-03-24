from kafka import KafkaProducer
import json

KAFKA_BROKER = "localhost:9092"   # Wenn du außerhalb des Containers bist!
TOPIC = "race.1.segment.start-and-goal-1"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    # api_version=(2, 7, 0)  # Bitnami Kafka >2.7
)

message = {"test": "Hallo Kafka, läuft es?"}

try:
    future = producer.send(TOPIC, value=message)
    metadata = future.get(timeout=10)
    print(f"✅ Nachricht erfolgreich gesendet ➔ Partition {metadata.partition}, Offset {metadata.offset}")
except Exception as e:
    print(f"❌ Fehler beim Senden: {e}")
finally:
    producer.flush()
    producer.close()
