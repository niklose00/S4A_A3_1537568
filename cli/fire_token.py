import os
import argparse
import json
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer, errors as kafka_errors

KAFKA_BROKER = "localhost:9092"

def create_token(racer_id: str, track_id: str, total_laps: int) -> dict:
    token = {
        "tokenId": str(uuid.uuid4()),
        "racerId": racer_id,
        "trackId": track_id,
        "currentLap": 0,
        "totalLaps": total_laps,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    print(f"🪪 Token erstellt: {json.dumps(token, indent=2)}")
    return token

def publish_token(track_id: str, racer_id: str, laps: int):
    print(f"📡 Verbinde mit Kafka-Broker: {KAFKA_BROKER}")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            api_version=(0, 11, 5),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3
        )
    except kafka_errors.KafkaError as e:
        print(f"❌ Kafka-Verbindungsfehler: {e}")
        return

    token = create_token(racer_id, track_id, laps)

    # Debug-Ausgabe des Topic-Namens
    start_topic = f"race.{track_id}.segment.start-and-goal-{track_id}"
    print(f"📝 Erwartetes Topic: {start_topic}")

    try:
        print(f"🏎️ Sende Token für {racer_id} an Topic: {start_topic}")
        future = producer.send(start_topic, value=token)
        record_metadata = future.get(timeout=10)
        print(f"✅ Token gesendet an Partition {record_metadata.partition}, Offset {record_metadata.offset}")
    except kafka_errors.UnknownTopicOrPartitionError as e:
        print(f"❌ Fehler: Topic '{start_topic}' existiert nicht! Kafka-Fehler: {e}")
    except kafka_errors.KafkaTimeoutError as e:
        print(f"⏳ Kafka Timeout beim Senden: {e}")
    except kafka_errors.KafkaError as e:
        print(f"❌ Allgemeiner Kafka-Fehler beim Senden: {e}")
    finally:
        producer.flush()
        producer.close()
        print(f"✅ Producer geschlossen")

def main():
    parser = argparse.ArgumentParser(description="🚀 Feuert einen Start-Token für einen Racer")
    parser.add_argument("--track", required=True, help="Track ID (z.B. 1)")
    parser.add_argument("--racer", required=True, help="Racer ID (z.B. R1)")
    parser.add_argument("--laps", type=int, default=3, help="Anzahl der Runden")
    args = parser.parse_args()

    print(f"🔎 Starte Fire-Token mit Args: track={args.track}, racer={args.racer}, laps={args.laps}")
    publish_token(track_id=args.track, racer_id=args.racer, laps=args.laps)

if __name__ == "__main__":
    main()
