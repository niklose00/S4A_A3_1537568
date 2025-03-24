import os
import json
from datetime import datetime, timezone
import time
from kafka import KafkaConsumer, KafkaProducer
from dateutil import parser

# ENV-Variablen laden
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-1:9092,kafka-2:9092,kafka-3:9092").split(",")
TRACK_ID = os.getenv("TRACK_ID")
SEGMENT_ID = os.getenv("SEGMENT_ID")
NEXT_SEGMENTS = os.getenv("NEXT_SEGMENTS", "")

print(NEXT_SEGMENTS)

if not TRACK_ID or not SEGMENT_ID:
    raise ValueError("ERROR: TRACK_ID und SEGMENT_ID müssen als Umgebungsvariablen gesetzt sein!")

TOPIC_NAME = f"race.{TRACK_ID}.segment.{SEGMENT_ID}"
NEXT_TOPICS = [f"race.{TRACK_ID}.segment.{seg.strip()}" for seg in NEXT_SEGMENTS.split(",") if seg.strip()]

print(f"Worker '{SEGMENT_ID}' läuft. Lauscht auf Topic: {TOPIC_NAME}")
print(f"Weiterleitung an: {NEXT_TOPICS if NEXT_TOPICS else 'End-Segment (keine Weiterleitung)'}")

# Kafka-Consumer und Producer mit Retry
for i in range(10):
    try:
        print(f"Verbindung zu Kafka-Broker: {KAFKA_BROKER} (Versuch {i+1})")
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            group_id=f"group-{TRACK_ID}-{SEGMENT_ID}",
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Kafka verbunden.")
        break
    except Exception as e:
        print(f"Fehler bei Kafka-Verbindung: {e}")
        time.sleep(2)
else:
    print("Kafka nicht erreichbar. Worker bricht ab.")
    exit(1)

try:
    for message in consumer:
        token = message.value
        print(f"📥 [{SEGMENT_ID}] Token empfangen: {token}")

        # Initiale Felder sicherstellen
        token.setdefault('labCount', 0)
        token.setdefault('totalLaps', 3)
        token.setdefault('racerId', 'Unknown')
        token.setdefault('raceStarted', False)

        # Starte Rennen bei erstem Besuch der Startlinie, aber ohne Rundenzählung
        if SEGMENT_ID.startswith("start-and-goal"):
            if not token['raceStarted']:
                token['raceStarted'] = True
                print(f"🚀 Startsignal erhalten. Rennen beginnt für {token['racerId']}.")
            else:
                # Jetzt ist es eine "Zieldurchfahrt" → Runde hochzählen
                token['labCount'] += 1
                print(f"Neue Runde abgeschlossen! Aktuelle Runde: {token['labCount']} / {token['totalLaps']}")

        # Rennen beenden, wenn Max-Runden erreicht
        if token['labCount'] >= token['totalLaps']:
            # Endzeit setzen (UTC)
            token['endTime'] = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

            # Startzeit holen und Laufzeit berechnen
            from datetime import datetime
            try:
                start = parser.isoparse(token['startTime'])
                end = parser.isoparse(token['endTime'])
                token['durationMs'] = int((end - start).total_seconds() * 1000)
            except Exception as e:
                print(f"Fehler bei der Zeitberechnung: {e}")
                token['durationMs'] = -1  # Fehlercode

            print(f"[{SEGMENT_ID}] Rennen beendet für {token['racerId']} nach {token['labCount']} Runden.")
            print(f"Gesamtlaufzeit: {token['durationMs']} ms")
            print(f"Finaler Token: {token}")
            continue  # Kein Weiterleiten mehr


        # Weiterleiten an die nächsten Segmente
        if not NEXT_TOPICS:
            print(f"[{SEGMENT_ID}] End-Segment erreicht. Token: {token}")
            continue

        for next_topic in NEXT_TOPICS:
            producer.send(next_topic, token)
            print(f"[{SEGMENT_ID}] Token weitergeleitet an {next_topic}")
        producer.flush()

except KeyboardInterrupt:
    print(f"[{SEGMENT_ID}] Stop durch Benutzer.")
finally:
    consumer.close()
    producer.close()
