import os
import json
from datetime import datetime, timezone
import random
import time
from kafka import KafkaConsumer, KafkaProducer
from dateutil import parser

# ENV-Variablen laden
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-1:9092,kafka-2:9092,kafka-3:9092").split(",")
TRACK_ID = os.getenv("TRACK_ID")
SEGMENT_ID = os.getenv("SEGMENT_ID")
NEXT_SEGMENTS = os.getenv("NEXT_SEGMENTS", "")

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
        print(f"[{SEGMENT_ID}] Token empfangen: {token}")

        # History initialisieren und erweitern
        token.setdefault('history', [])
        token['history'].append(SEGMENT_ID)

        # Initiale Felder sicherstellen
        token.setdefault('labCount', 0)
        token.setdefault('totalLaps', 3)
        token.setdefault('racerId', 'Unknown')
        token.setdefault('raceStarted', False)
        token.setdefault('caesarGreeted', False)

        # Starte Rennen bei erstem Besuch der Startlinie, aber ohne Rundenzählung
        if SEGMENT_ID.startswith("start-and-goal"):
            if not token['raceStarted']:
                token['raceStarted'] = True
                print(f"Startsignal erhalten. Rennen beginnt für {token['racerId']}.")
            else:
                token['labCount'] += 1
                print(f"Neue Runde abgeschlossen! Aktuelle Runde: {token['labCount']} / {token['totalLaps']}")

                if token['labCount'] == 1:
                    if random.random() < 0.5:
                        token['caesarGreeted'] = True
                        print(f"{token['racerId']} grüßt Caesar nach Runde 1")
                    else:
                        print(f"{token['racerId']} grüßt Caesar noch nicht")

                if token['labCount'] == 2 and not token['caesarGreeted']:
                    token['caesarGreeted'] = True
                    print(f"{token['racerId']} grüßt Caesar nach Runde 2 (erzwungen)")

        # Rennen beenden, wenn Max-Runden erreicht
        if token['labCount'] >= token['totalLaps']:
            token['endTime'] = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
            try:
                start = parser.isoparse(token['startTime'])
                end = parser.isoparse(token['endTime'])
                token['durationMs'] = int((end - start).total_seconds() * 1000)
            except Exception as e:
                print(f"Fehler bei der Zeitberechnung: {e}")
                token['durationMs'] = -1

            print(f"[{SEGMENT_ID}] Rennen beendet für {token['racerId']} nach {token['labCount']} Runden.")
            print(f"Gesamtlaufzeit: {token['durationMs']} ms")
            print(f"Gefahrene Strecke: {token['history']}")
            print(f"Finaler Token: {json.dumps(token, indent=2)}")
            continue  # Kein Weiterleiten mehr

        # Speziallogik für Abzweigung Kaisergasse oder Start-Ziel
        if len(NEXT_SEGMENTS.split(",")) == 2 and any("kaisergasse" in seg for seg in NEXT_SEGMENTS.split(",")):
            if (token['labCount'] in [1, 2]) and token['caesarGreeted'] and "kaisergasse" not in token['history']:
                # Caesar wird gegrüßt ➔ Kaisergasse
                for seg, topic in zip(NEXT_SEGMENTS.split(","), NEXT_TOPICS):
                    if "kaisergasse" in seg:
                        producer.send(topic, token)
                        print(f"[{SEGMENT_ID}] {token['racerId']} fährt in die Kaisergasse ➔ {topic}")
            else:
                # Kein Gruß ➔ normale Route
                for seg, topic in zip(NEXT_SEGMENTS.split(","), NEXT_TOPICS):
                    if "start-and-goal" in seg:
                        producer.send(topic, token)
                        print(f"[{SEGMENT_ID}] {token['racerId']} fährt über Start/Ziel ➔ {topic}")
        else:
            # Normale Weiterleitung
            for next_topic in NEXT_TOPICS:
                producer.send(next_topic, token)
                print(f"[{SEGMENT_ID}] Token weitergeleitet an {next_topic}")
        producer.flush()

except KeyboardInterrupt:
    print(f"[{SEGMENT_ID}] Stop durch Benutzer.")
finally:
    consumer.close()
    producer.close()
