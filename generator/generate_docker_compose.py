import json
import yaml
import os

def generate_compose(json_path, output_path):
    # Lade das JSON-File
    with open(json_path, 'r', encoding='utf-8') as f:
        track_data = json.load(f)

    # Starte Grundstruktur für docker-compose.yml
    compose = {
        'services': {},
        'networks': {
            'race-network': {
                'driver': 'bridge'
            }
        }
    }

    # Zookeeper
    compose['services']['zookeeper'] = {
        'image': 'bitnami/zookeeper:latest',
        'ports': ['2181:2181'],
        'environment': [
            'ALLOW_ANONYMOUS_LOGIN=yes'
        ],
        'networks': ['race-network']
    }

    # Kafka mit INTERN und EXTERN Listener
    compose['services']['kafka'] = {
        'image': 'bitnami/kafka:latest',
        'ports': [
            '9092:9092',   # Optional für internen Zugriff debuggen
            '29092:29092'  # Externer Zugriff (Host -> Kafka)
        ],
        'environment': [
            'KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181',
            'ALLOW_PLAINTEXT_LISTENER=yes',
            # Parallele Listener-Config
            'KAFKA_CFG_LISTENERS=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:29092',
            'KAFKA_CFG_ADVERTISED_LISTENERS=INTERNAL://kafka:9092,EXTERNAL://localhost:29092',
            'KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT',
            'KAFKA_CFG_INTER_BROKER_LISTENER_NAME=INTERNAL'
        ],
        'depends_on': ['zookeeper'],
        'networks': ['race-network']
    }

    # Segment-Worker-Services
    for track in track_data.get('tracks', []):
        track_id = track.get('trackId')
        for segment in track.get('segments', []):
            segment_id = segment.get('segmentId')
            next_segments = ','.join(segment.get('nextSegments', []))

            service_name = f"race-{track_id}-{segment_id}"
            compose['services'][service_name] = {
                'image': 'segment-worker:latest',
                'environment': [
                    f"TRACK_ID={track_id}",
                    f"SEGMENT_ID={segment_id}",
                    f"NEXT_SEGMENTS={next_segments}",
                    "KAFKA_BROKER=kafka:9092"  # INTERNER Zugriff!
                ],
                'depends_on': ['kafka'],
                'networks': ['race-network']
            }

    # Schreibe die docker-compose.yml
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        yaml.dump(compose, f, sort_keys=False)

    print(f"Docker Compose erfolgreich erzeugt: {output_path}")


if __name__ == "__main__":
    # Pfade zu den Dateien
    json_input = "config/track_config.json"
    compose_output = "deployment/docker-compose.yml"

    generate_compose(json_input, compose_output)
