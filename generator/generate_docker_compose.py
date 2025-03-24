import argparse
import json
import yaml
import os

def generate_compose(json_path, output_path, number_of_brokers):
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
    for i in range(1, number_of_brokers + 1):
        compose['services'][f'kafka-{i}'] = {
            'image': 'bitnami/kafka:latest',
            'ports': [f'{9091 + i}:9092', f'{29091 + i}:29092'],
            'environment': [
                f'KAFKA_BROKER_ID={i}',
                'KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181',
                'ALLOW_PLAINTEXT_LISTENER=yes',
                # >>> WICHTIG: Parallele Listener für intern und extern
                'KAFKA_CFG_LISTENERS=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:29092',
                f'KAFKA_CFG_ADVERTISED_LISTENERS=INTERNAL://kafka-{i}:9092,EXTERNAL://localhost:{29091 + i}',
                'KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT',
                'KAFKA_CFG_INTER_BROKER_LISTENER_NAME=INTERNAL',
                # Optional: Cluster-Parameter
                'KAFKA_CFG_DEFAULT_REPLICATION_FACTOR=3',
                'KAFKA_CFG_MIN_INSYNC_REPLICAS=2',
                'KAFKA_CFG_NUM_PARTITIONS=3'
            ],
            'depends_on': ['zookeeper'],
            'networks': ['race-network']
        }
        
    # Dynamischer Kafka-Bootstrap-String für alle Segment-Worker
    kafka_bootstrap = ",".join([f'kafka-{i}:9092' for i in range(1, number_of_brokers + 1)])

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
                    f"KAFKA_BROKER={kafka_bootstrap}"
                ],
                'depends_on': [f'kafka-{i}' for i in range(1, number_of_brokers + 1)],
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

    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", type=int, required=True, help="Anzahl Kafka Broker")
    args = parser.parse_args()

    generate_compose(json_input, compose_output, args.brokers)
