import argparse
import json
from topic_extractor import extract_kafka_topics_from_json

KAFKA_BOOTSTRAP_SERVER = "kafka:9092"
DEFAULT_PARTITIONS = 3
MIN_REPLICATION = 3

def generate_kafka_setup_script(json_file_path: str, output_file_path: str, number_of_brokers: int):
    topics = extract_kafka_topics_from_json(json_file_path)
    
    # Dynamischer Kafka Bootstrap
    kafka_bootstrap = ",".join([f"kafka-{i}:9092" for i in range(1, number_of_brokers + 1)])
    replication_factor = max(MIN_REPLICATION, min(number_of_brokers, MIN_REPLICATION))

    with open(output_file_path, "w", encoding="utf-8") as f:
        f.write("#!/bin/bash\n\n")
        f.write("set -e\n\n")  # Fehlerhart
        f.write(f"# Kafka Setup Script - Auto-generated\n")
        f.write(f"# Bootstrap Servers: {kafka_bootstrap}\n\n")

        for topic in topics:
            cmd = (
                f"kafka-topics.sh --create "
                f"--topic {topic} "
                f"--partitions {DEFAULT_PARTITIONS} "
                f"--replication-factor {replication_factor} "
                f"--if-not-exists "
                f"--bootstrap-server {kafka_bootstrap}"
            )
            f.write(cmd + "\n")

    print(f"✅ Kafka setup script written to {output_file_path}")

if __name__ == "__main__":
    import os
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(BASE_DIR, "../config/track_config.json")
    output_path = os.path.join(BASE_DIR, "../deployment/kafka-setup.sh")
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", type=int, required=True, help="Anzahl Kafka Broker")
    args = parser.parse_args()
    
    generate_kafka_setup_script(
        json_file_path=json_path,
        output_file_path=output_path,
        number_of_brokers=args.brokers
    )
