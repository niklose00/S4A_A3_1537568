import json
from topic_extractor import extract_kafka_topics_from_json

KAFKA_BOOTSTRAP_SERVER = "kafka:9092"
DEFAULT_PARTITIONS = 1
DEFAULT_REPLICATION = 1

def generate_kafka_setup_script(json_file_path: str, output_file_path: str):
    topics = extract_kafka_topics_from_json(json_file_path)

    with open(output_file_path, "w", encoding="utf-8") as f:
        f.write("#!/bin/bash\n\n")
        f.write(f"# Kafka Setup Script - Auto-generated\n")
        f.write(f"# Bootstrap Server: {KAFKA_BOOTSTRAP_SERVER}\n\n")

        for topic in topics:
            cmd = (
                f"kafka-topics.sh --create "
                f"--topic {topic} "
                f"--partitions {DEFAULT_PARTITIONS} "
                f"--replication-factor {DEFAULT_REPLICATION} "
                f"--if-not-exists "
                f"--bootstrap-server {KAFKA_BOOTSTRAP_SERVER}"
            )
            f.write(cmd + "\n")

    print(f"✅ Kafka setup script written to {output_file_path}")

if __name__ == "__main__":
    import os
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(BASE_DIR, "../config/track_config.json")
    output_path = os.path.join(BASE_DIR, "../deployment/kafka-setup.sh")
    
    generate_kafka_setup_script(
        json_file_path=json_path,
        output_file_path=output_path
    )
