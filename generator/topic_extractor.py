import json
from typing import List, Set

def extract_kafka_topics_from_json(json_file_path: str) -> List[str]:
    """
    Liest die track_config.json ein und extrahiert alle benötigten Kafka-Topics
    nach dem Schema: race.<trackId>.segment.<segmentId>

    :param json_file_path: Pfad zur Strecken-JSON
    :return: Alphabetisch sortierte Liste aller Topics
    """
    with open(json_file_path, 'r', encoding='utf-8') as f:
        track_data = json.load(f)

    topics: Set[str] = set()

    for track in track_data.get("tracks", []):
        track_id = track.get("trackId")
        if not track_id:
            raise ValueError("Fehlende trackId in einem Track-Block")

        for segment in track.get("segments", []):
            segment_id = segment.get("segmentId")
            if not segment_id:
                raise ValueError(f"Fehlende segmentId in track {track_id}")
            # Kafka-Topic-Schema anwenden
            kafka_topic = f"race.{track_id}.segment.{segment_id}"
            topics.add(kafka_topic)

    return sorted(topics)


if __name__ == "__main__":
    # Testlauf
    import os
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(BASE_DIR, "../config/track_config.json")

    topics = extract_kafka_topics_from_json(json_path)
    print("Erkannte Kafka-Topics:")
    for topic in topics:
        print(topic)
