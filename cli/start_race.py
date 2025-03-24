import argparse
import os
import time
import random
import sys

def run(cmd):
    print(f"{cmd}")
    result = os.system(cmd)
    if result != 0:
        print(f"Fehler bei Befehl: {cmd}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Starte das Wagenrennen mit sicherem Kafka-Setup")
    parser.add_argument("--tracks", type=int, required=True, help="Anzahl der Tracks / Rennfahrer")
    parser.add_argument("--segments", type=int, required=True, help="Anzahl der Segmente pro Track")
    parser.add_argument("--laps", type=int, default=3, help="Anzahl der Runden")
    parser.add_argument("--brokers", type=int, default=3, help="Anzahl der Broker")
    args = parser.parse_args()

    # 1Strecken-JSON generieren
    print("Erzeuge Streckenbeschreibung...")
    run(f"python3 generator/circular-course.py {args.tracks} {args.segments} config/track_config.json")

    # 2 Kafka-Setup-Skript generieren
    print("Erzeuge Kafka-Setup-Skript...")
    run(f"python3 generator/kafka_setup.py --brokers {args.brokers}")
    
    # 3 Docker-Compose-Datei generieren
    print("Generiere Docker-Compose-Datei...")
    run(f"python3 generator/generate_docker_compose.py --brokers {args.brokers}")


    # 4 Existenz der Docker-Compose-Datei prüfen
    if not os.path.exists("deployment/docker-compose.yml"):
        print("docker-compose.yml fehlt. Abbruch.")
        sys.exit(1)

    # 5 Segment-Worker Image bauen (zuerst!)
    print("Baue Segment-Worker Docker-Image...")
    run("docker build -t segment-worker ./workers")

    # 6 Docker-Compose hochfahren mit Build
    print("Starte Docker-Compose inkl. Build...")
    run("docker-compose -f deployment/docker-compose.yml up -d --build")

    # 7 Kafka-Container-ID holen
    print("Ermittle Kafka-Container...")
    # kafka_container_id = os.popen("docker-compose -f deployment/docker-compose.yml ps -q kafka-1").read().strip()
    kafka_container_id = os.popen("docker-compose -f deployment/docker-compose.yml ps -q | head -n 1").read().strip()
    if not kafka_container_id:
        print("Kafka-Container nicht gefunden. Abbruch.")
        sys.exit(1)
    print(f"Kafka-Container: {kafka_container_id}")

    # 8 Kafka-Setup sicher ausführen
    print("Führe Kafka-Setup im Container aus...")
    run(f"docker cp deployment/kafka-setup.sh {kafka_container_id}:/tmp/kafka-setup.sh")
    run(f"docker exec -i {kafka_container_id} bash /tmp/kafka-setup.sh")


    # 9 Pro Track Racer starten
    print("Feuere Start-Token pro Racer (mit Zufalls-Delay)...\n")
    for track_id in range(1, args.tracks + 1):
        racer_id = f"R{track_id}"
        delay = round(random.uniform(0.5, 2.5), 2)
        print(f"Warte {delay} Sekunden... ➔ Starte Racer {racer_id} auf Track {track_id}")
        time.sleep(delay)
        run(f"venv/bin/python cli/fire_token.py --track {track_id} --racer {racer_id} --laps {args.laps}")

    print("\nAlle Racer sind gestartet – das Rennen läuft!")

if __name__ == "__main__":
    main()
