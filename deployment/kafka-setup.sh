#!/bin/bash

# Kafka Setup Script - Auto-generated
# Bootstrap Server: localhost:9092

kafka-topics.sh --create --topic race.1.segment.segment-1-1 --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
kafka-topics.sh --create --topic race.1.segment.segment-1-2 --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
kafka-topics.sh --create --topic race.1.segment.segment-1-3 --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
kafka-topics.sh --create --topic race.1.segment.segment-1-4 --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
kafka-topics.sh --create --topic race.1.segment.start-and-goal-1 --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
kafka-topics.sh --create --topic race.2.segment.segment-2-1 --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
kafka-topics.sh --create --topic race.2.segment.segment-2-2 --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
kafka-topics.sh --create --topic race.2.segment.segment-2-3 --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
kafka-topics.sh --create --topic race.2.segment.segment-2-4 --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
kafka-topics.sh --create --topic race.2.segment.start-and-goal-2 --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
kafka-topics.sh --create --topic race.3.segment.segment-3-1 --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
kafka-topics.sh --create --topic race.3.segment.segment-3-2 --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
kafka-topics.sh --create --topic race.3.segment.segment-3-3 --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
kafka-topics.sh --create --topic race.3.segment.segment-3-4 --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
kafka-topics.sh --create --topic race.3.segment.start-and-goal-3 --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
