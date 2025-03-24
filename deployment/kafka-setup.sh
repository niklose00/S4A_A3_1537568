#!/bin/bash

set -e

# Kafka Setup Script - Auto-generated
# Bootstrap Servers: kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092,kafka-7:9092,kafka-8:9092,kafka-9:9092,kafka-10:9092

kafka-topics.sh --create --topic race.1.segment.segment-1-1 --partitions 3 --replication-factor 3 --if-not-exists --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092,kafka-7:9092,kafka-8:9092,kafka-9:9092,kafka-10:9092
kafka-topics.sh --create --topic race.1.segment.segment-1-2 --partitions 3 --replication-factor 3 --if-not-exists --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092,kafka-7:9092,kafka-8:9092,kafka-9:9092,kafka-10:9092
kafka-topics.sh --create --topic race.1.segment.segment-1-3 --partitions 3 --replication-factor 3 --if-not-exists --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092,kafka-7:9092,kafka-8:9092,kafka-9:9092,kafka-10:9092
kafka-topics.sh --create --topic race.1.segment.segment-1-4 --partitions 3 --replication-factor 3 --if-not-exists --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092,kafka-7:9092,kafka-8:9092,kafka-9:9092,kafka-10:9092
kafka-topics.sh --create --topic race.1.segment.start-and-goal-1 --partitions 3 --replication-factor 3 --if-not-exists --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092,kafka-7:9092,kafka-8:9092,kafka-9:9092,kafka-10:9092
kafka-topics.sh --create --topic race.2.segment.segment-2-1 --partitions 3 --replication-factor 3 --if-not-exists --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092,kafka-7:9092,kafka-8:9092,kafka-9:9092,kafka-10:9092
kafka-topics.sh --create --topic race.2.segment.segment-2-2 --partitions 3 --replication-factor 3 --if-not-exists --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092,kafka-7:9092,kafka-8:9092,kafka-9:9092,kafka-10:9092
kafka-topics.sh --create --topic race.2.segment.segment-2-3 --partitions 3 --replication-factor 3 --if-not-exists --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092,kafka-7:9092,kafka-8:9092,kafka-9:9092,kafka-10:9092
kafka-topics.sh --create --topic race.2.segment.segment-2-4 --partitions 3 --replication-factor 3 --if-not-exists --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092,kafka-7:9092,kafka-8:9092,kafka-9:9092,kafka-10:9092
kafka-topics.sh --create --topic race.2.segment.start-and-goal-2 --partitions 3 --replication-factor 3 --if-not-exists --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092,kafka-7:9092,kafka-8:9092,kafka-9:9092,kafka-10:9092
kafka-topics.sh --create --topic race.3.segment.segment-3-1 --partitions 3 --replication-factor 3 --if-not-exists --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092,kafka-7:9092,kafka-8:9092,kafka-9:9092,kafka-10:9092
kafka-topics.sh --create --topic race.3.segment.segment-3-2 --partitions 3 --replication-factor 3 --if-not-exists --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092,kafka-7:9092,kafka-8:9092,kafka-9:9092,kafka-10:9092
kafka-topics.sh --create --topic race.3.segment.segment-3-3 --partitions 3 --replication-factor 3 --if-not-exists --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092,kafka-7:9092,kafka-8:9092,kafka-9:9092,kafka-10:9092
kafka-topics.sh --create --topic race.3.segment.segment-3-4 --partitions 3 --replication-factor 3 --if-not-exists --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092,kafka-7:9092,kafka-8:9092,kafka-9:9092,kafka-10:9092
kafka-topics.sh --create --topic race.3.segment.start-and-goal-3 --partitions 3 --replication-factor 3 --if-not-exists --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092,kafka-7:9092,kafka-8:9092,kafka-9:9092,kafka-10:9092
