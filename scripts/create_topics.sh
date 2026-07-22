#!/usr/bin/env bash
set -euo pipefail
BROKER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
for topic in university-regulations-raw university-regulations-valid university-regulations-dlq; do
  docker exec university-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
done
docker exec university-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
