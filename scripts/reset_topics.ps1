$ErrorActionPreference = "Stop"

$topics = @(
  "university-regulations-raw",
  "university-regulations-valid",
  "university-regulations-dlq"
)

foreach ($topic in $topics) {
  docker exec university-kafka /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --delete --topic $topic 2>$null
}

Start-Sleep -Seconds 3

foreach ($topic in $topics) {
  docker exec university-kafka /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --create --if-not-exists --topic $topic `
    --partitions 1 --replication-factor 1
}

docker exec university-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 --list

Write-Host "Kafka topics reset successfully."
