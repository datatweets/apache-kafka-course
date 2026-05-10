# ============================================================
#  Create all course topics on the 3-broker KRaft cluster
#  Run after: docker compose up -d
#  Usage from docker/: powershell -ExecutionPolicy Bypass -File scripts/create-topics.ps1
# ============================================================

$Bootstrap = "localhost:9092"
$Replication = 3
$Partitions = 3

$Topics = @(
    "m4-simple-topic",
    "m4-partitioned-topic",
    "m4-keyed-topic",
    "m5-consumer-topic",
    "m5-group-topic",
    "m5-offsets-topic",
    "m6-replication-topic",
    "m6-isr-topic",
    "m7-reliable-topic",
    "m7-exactly-once-topic",
    "m8-pipeline-topic",
    "m9-mysql-customers",
    "m9-mysql-orders",
    "m9-elasticsearch-sink",
    "m11-admin-topic",
    "m11-dynamic-config-topic",
    "m12-metrics-topic",
    "m13-stream-input",
    "m13-stream-output",
    "m13-wordcount-input",
    "m13-wordcount-output"
)

Write-Host ""
Write-Host "=============================================="
Write-Host "  Creating Kafka Course Topics"
Write-Host "  Bootstrap: $Bootstrap"
Write-Host "  Replication: $Replication  Partitions: $Partitions"
Write-Host "=============================================="
Write-Host ""

$Success = 0
$Failed = 0

foreach ($Topic in $Topics) {
    Write-Host -NoNewline "  Creating: $Topic ... "

    docker exec kafka1 /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server $Bootstrap `
        --create `
        --if-not-exists `
        --topic $Topic `
        --partitions $Partitions `
        --replication-factor $Replication *> $null

    if ($LASTEXITCODE -eq 0) {
        Write-Host "OK"
        $Success += 1
    } else {
        Write-Host "FAILED"
        $Failed += 1
    }
}

Write-Host ""
Write-Host "=============================================="
Write-Host "  Done: $Success created, $Failed failed"
Write-Host "=============================================="
Write-Host ""
Write-Host "  View topics in Kafdrop: http://localhost:9000"
Write-Host ""
