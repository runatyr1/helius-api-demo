# Kafka Queue

Message broker for transaction data pipeline using Kafka KRaft (no Zookeeper).

Deployed in kubernetes infra repo. The 2-queue folder is for documentation purpose.

## Purpose

Decouples producer and consumer components:
- **Input**: Transactions from producer instances
- **Output**: Transactions to consumer instances
- **Topic**: `transactions`

## Architecture

- **KRaft mode**: Self-managed metadata (no Zookeeper dependency)
- **Single broker**: Suitable for demo/dev workloads
- **Port**: 9092 (ClusterIP service)

## Configuration

Set in `helius-api-demo-envs` secret:
```
KAFKA_BOOTSTRAP_SERVERS=kafka.helius-api-demo.svc.cluster.local:9092
KAFKA_TOPIC=transactions
```

## Scaling

Current setup uses single broker. For production:
- Scale to 3+ brokers for HA
- Configure replication factor (currently 1)
- Add resource limits based on throughput
