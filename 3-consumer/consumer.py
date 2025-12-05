#!/usr/bin/env python3
import os
import time
import json
import psycopg2
from kafka import KafkaConsumer
from prometheus_client import start_http_server, Counter, Histogram, Gauge

# Prometheus metrics
messages_consumed = Counter('messages_consumed_total', 'Total messages consumed from Kafka')
messages_inserted = Counter('messages_inserted_total', 'Total messages inserted to PostgreSQL')
insert_errors = Counter('insert_errors_total', 'Total insert errors')
batch_insert_duration = Histogram('batch_insert_duration_seconds', 'Time spent inserting batches')
kafka_lag = Gauge('kafka_consumer_lag', 'Current consumer lag')
batch_size_gauge = Gauge('consumer_batch_size', 'Current batch size')
kafka_connection_errors = Counter('kafka_connection_errors_total', 'Kafka connection errors')

# Configuration from environment
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'transactions')
CONSUMER_GROUP_ID = os.getenv('CONSUMER_GROUP_ID', 'transaction-consumer')
BATCH_SIZE = int(os.getenv('CONSUMER_BATCH_SIZE', '100'))
METRICS_PORT = int(os.getenv('CONSUMER_METRICS_PORT', '8001'))

DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'solana')
DB_USER = os.getenv('DB_USER', 'solana')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'solana123')

def connect_db():
    """Connect to PostgreSQL with retry"""
    retries = 5
    for i in range(retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            print(f"Connected to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}")
            return conn
        except Exception as e:
            print(f"PostgreSQL connection attempt {i+1}/{retries} failed: {e}")
            if i < retries - 1:
                time.sleep(2 ** i)  # Exponential backoff
            else:
                raise

def connect_kafka():
    """Connect to Kafka with retry"""
    retries = 5
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=CONSUMER_GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',  # Start from beginning if no offset
                enable_auto_commit=False,  # Manual commit after successful insert
                max_poll_records=BATCH_SIZE
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            print(f"Subscribed to topic: {KAFKA_TOPIC}, group: {CONSUMER_GROUP_ID}")
            return consumer
        except Exception as e:
            kafka_connection_errors.inc()
            print(f"Kafka connection attempt {i+1}/{retries} failed: {e}")
            if i < retries - 1:
                time.sleep(2 ** i)  # Exponential backoff
            else:
                raise

def batch_insert_transactions(conn, transactions):
    """Batch insert transactions to PostgreSQL"""
    if not transactions:
        return 0

    try:
        with batch_insert_duration.time():
            cursor = conn.cursor()

            # Prepare batch insert query
            insert_query = """
                INSERT INTO transactions
                (signature, generator_id, sequence_id, slot, block_time, fee, success, data)
                VALUES %s
                ON CONFLICT (signature) DO NOTHING
            """

            # Build values list
            values = []
            for tx in transactions:
                values.append((
                    tx['signature'],
                    tx['generator_id'],
                    tx['sequence_id'],
                    tx['slot'],
                    tx['block_time'],
                    tx['fee'],
                    tx['success'],
                    json.dumps(tx['data'])
                ))

            # Use execute_values for efficient batch insert
            from psycopg2.extras import execute_values
            execute_values(cursor, insert_query, values)

            conn.commit()
            cursor.close()

            inserted = len(transactions)
            messages_inserted.inc(inserted)
            return inserted

    except Exception as e:
        insert_errors.inc()
        print(f"Error inserting batch: {e}")
        conn.rollback()
        raise

def main():
    print(f"Starting transaction consumer...")
    print(f"Config: batch_size={BATCH_SIZE}")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}, topic={KAFKA_TOPIC}, group={CONSUMER_GROUP_ID}")
    print(f"PostgreSQL: {DB_HOST}:{DB_PORT}/{DB_NAME}")

    # Start Prometheus metrics server
    start_http_server(METRICS_PORT)
    print(f"Metrics server started on port {METRICS_PORT}")

    # Connect to services
    conn = connect_db()
    consumer = connect_kafka()
    batch_size_gauge.set(BATCH_SIZE)

    print("Starting message consumption loop...")
    batch = []

    try:
        while True:
            # Poll for messages
            message_batch = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)

            if not message_batch:
                # No messages, insert pending batch if exists
                if batch:
                    print(f"Timeout reached, inserting pending batch of {len(batch)} messages")
                    inserted = batch_insert_transactions(conn, batch)
                    print(f"Inserted {inserted} transactions")
                    consumer.commit()
                    batch = []
                continue

            # Process messages from all partitions
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    messages_consumed.inc()
                    batch.append(message.value)

                    # Insert when batch is full
                    if len(batch) >= BATCH_SIZE:
                        inserted = batch_insert_transactions(conn, batch)
                        print(f"Inserted batch of {inserted} transactions (offsets {message.offset - len(batch) + 1} - {message.offset})")
                        consumer.commit()
                        batch = []

    except KeyboardInterrupt:
        print("Shutting down...")
    except Exception as e:
        print(f"Error in main loop: {e}")
        raise
    finally:
        # Insert remaining messages
        if batch:
            print(f"Inserting final batch of {len(batch)} messages")
            try:
                batch_insert_transactions(conn, batch)
                consumer.commit()
            except Exception as e:
                print(f"Error inserting final batch: {e}")

        consumer.close()
        conn.close()
        print("Consumer stopped")

if __name__ == '__main__':
    main()
