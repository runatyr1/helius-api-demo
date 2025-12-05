#!/usr/bin/env python3
import os
import time
import random
import string
import json
import socket
from pathlib import Path
from kafka import KafkaProducer
from prometheus_client import start_http_server, Counter, Histogram, Gauge

# Prometheus metrics
tx_generated = Counter('transactions_generated_total', 'Total transactions generated')
tx_published = Counter('transactions_published_total', 'Total transactions published to Kafka')
tx_errors = Counter('transactions_errors_total', 'Total transaction publication errors')
publish_duration = Histogram('transaction_publish_duration_seconds', 'Time spent publishing transactions')
batch_size_gauge = Gauge('transaction_batch_size', 'Current batch size')
kafka_connection_errors = Counter('kafka_connection_errors_total', 'Kafka connection errors')
sequence_id_gauge = Gauge('sequence_id_current', 'Current sequence ID')

# Configuration from environment
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'transactions')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))
INTERVAL_MS = int(os.getenv('INTERVAL_MS', '400'))
METRICS_PORT = int(os.getenv('METRICS_PORT', '8000'))
STATE_FILE = Path('/data/producer-state.json')

def get_generator_id():
    """Extract generator_id from hostname (StatefulSet ordinal)"""
    hostname = socket.gethostname()
    # hostname format: producer-0, producer-1, producer-2, etc.
    if '-' in hostname and hostname.split('-')[-1].isdigit():
        return int(hostname.split('-')[-1])
    else:
        # Fallback for non-StatefulSet deployments
        return int(os.getenv('GENERATOR_ID', '0'))

def load_state():
    """Load persistent state from file"""
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
                print(f"Loaded state: {state}")
                return state
        except Exception as e:
            print(f"Error loading state: {e}, starting fresh")
    return None

def save_state(generator_id, sequence_id):
    """Atomically save state to file"""
    try:
        # Ensure directory exists
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

        # Write to temp file then rename for atomicity
        temp_file = STATE_FILE.with_suffix('.tmp')
        state = {
            'generator_id': generator_id,
            'sequence_id': sequence_id,
            'last_updated': time.time()
        }

        with open(temp_file, 'w') as f:
            json.dump(state, f)

        # Atomic rename
        temp_file.rename(STATE_FILE)
    except Exception as e:
        print(f"Error saving state: {e}")

def generate_signature():
    """Generate a fake Solana transaction signature (88 chars base58)"""
    chars = string.ascii_letters + string.digits
    return ''.join(random.choices(chars, k=88))

def generate_transaction(generator_id, sequence_id):
    """Generate a fake Solana transaction with generator tracking"""
    # Use timestamp-based slot (not sequential like real Solana, but realistic)
    slot = int(time.time() * 1000)  # Millisecond timestamp as slot
    block_time = int(time.time())
    fee = random.randint(5000, 50000)
    success = random.random() > 0.05  # 95% success rate

    # Fake transaction data
    data = {
        "accounts": [
            ''.join(random.choices(string.ascii_letters + string.digits, k=44))
            for _ in range(random.randint(2, 8))
        ],
        "instructions": [
            {
                "program_id": ''.join(random.choices(string.ascii_letters + string.digits, k=44)),
                "data": ''.join(random.choices(string.hexdigits, k=random.randint(10, 100)))
            }
            for _ in range(random.randint(1, 5))
        ],
        "recent_blockhash": ''.join(random.choices(string.ascii_letters + string.digits, k=44))
    }

    return {
        'signature': generate_signature(),
        'generator_id': generator_id,
        'sequence_id': sequence_id,
        'slot': slot,
        'block_time': block_time,
        'fee': fee,
        'success': success,
        'data': data
    }

def connect_kafka():
    """Connect to Kafka with retry"""
    retries = 5
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Wait for all replicas
                retries=3
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            kafka_connection_errors.inc()
            print(f"Kafka connection attempt {i+1}/{retries} failed: {e}")
            if i < retries - 1:
                time.sleep(2 ** i)  # Exponential backoff
            else:
                raise

def publish_transactions(producer, transactions):
    """Publish a batch of transactions to Kafka"""
    try:
        with publish_duration.time():
            for tx in transactions:
                future = producer.send(KAFKA_TOPIC, value=tx)
                # Wait for send to complete (synchronous for reliability)
                future.get(timeout=10)

            producer.flush()
            tx_published.inc(len(transactions))
            return len(transactions)
    except Exception as e:
        tx_errors.inc()
        print(f"Error publishing transactions: {e}")
        raise

def main():
    print(f"Starting transaction producer...")

    # Get generator_id from hostname
    generator_id = get_generator_id()
    print(f"Generator ID: {generator_id}")

    # Load or initialize state
    state = load_state()
    if state and state.get('generator_id') == generator_id:
        sequence_id = state['sequence_id'] + 1  # Resume from last
        print(f"Resuming from sequence_id: {sequence_id}")
    else:
        sequence_id = 0
        print(f"Starting fresh with sequence_id: {sequence_id}")

    print(f"Config: batch_size={BATCH_SIZE}, interval={INTERVAL_MS}ms")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}, topic={KAFKA_TOPIC}")

    # Start Prometheus metrics server
    start_http_server(METRICS_PORT)
    print(f"Metrics server started on port {METRICS_PORT}")

    # Connect to Kafka
    producer = connect_kafka()
    batch_size_gauge.set(BATCH_SIZE)

    print("Starting transaction generation loop...")
    while True:
        try:
            # Generate batch of transactions
            transactions = []
            for _ in range(BATCH_SIZE):
                tx = generate_transaction(generator_id, sequence_id)
                transactions.append(tx)
                sequence_id += 1

            tx_generated.inc(BATCH_SIZE)
            sequence_id_gauge.set(sequence_id - 1)  # Last sequence_id generated

            # Publish to Kafka
            published = publish_transactions(producer, transactions)
            print(f"Generated and published {published}/{BATCH_SIZE} transactions (seq: {sequence_id - BATCH_SIZE} - {sequence_id - 1})")

            # Save state after successful publish
            save_state(generator_id, sequence_id - 1)

            # Wait for next interval
            time.sleep(INTERVAL_MS / 1000.0)

        except Exception as e:
            tx_errors.inc()
            print(f"Error in main loop: {e}")
            try:
                # Try to reconnect to Kafka
                print("Attempting to reconnect to Kafka...")
                producer.close()
                producer = connect_kafka()
            except Exception as reconnect_error:
                print(f"Failed to reconnect: {reconnect_error}. Retrying in 5s...")
                time.sleep(5)
        except KeyboardInterrupt:
            print("Shutting down...")
            break

    producer.close()
    save_state(generator_id, sequence_id - 1)
    print("Producer stopped")

if __name__ == '__main__':
    main()
