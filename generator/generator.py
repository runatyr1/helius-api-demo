#!/usr/bin/env python3
import os
import time
import random
import string
import json
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
from prometheus_client import start_http_server, Counter, Histogram, Gauge

# Prometheus metrics
tx_generated = Counter('transactions_generated_total', 'Total transactions generated')
tx_inserted = Counter('transactions_inserted_total', 'Total transactions inserted to DB')
tx_errors = Counter('transactions_errors_total', 'Total transaction insertion errors')
insert_duration = Histogram('transaction_insert_duration_seconds', 'Time spent inserting transactions')
batch_size_gauge = Gauge('transaction_batch_size', 'Current batch size')
db_connection_errors = Counter('db_connection_errors_total', 'Database connection errors')

# Configuration from environment
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_NAME = os.getenv('DB_NAME', 'solana')
DB_USER = os.getenv('DB_USER', 'solana')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'solana123')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))  # Solana ~4000 tx/block, scaled down 40x
INTERVAL_MS = int(os.getenv('INTERVAL_MS', '400'))  # Solana block time ~400ms
METRICS_PORT = int(os.getenv('METRICS_PORT', '8000'))

def generate_signature():
    """Generate a fake Solana transaction signature (88 chars base58)"""
    chars = string.ascii_letters + string.digits
    return ''.join(random.choices(chars, k=88))

def generate_transaction():
    """Generate a fake Solana transaction"""
    slot = random.randint(100000000, 300000000)
    block_time = int(time.time()) - random.randint(0, 3600)
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

    return (
        generate_signature(),
        slot,
        block_time,
        fee,
        success,
        json.dumps(data)
    )

def connect_db():
    """Connect to PostgreSQL with retry"""
    retries = 5
    for i in range(retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            print(f"Connected to database at {DB_HOST}:{DB_PORT}")
            return conn
        except Exception as e:
            db_connection_errors.inc()
            print(f"Database connection attempt {i+1}/{retries} failed: {e}")
            if i < retries - 1:
                time.sleep(2 ** i)  # Exponential backoff
            else:
                raise

def insert_transactions(conn, transactions):
    """Insert a batch of transactions into the database"""
    try:
        with conn.cursor() as cur:
            with insert_duration.time():
                execute_batch(
                    cur,
                    """
                    INSERT INTO transactions (signature, slot, block_time, fee, success, data)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (signature) DO NOTHING
                    """,
                    transactions,
                    page_size=len(transactions)
                )
                conn.commit()
                inserted = cur.rowcount
                tx_inserted.inc(inserted)
                return inserted
    except Exception as e:
        tx_errors.inc()
        conn.rollback()
        print(f"Error inserting transactions: {e}")
        raise

def main():
    print(f"Starting transaction generator...")
    print(f"Config: batch_size={BATCH_SIZE}, interval={INTERVAL_MS}ms")
    print(f"Database: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    # Start Prometheus metrics server
    start_http_server(METRICS_PORT)
    print(f"Metrics server started on port {METRICS_PORT}")

    # Connect to database
    conn = connect_db()
    batch_size_gauge.set(BATCH_SIZE)

    print("Starting transaction generation loop...")
    while True:
        try:
            # Generate batch of transactions
            transactions = [generate_transaction() for _ in range(BATCH_SIZE)]
            tx_generated.inc(BATCH_SIZE)

            # Insert into database
            inserted = insert_transactions(conn, transactions)
            print(f"Generated and inserted {inserted}/{BATCH_SIZE} transactions")

            # Wait for next interval
            time.sleep(INTERVAL_MS / 1000.0)

        except psycopg2.OperationalError as e:
            print(f"Database connection lost: {e}. Reconnecting...")
            db_connection_errors.inc()
            try:
                conn = connect_db()
            except Exception as reconnect_error:
                print(f"Failed to reconnect: {reconnect_error}. Retrying in 5s...")
                time.sleep(5)
        except KeyboardInterrupt:
            print("Shutting down...")
            break
        except Exception as e:
            tx_errors.inc()
            print(f"Unexpected error: {e}")
            time.sleep(1)

    conn.close()
    print("Generator stopped")

if __name__ == '__main__':
    main()
