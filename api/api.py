#!/usr/bin/env python3
import os
import time
import json
import psycopg2
import redis
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
import uvicorn

# Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_NAME = os.getenv('DB_NAME', 'solana')
DB_USER = os.getenv('DB_USER', 'solana')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'solana123')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
CACHE_TTL = int(os.getenv('CACHE_TTL', '60'))

# Prometheus metrics
api_requests = Counter('api_requests_total', 'Total API requests', ['endpoint', 'method'])
api_errors = Counter('api_errors_total', 'Total API errors', ['endpoint'])
api_duration = Histogram('api_request_duration_seconds', 'API request duration', ['endpoint'])
cache_hits = Counter('cache_hits_total', 'Total cache hits', ['endpoint'])
cache_misses = Counter('cache_misses_total', 'Total cache misses', ['endpoint'])
db_queries = Counter('db_queries_total', 'Total database queries', ['query_type'])
db_query_duration = Histogram('db_query_duration_seconds', 'Database query duration', ['query_type'])

app = FastAPI(title="Helius API Demo", version="1.0.0")

# Database connection pool
def get_db():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

# Redis connection
redis_client = None
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    redis_client.ping()
    print(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    print(f"Redis connection failed: {e}. Running without cache.")
    redis_client = None

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": time.time()}

@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/transactions")
async def get_transactions(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """Get latest transactions"""
    endpoint = "get_transactions"
    api_requests.labels(endpoint=endpoint, method="GET").inc()

    cache_key = f"transactions:limit={limit}:offset={offset}"

    # Try cache first
    if redis_client:
        try:
            cached = redis_client.get(cache_key)
            if cached:
                cache_hits.labels(endpoint=endpoint).inc()
                return JSONResponse(content=json.loads(cached))
            else:
                cache_misses.labels(endpoint=endpoint).inc()
        except Exception as e:
            print(f"Cache error: {e}")

    # Query database
    try:
        with api_duration.labels(endpoint=endpoint).time():
            with get_db() as conn:
                with conn.cursor() as cur:
                    with db_query_duration.labels(query_type="get_transactions").time():
                        db_queries.labels(query_type="get_transactions").inc()
                        cur.execute(
                            """
                            SELECT signature, slot, block_time, fee, success, timestamp, data
                            FROM transactions
                            ORDER BY id DESC
                            LIMIT %s OFFSET %s
                            """,
                            (limit, offset)
                        )
                        rows = cur.fetchall()

                        transactions = [
                            {
                                "signature": row[0],
                                "slot": row[1],
                                "block_time": row[2],
                                "fee": row[3],
                                "success": row[4],
                                "timestamp": row[5].isoformat() if row[5] else None,
                                "data": row[6]
                            }
                            for row in rows
                        ]

                        result = {
                            "count": len(transactions),
                            "limit": limit,
                            "offset": offset,
                            "transactions": transactions
                        }

                        # Cache the result
                        if redis_client:
                            try:
                                redis_client.setex(
                                    cache_key,
                                    CACHE_TTL,
                                    json.dumps(result)
                                )
                            except Exception as e:
                                print(f"Cache set error: {e}")

                        return JSONResponse(content=result)

    except Exception as e:
        api_errors.labels(endpoint=endpoint).inc()
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/transaction/{signature}")
async def get_transaction(signature: str):
    """Get transaction by signature"""
    endpoint = "get_transaction"
    api_requests.labels(endpoint=endpoint, method="GET").inc()

    cache_key = f"transaction:{signature}"

    # Try cache
    if redis_client:
        try:
            cached = redis_client.get(cache_key)
            if cached:
                cache_hits.labels(endpoint=endpoint).inc()
                return JSONResponse(content=json.loads(cached))
            else:
                cache_misses.labels(endpoint=endpoint).inc()
        except Exception as e:
            print(f"Cache error: {e}")

    # Query database
    try:
        with api_duration.labels(endpoint=endpoint).time():
            with get_db() as conn:
                with conn.cursor() as cur:
                    with db_query_duration.labels(query_type="get_transaction").time():
                        db_queries.labels(query_type="get_transaction").inc()
                        cur.execute(
                            """
                            SELECT signature, slot, block_time, fee, success, timestamp, data
                            FROM transactions
                            WHERE signature = %s
                            """,
                            (signature,)
                        )
                        row = cur.fetchone()

                        if not row:
                            raise HTTPException(status_code=404, detail="Transaction not found")

                        transaction = {
                            "signature": row[0],
                            "slot": row[1],
                            "block_time": row[2],
                            "fee": row[3],
                            "success": row[4],
                            "timestamp": row[5].isoformat() if row[5] else None,
                            "data": row[6]
                        }

                        # Cache result
                        if redis_client:
                            try:
                                redis_client.setex(
                                    cache_key,
                                    CACHE_TTL * 10,  # Cache individual transactions longer
                                    json.dumps(transaction)
                                )
                            except Exception as e:
                                print(f"Cache set error: {e}")

                        return JSONResponse(content=transaction)

    except HTTPException:
        raise
    except Exception as e:
        api_errors.labels(endpoint=endpoint).inc()
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/stats")
async def get_stats():
    """Get transaction statistics"""
    endpoint = "get_stats"
    api_requests.labels(endpoint=endpoint, method="GET").inc()

    cache_key = "stats"

    # Try cache
    if redis_client:
        try:
            cached = redis_client.get(cache_key)
            if cached:
                cache_hits.labels(endpoint=endpoint).inc()
                return JSONResponse(content=json.loads(cached))
            else:
                cache_misses.labels(endpoint=endpoint).inc()
        except Exception as e:
            print(f"Cache error: {e}")

    # Query database
    try:
        with api_duration.labels(endpoint=endpoint).time():
            with get_db() as conn:
                with conn.cursor() as cur:
                    with db_query_duration.labels(query_type="get_stats").time():
                        db_queries.labels(query_type="get_stats").inc()
                        cur.execute(
                            """
                            SELECT
                                COUNT(*) as total_transactions,
                                COUNT(CASE WHEN success THEN 1 END) as successful_transactions,
                                AVG(fee) as avg_fee,
                                MIN(slot) as min_slot,
                                MAX(slot) as max_slot
                            FROM transactions
                            """
                        )
                        row = cur.fetchone()

                        stats = {
                            "total_transactions": row[0],
                            "successful_transactions": row[1],
                            "failed_transactions": row[0] - (row[1] or 0),
                            "avg_fee": float(row[2]) if row[2] else 0,
                            "slot_range": {
                                "min": row[3],
                                "max": row[4]
                            }
                        }

                        # Cache for shorter time
                        if redis_client:
                            try:
                                redis_client.setex(cache_key, 5, json.dumps(stats))
                            except Exception as e:
                                print(f"Cache set error: {e}")

                        return JSONResponse(content=stats)

    except Exception as e:
        api_errors.labels(endpoint=endpoint).inc()
        print(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

if __name__ == "__main__":
    print(f"Starting API server...")
    print(f"Database: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    uvicorn.run(app, host="0.0.0.0", port=8080)
