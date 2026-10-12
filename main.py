import json
import uuid
from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
import redis

# Kafka producer for publishing order events
from kafka_producer import send_order_event

from schemas import ItemResponse, OrderCreate, OrderResponse
from database import get_db, SessionLocal
from models import Item, InventoryBucket, Order, OrderStatus

app = FastAPI(title="Distributed High-Throughput Order Engine")

# Printing to the console is slow on Windows, so I only log when debugging.
# Keep this False while benchmarking, True for demos.
DEBUG_LOGS = False

def log(message: str):
    if DEBUG_LOGS:
        print(message)

# 1. connect to redis
# using 127.0.0.1 instead of localhost, localhost can resolve to IPv6 first on Windows and add delay
redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

# 2. seed redis data on ram on startup
# TODO: this overwrites Redis stock with the Postgres value on every restart.
# Only safe if worker.py also decrements InventoryBucket.stock when it saves an order. Need to verify.
@app.on_event("startup")
def startup_event():
    db = SessionLocal()
    try:
        items = db.query(InventoryBucket).all()
        for item in items:
            redis_client.set(f"item:{item.item_id}:stock", item.stock)
            print(f"⚡ Seeded Redis -> item:{item.item_id}:stock = {item.stock}")
    finally:
        db.close()

# --- API Endpoints ---
@app.get("/")
def root():
    return {"status": "online", "message": "Order Engine API is running!"}

@app.get("/items", response_model=List[ItemResponse])
def get_items(db: Session = Depends(get_db)):

    # Define the string key name
    static_cache_key = "catalog:items:static"
    
    # Step 1: Check the redis ram first using the key name
    cached_catalog = redis_client.get(static_cache_key)

    if cached_catalog:
        log("⚡ Redis Cache HIT! Returning catalog directly from RAM.")
        items_meta = json.loads(cached_catalog)
    else:
        # Cache Miss: Only runs once per hour or on cold start
        log("🐢 Static Catalog MISS -> Fetching base item metadata from PostgreSQL...")
        
        # PROJECTION: Fetch only the specific columns needed, skipping ORM overhead
        items = db.query(Item.id, Item.name, Item.price).all()
        
        # Convert the lightweight Row objects into dictionaries
        items_meta = [{"id": row.id, "name": row.name, "price": row.price} for row in items]
        
        # Now static_cache_key is safely the string "catalog:items:static"
        redis_client.set(static_cache_key, json.dumps(items_meta), ex=3600)  # 1-hour TTL

    if not items_meta:
        return []

    # Step 2: get live stock for ALL items in one round trip with mget.
    # Name and price are cached for an hour, but stock has to be live, so it comes from Redis.
    stock_keys = [f"item:{item['id']}:stock" for item in items_meta]
    live_stocks = redis_client.mget(stock_keys)

    # Step 3: combine name/price with live stock and send it back.
    # Postgres is never touched on a cache hit.
    response = []
    for item, stock_val in zip(items_meta, live_stocks):
        current_stock = int(stock_val) if stock_val is not None else 0
        response.append({
            "id": item["id"],
            "name": item["name"],
            "price": item["price"],
            "inventory": {"stock": current_stock}
        })  

    return response


# 202 ACCEPTED instead of 201: the order is queued, not saved yet.
# No response_model since we return a tracking UUID now.
@app.post("/orders", status_code=status.HTTP_202_ACCEPTED)
def create_order(order_data: OrderCreate, db: Session = Depends(get_db)):

    # redis 
    redis_key = f"item:{order_data.item_id}:stock"

    # case where a item that is not in redis query
    # 1. Guard: Check if key exists in Redis
    if not redis_client.exists(redis_key):
        # Fallback query to Postgres in case Redis hasn't warmed up
        item_exists = db.query(Item).filter(Item.id == order_data.item_id).first()
        if not item_exists:
            raise HTTPException(status_code=404, detail="Item not found")
        
        # If item exists in DB but not in Redis, warm up the cache
        inv = db.query(InventoryBucket).filter(InventoryBucket.item_id == order_data.item_id).first()
        # nx=True so only the first request warms the cache. Without it, two concurrent
        # requests can overwrite each other's DECRBY and the stock jumps back up.
        redis_client.set(redis_key, inv.stock if inv else 0, nx=True)

    # sub millisecond ram reservation 
    remaining_stock = redis_client.decrby(redis_key, order_data.quantity)

    if remaining_stock < 0:
        redis_client.incrby(redis_key, order_data.quantity)
        raise HTTPException(status_code=400, detail="Out of stock or insufficent inventory")

    # =====================================================================
    # 🛑 PHASE 3 (OLD ARCHITECTURE): Synchronous Database Writes - we do this right when the update was happening slow cuz we write to database and then send the to the user but  100% was safe and correct 
    # =====================================================================
    """
    try:
        updated_rows = db.query(InventoryBucket).filter(
            InventoryBucket.item_id == order_data.item_id,
            InventoryBucket.stock >= order_data.quantity
        ).update(
            {InventoryBucket.stock: InventoryBucket.stock - order_data.quantity},
            synchronize_session=False
        )

        if updated_rows == 0:
            redis_client.incrby(redis_key, order_data.quantity)
            raise HTTPException(status_code=400, detail="Out of stock in database!")
    
        # Invalidate the catalog cache so fresh stock levels reflect immediately
        # redis_client.delete("catalog:items") // not good thrashing stampede problem

    except Exception as e: # - cant use this since we are handling the thing to kafka 
        db.rollback()
        redis_client.incrby(redis_key, order_data.quantity)
        raise e
    """

    # =====================================================================
    # 🚀 PHASE 4 (NEW ARCHITECTURE): Async Event-Driven Kafka Streaming
    # =====================================================================
    
    # 3. Create Async Payload with a tracking UUID
    order_id = str(uuid.uuid4())
    event_payload = {
        "order_id": order_id,
        "user_id": order_data.user_id,
        "item_id": order_data.item_id,
        "quantity": order_data.quantity,
        "status": "PENDING"
    }

    # producer.send() is async, so a delivery failure can happen after this function has
    # already moved on and the try/except below would never see it.
    # This callback handles that case: put the stock back and mark the order FAILED.
    # It runs on Kafka's background thread (the redis client is thread-safe).
    def compensate(exc):
        redis_client.incrby(redis_key, order_data.quantity)
        redis_client.hset(f"order:{order_id}:status", "status", "FAILED")
        print(f"❌ Kafka delivery failed for {order_id}: {exc}")

    # 4. Send to Kafka. No DB write here, worker.py handles that.
    try:
        # Set initial status in Redis RAM so immediate polling finds "PENDING"
        # one round trip instead of two
        pipe = redis_client.pipeline()
        pipe.hset(f"order:{order_id}:status", mapping={
            "status": "PENDING",
            "item_id": order_data.item_id,
            "quantity": order_data.quantity
        })
        pipe.expire(f"order:{order_id}:status", 600)  # 10-minute TTL
        pipe.execute()

        send_order_event(event_payload, on_error=compensate)
    except Exception as e:
        # send() failed right away (e.g. Kafka is down), so the callback was never attached.
        # Clean up the status key, give the stock back, and tell the client to retry.
        redis_client.delete(f"order:{order_id}:status")
        redis_client.incrby(redis_key, order_data.quantity)
        raise HTTPException(status_code=503, detail="Failed to queue order event")
    return {"order_id": order_id, "message": "Order queued successfully"}
 
# 5. Status Polling Endpoint
@app.get("/orders/{order_id}") 
def get_order_status(order_id: str, db: Session = Depends(get_db)):
    # Fast Path: Check Redis RAM (< 1ms lookup)
    cached_status = redis_client.hgetall(f"order:{order_id}:status")
    if cached_status:
        return {"order_id": order_id, **cached_status}

    # Slow Path: Fallback to PostgreSQL for older orders
    order = db.query(Order).filter(Order.id == order_id).first()
    if order:
        return {
            "order_id": order.id,
            "status": order.status,
            "item_id": order.item_id,
            "quantity": order.quantity
        }

    raise HTTPException(status_code=404, detail="Order not found")