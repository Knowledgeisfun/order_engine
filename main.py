import json
import uuid
from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
import redis

# Added the Kafka producer import
from kafka_producer import send_order_event

from schemas import ItemResponse, OrderCreate, OrderResponse
from database import get_db, SessionLocal
from models import Item, InventoryBucket, Order, OrderStatus

app = FastAPI(title="Distributed High-Throughput Order Engine")

# 1. connect to redis
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

# 2. seed redis data on ram on startup
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
        print("⚡ Redis Cache HIT! Returning catalog directly from RAM.")
        items_meta = json.loads(cached_catalog)
    else:
        # Cache Miss: Only runs once per hour or on cold start
        print("🐢 Static Catalog MISS -> Fetching base item metadata from PostgreSQL...")
        
        # PROJECTION: Fetch only the specific columns needed, skipping ORM overhead
        items = db.query(Item.id, Item.name, Item.price).all()
        
        # Convert the lightweight Row objects into dictionaries
        items_meta = [{"id": row.id, "name": row.name, "price": row.price} for row in items]
        
        # Now static_cache_key is safely the string "catalog:items:static"
        redis_client.set(static_cache_key, json.dumps(items_meta), ex=3600)  # 1-hour TTL

    if not items_meta:
        return []

    # WHERE: Step 2
    # WHAT: Ask Redis for the live stock of ALL items at the exact same time using 'mget'
    # WHY: This gets 100% accurate real-time stock in less than 1 millisecond.
    stock_keys = [f"item:{item['id']}:stock" for item in items_meta]
    live_stocks = redis_client.mget(stock_keys)

    # WHERE: Step 3
    # WHAT: Combine the Name/Price with the Live Stock and send it to the user.
    # WHY: The user sees accurate stock instantly, and PostgreSQL was never touched!
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

# Changed to 202 ACCEPTED and removed response_model since we return a tracking UUID now
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
        redis_client.set(redis_key, inv.stock if inv else 0)

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

    # 4. Fire to Kafka and forget (No DB write here! Response is < 2ms)
    try:
        # Set initial status in Redis RAM so immediate polling finds "PENDING"
        redis_client.hset(f"order:{order_id}:status", mapping={
            "status": "PENDING",
            "item_id": order_data.item_id,
            "quantity": order_data.quantity
        })
        redis_client.expire(f"order:{order_id}:status", 600)  # 10-minute TTL

        send_order_event(event_payload)
    except Exception as e:
        # If Kafka itself goes down, clean up the status key and roll back stock
        redis_client.delete(f"order:{order_id}:status")
        redis_client.incrby(redis_key, order_data.quantity)
        raise HTTPException(status_code=500, detail="Failed to queue order event")
    return {"order_id": order_id, "message": "Order queued successfully"}

# 5. NEW: Status Polling Endpoint
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