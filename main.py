import json
from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
import redis

from schemas import ItemResponse, OrderCreate, OrderResponse
from database import get_db, SessionLocal
from models import Item, InventoryBucket, Order, OrderStatus

app = FastAPI(title="Distributed High-Throughput Order Engine")


# 1. connect to redis
redis_client = redis.Redis(host ="localhost", port=6379, db=0, decode_responses=True)

#2.seed redis data on ram on startup
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
        return[]


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

@app.post("/orders", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
def create_order(order_data: OrderCreate, db: Session = Depends(get_db)):

    #redis 
    redis_key = f"item:{order_data.item_id}:stock"

    #case where a item that is not in redis query
    # 1. Guard: Check if key exists in Redis
    if not redis_client.exists(redis_key):
        # Fallback query to Postgres in case Redis hasn't warmed up
        item_exists = db.query(Item).filter(Item.id == order_data.item_id).first()
        if not item_exists:
            raise HTTPException(status_code=404, detail="Item not found")
        
        # If item exists in DB but not in Redis, warm up the cache
        inv = db.query(InventoryBucket).filter(InventoryBucket.item_id == order_data.item_id).first()
        redis_client.set(redis_key, inv.stock if inv else 0)

    #sub millisecond ram reservation 
    remaining_stock = redis_client.decrby(redis_key, order_data.quantity)

    if remaining_stock < 0:
        redis_client.incrby(redis_key, order_data.quantity)
        raise HTTPException(status_code=400, detail="Out of stock or insufficent inventory")

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

        new_order = Order(
            user_id=order_data.user_id,
            item_id=order_data.item_id,
            quantity=order_data.quantity,
            status=OrderStatus.COMPLETED
        )

        db.add(new_order)
        db.commit()
        db.refresh(new_order)

        # Invalidate the catalog cache so fresh stock levels reflect immediately
        #redis_client.delete("catalog:items") //not good thrashing stamped problem

        return new_order

    except Exception as e:
        db.rollback()
        redis_client.incrby(redis_key, order_data.quantity)
        raise e