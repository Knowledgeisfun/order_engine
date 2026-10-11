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
    return db.query(Item).all()

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

        return new_order

    except Exception as e:
        db.rollback()
        redis_client.incrby(redis_key, order_data.quantity)
        raise e