import json
import redis
from kafka import KafkaConsumer, KafkaProducer
from database import SessionLocal
from models import Order, InventoryBucket, OrderStatus

redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

def run_worker():
    # 1. The DLQ Producer (Sends bad messages to the graveyard)
    dlq_producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"]
    )

    # 2. The Main Consumer
    consumer = KafkaConsumer(
        "order-events",
        bootstrap_servers=["localhost:9092"],
        group_id="order-processing-group-production", 
        auto_offset_reset="earliest",  # 👈 Safe to use 'earliest' now!
        enable_auto_commit=True
        # Notice: We removed `value_deserializer` to prevent instant crashes
    )

    print("👷 [Worker] Listening for order events (with DLQ Protection)...")

    for message in consumer:
        db = SessionLocal()
        raw_byte_data = message.value
        
        # --- PHASE 1: DESERIALIZATION & POISON PILL CATCHER ---
        try:
            event = json.loads(raw_byte_data.decode("utf-8"))
        except Exception as e:
            print(f"☠️ [DLQ] Corrupted payload detected! Routing to DLQ. Error: {e}")
            dlq_producer.send("order-events-dlq", raw_byte_data)
            continue  # 👈 Skips the rest of the loop and safely moves to the next message
            
        # --- PHASE 2: NORMAL PROCESSING ---
        try:
            # 1. Idempotency Guard
            existing_order = db.query(Order).filter(Order.id == event["order_id"]).first()
            if existing_order:
                print(f"⚠️ [Worker] Duplicate order {event['order_id']} detected. Skipping.")
                continue

            print(f"📥 [Worker] Processing event: {event['order_id']}")

            # 2. PostgreSQL Write
            db.query(InventoryBucket).filter(
                InventoryBucket.item_id == event["item_id"]
            ).update(
                {InventoryBucket.stock: InventoryBucket.stock - event["quantity"]},
                synchronize_session=False
            )

            new_order = Order(
                id=event["order_id"],
                user_id=event["user_id"],
                item_id=event["item_id"],
                quantity=event["quantity"],
                status=OrderStatus.COMPLETED
            )
            db.add(new_order)
            db.commit()
            
            # 3. Update Redis Status
            redis_client.hset(f"order:{event['order_id']}:status", "status", "COMPLETED")
            print(f"✅ [Worker] Order {event['order_id']} successfully committed!")

        except Exception as e:
            db.rollback()
            print(f"❌ [Worker] Database error for {event['order_id']}: {e}")
            
            # 4. Compensating Transaction
            redis_key = f"item:{event['item_id']}:stock"
            redis_client.incrby(redis_key, event["quantity"])
            redis_client.hset(f"order:{event['order_id']}:status", mapping={
                "status": "FAILED",
                "reason": "Database processing error"
            })
            
            # Optional: We can also route logical database failures to the DLQ 
            # so engineers can investigate why the database rejected a perfectly formatted JSON order.
            dlq_producer.send("order-events-dlq", raw_byte_data)
            
        finally:
            db.close()

if __name__ == "__main__":
    run_worker()