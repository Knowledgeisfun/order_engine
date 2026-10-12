import json
import redis
from kafka import KafkaConsumer, KafkaProducer
from sqlalchemy.exc import IntegrityError
from database import SessionLocal
from models import Order, InventoryBucket, OrderStatus

redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)


def fail_order(event, raw_byte_data, dlq_producer, error):
    # Compensating transaction: the database rejected this order, so give the reserved
    # stock back in Redis, mark the order FAILED, and park the message in the DLQ.
    redis_key = f"item:{event['item_id']}:stock"
    redis_client.incrby(redis_key, event["quantity"])
    redis_client.hset(f"order:{event['order_id']}:status", mapping={
        "status": "FAILED",
        "reason": "Database processing error"
    })
    # NOTE: stock was already given back above. If someone replays this DLQ message later,
    # the stock has to be reserved again first, otherwise replaying could oversell.
    dlq_producer.send("order-events-dlq", raw_byte_data)
    dlq_producer.flush()  # make sure it actually reached Kafka before we commit the offset
    print(f"❌ [Worker] Order {event['order_id']} failed and was sent to DLQ: {error}")


def handle_order(event, raw_byte_data, dlq_producer):
    order_id = event["order_id"]
    db = SessionLocal()

    try:
        # 1. Idempotency Guard
        if db.query(Order).filter(Order.id == order_id).first():
            print(f"⚠️ [Worker] Duplicate order {order_id} detected. Skipping.")
            return

        print(f"📥 [Worker] Processing event: {order_id}")

        # 2. PostgreSQL Write (stock decrement + order insert in ONE transaction)
        # The stock >= quantity check makes Postgres a second safety net behind Redis,
        # so stock can never go negative in the database.
        updated_rows = db.query(InventoryBucket).filter(
            InventoryBucket.item_id == event["item_id"],
            InventoryBucket.stock >= event["quantity"]
        ).update(
            {InventoryBucket.stock: InventoryBucket.stock - event["quantity"]},
            synchronize_session=False
        )
        if updated_rows == 0:
            raise ValueError("Not enough stock in database (or item missing)")

        new_order = Order(
            id=order_id,
            user_id=event["user_id"],
            item_id=event["item_id"],
            quantity=event["quantity"],
            status=OrderStatus.COMPLETED
        )
        db.add(new_order)
        db.commit()

    except IntegrityError as e:
        db.rollback()
        # IntegrityError can mean two things: another worker saved this same order_id first
        # (a real duplicate), or a different constraint failed (e.g. a foreign key).
        # Only the first one is safe to skip, so check which it was.
        if db.query(Order).filter(Order.id == order_id).first():
            print(f"⚠️ [Worker] Order {order_id} was saved by another worker. Skipping.")
            return
        fail_order(event, raw_byte_data, dlq_producer, e)
        return

    except Exception as e:
        db.rollback()
        fail_order(event, raw_byte_data, dlq_producer, e)
        return

    finally:
        db.close()

    # 3. Update Redis status.
    # This is outside the DB try block on purpose: the order is already committed in Postgres,
    # so a Redis problem here must NOT give the stock back or mark the order FAILED.
    # Polling falls back to Postgres if this update is missed.
    try:
        pipe = redis_client.pipeline()
        pipe.hset(f"order:{order_id}:status", "status", "COMPLETED")
        pipe.expire(f"order:{order_id}:status", 600)  # keeps the 10-minute TTL even if the key had expired
        pipe.execute()
        print(f"✅ [Worker] Order {order_id} successfully committed!")
    except Exception as e:
        print(f"⚠️ [Worker] Order {order_id} saved, but Redis status update failed: {e}")


def run_worker():
    # 1. The DLQ Producer (Sends bad messages to the graveyard)
    dlq_producer = KafkaProducer(
        bootstrap_servers=["127.0.0.1:9092"]
    )

    # 2. The Main Consumer
    consumer = KafkaConsumer(
        "order-events",
        bootstrap_servers=["127.0.0.1:9092"],
        group_id="order-processing-group-production",
        auto_offset_reset="earliest",
        # Auto-commit saves the offset on a timer, not after the order is actually saved,
        # so a crash could skip or replay messages. I commit by hand after processing instead.
        enable_auto_commit=False
        # Notice: We removed `value_deserializer` to prevent instant crashes
    )

    print("👷 [Worker] Listening for order events (with DLQ Protection)...")

    for message in consumer:
        raw_byte_data = message.value

        # --- PHASE 1: DESERIALIZATION & POISON PILL CATCHER ---
        try:
            event = json.loads(raw_byte_data.decode("utf-8"))
        except Exception as e:
            print(f"☠️ [DLQ] Corrupted payload detected! Routing to DLQ. Error: {e}")
            dlq_producer.send("order-events-dlq", raw_byte_data)
            dlq_producer.flush()
            consumer.commit()
            continue  # skip to the next message (no DB session was opened yet, so nothing leaks)

        # --- PHASE 2: NORMAL PROCESSING ---
        handle_order(event, raw_byte_data, dlq_producer)

        # Commit the offset only after the order is fully handled (saved, skipped as a
        # duplicate, or failed and routed to the DLQ). If the worker crashes before this
        # line, Kafka redelivers the message and the idempotency guard handles the repeat.
        consumer.commit()


if __name__ == "__main__":
    run_worker()