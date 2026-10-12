import json
import logging
from kafka import KafkaProducer

logger = logging.getLogger("kafka_producer")

# Initialize Producer pointing to Kafka broker (Using 127.0.0.1 to avoid Windows IPv6 delays)
producer = KafkaProducer(
    bootstrap_servers=["127.0.0.1:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    acks="all",             # Wait for leader & replica confirmation for zero message loss
    retries=3,              # Retry automatically on transient network issues
    max_in_flight_requests_per_connection=1  # Maintain strict ordering per key
)

def send_order_event(order_data: dict, on_error=None):
    """Publishes an order reservation event to the 'order-events' Kafka topic asynchronously."""
    try:
        topic = "order-events"
        key = str(order_data["item_id"])  # Route same item to same partition

        # Asynchronously send message to Kafka (returns a Future immediately)
        future = producer.send(topic, key=key, value=order_data)
        
        # Attach asynchronous error callback if delivery fails later in the background
        if on_error:
            future.add_errback(on_error)
            
        print(f"🚀 [Producer] Published order event to topic '{topic}': {order_data}")
        return future
        
    except Exception as e:
        logger.error(f"Failed to initiate event publication to Kafka: {e}")
        raise e