import json
from kafka import KafkaConsumer

def inspect_dlq():
    consumer = KafkaConsumer(
        "order-events-dlq",
        bootstrap_servers=["localhost:9092"],
        group_id="dlq-diagnostic-team",
        auto_offset_reset="earliest",  # Pull everything sitting in the graveyard
        enable_auto_commit=False       # Read-only mode: don't delete them from the queue after reading
    )

    print("🕵️‍♂️ [DLQ Inspector] Scanning for corrupted messages...\n")

    for message in consumer:
        raw_bytes = message.value
        print("-" * 50)
        print(f"📦 Offset: {message.offset} | Partition: {message.partition}")
        
        try:
            # Step 1: Can we even read this as text?
            text_payload = raw_bytes.decode("utf-8")
            print(f"📄 Raw Text Received:\n{text_payload}\n")
            
            # Step 2: Exactly why did the original worker crash?
            json.loads(text_payload)
            
        except UnicodeDecodeError:
            print(f"🚨 ENCODING ERROR: Payload is not text. Raw bytes: {raw_bytes}")
        except json.JSONDecodeError as e:
            print(f"💥 JSON ERROR: {e}")
            print("💡 Fix: Check the frontend or API gateway making the POST request.")

if __name__ == "__main__":
    inspect_dlq()