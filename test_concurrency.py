import asyncio
import time
import httpx

BASE_URL = "http://127.0.0.1:8000"
NUM_REQUESTS = 50  # 50 concurrent buyers
ITEM_ID = 1

async def send_order(client, user_id):
    payload = {
        "user_id": user_id,
        "item_id": ITEM_ID,
        "quantity": 1
    }
    start = time.perf_counter()
    try:
        response = await client.post(f"{BASE_URL}/orders", json=payload)
        elapsed = (time.perf_counter() - start) * 1000  # Elapsed time in ms
        
        # We also need to handle cases where the server returns a 500 but still connects
        if response.status_code != 202:
            print(f"❌ Server rejected order {user_id}: Status {response.status_code} - {response.text}")
            
        data = response.json() if response.status_code == 202 else {}
        order_id = data.get("order_id")
        return response.status_code, order_id, elapsed
        
    except Exception as e:
        # NOW you will see if the server is off, DNS failed, or port is wrong!
        print(f"🔌 Network/Connection Error for user {user_id}: {e}")
        return 0, None, 0  # Changed to 0 so we know it never reached the server

async def check_status(client, order_id):
    try:
        response = await client.get(f"{BASE_URL}/orders/{order_id}") 
        if response.status_code == 200:
            return response.json().get("status", "UNKNOWN")
        return "NOT_FOUND"
    except Exception as e:
        return "ERROR"

async def main():
    print(f"🔥 Phase 1: Launching {NUM_REQUESTS} concurrent POST /orders requests...")
    start_total = time.perf_counter()

    async with httpx.AsyncClient(timeout=10.0) as client:
        # 1. Fire parallel order requests
        tasks = [send_order(client, user_id=i+1) for i in range(NUM_REQUESTS)]
        results = await asyncio.gather(*tasks)

        post_time = (time.perf_counter() - start_total) * 1000

        order_ids = []
        status_codes = {}
        latencies = []

        for code, order_id, latency in results:
            status_codes[code] = status_codes.get(code, 0) + 1
            if latency > 0:
                latencies.append(latency)
            if order_id:
                order_ids.append(order_id)

        avg_latency = sum(latencies) / len(latencies) if latencies else 0

        print(f"\n⚡ POST /orders Batch Completed in {post_time:.2f} ms")
        print(f"   - Average HTTP Latency: {avg_latency:.2f} ms")
        print(f"   - HTTP Response Codes:  {status_codes}")
        print(f"   - Order Tracking IDs:   {len(order_ids)} generated")

        if not order_ids:
            print("❌ No order IDs generated. Stopping test.")
            return

        # 2. Immediate Poll Check (Verify PENDING status in Redis RAM)
        print("\n🔍 Phase 2: Checking status for first 3 orders immediately (Expecting 'PENDING')...")
        for oid in order_ids[:3]:
            st = await check_status(client, oid)
            print(f"   - Order {oid[:8]}... -> Status: {st}")

        # 3. Wait for background Kafka consumer to process events
        print("\n⏳ Phase 3: Sleeping 2 seconds for worker.py to process Kafka events...")
        await asyncio.sleep(2.0)

        # 4. Final Poll Check (Verify COMPLETED / FAILED status)
        print("\n📊 Phase 4: Polling final status for all created orders...")
        status_tasks = [check_status(client, oid) for oid in order_ids]
        final_statuses = await asyncio.gather(*status_tasks)

        final_summary = {}
        for st in final_statuses:
            final_summary[st] = final_summary.get(st, 0) + 1

        print("\n==================================================")
        print("🏆 END-TO-END SYSTEM BENCHMARK SUMMARY")
        print("==================================================")
        print(f"🚀 Total Parallel Requests:     {NUM_REQUESTS}")
        print(f"⚡ Avg API Ingestion Latency:   {avg_latency:.2f} ms")
        print(f"📈 Final Statuses Breakdown:    {final_summary}")
        print("==================================================")

if __name__ == "__main__":
    asyncio.run(main())