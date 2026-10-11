import concurrent.futures
import json
import urllib.request
from collections import Counter

URL = "http://127.0.0.1:8000/orders"

def place_order(user_id):
    data = json.dumps({"user_id": user_id, "item_id": 1, "quantity": 1}).encode("utf-8")
    req = urllib.request.Request(URL, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:
        return f"Error: {e}"

def run_concurrency_test():
    print("🚀 Firing 50 simultaneous order requests at POST /orders...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        results = list(executor.map(place_order, range(1, 51)))

    # Count frequencies of all status codes
    counts = Counter(results)

    print("\n📊 Concurrency Test Results Breakdown:")
    print(f"  ✅ Successful Orders (201 Created): {counts.get(201, 0)}")
    print(f"  ❌ Out of Stock Rejections (400 Bad Request): {counts.get(400, 0)}")

    # Track edge-case exceptions cleanly
    if 404 in counts:
        print(f"  🔍 Item Not Found (404 Not Found): {counts.get(404, 0)}")
    if 500 in counts:
        print(f"  💥 Internal Server Errors (500 Server Error): {counts.get(500, 0)}")

    # Catch any connection drops or raw error strings
    known_codes = {201, 400, 404, 500}
    network_errors = {k: v for k, v in counts.items() if k not in known_codes}
    if network_errors:
        print(f"  ⚠️ Network / Unexpected Errors: {network_errors}")

if __name__ == "__main__":
    run_concurrency_test()