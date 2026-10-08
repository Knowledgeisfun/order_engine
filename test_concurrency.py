import concurrent.futures
import json
import urllib.request

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

    successful = results.count(201)
    failed = results.count(400)

    print("\n📊 Concurrency Test Results:")
    print(f"  ✅ Successful Orders (201 Created): {successful}")
    print(f"  ❌ Rejected / Out of Stock (400 Bad Request): {failed}")

if __name__ == "__main__":
    run_concurrency_test()