"""End-to-end check against a running deployment (stdlib only).

Usage: python scripts/smoke_test.py https://your-service.example.com
"""
import json
import sys
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

BASE_URL = (sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000").rstrip("/")


def request(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE_URL + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            status, raw = resp.status, resp.read()
    except urllib.error.HTTPError as e:
        status, raw = e.code, e.read()
    return status, (json.loads(raw) if raw else None), (time.perf_counter() - started) * 1000


def webhook(txn_id):
    return request("POST", "/v1/webhooks/transactions", {
        "transaction_id": txn_id, "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456", "amount": 1500, "currency": "INR",
    })


def main():
    print("Health:", request("GET", "/")[:2])

    single = f"txn_smoke_{uuid.uuid4().hex[:10]}"
    status, body, ms = webhook(single)
    print(f"Single webhook -> {status} in {ms:.0f}ms {body}")

    dup = f"txn_dup_{uuid.uuid4().hex[:10]}"
    with ThreadPoolExecutor(10) as pool:
        results = list(pool.map(lambda _: webhook(dup), range(10)))
    print("Duplicate webhooks ->", [r[0] for r in results],
          f"max {max(r[2] for r in results):.0f}ms,",
          sum(not r[1]["duplicate"] for r in results), "newly accepted")

    print("Status now:", request("GET", f"/v1/transactions/{single}")[1][0]["status"])
    print("Waiting 35s for background processing...")
    time.sleep(35)
    for txn_id in (single, dup):
        rows = request("GET", f"/v1/transactions/{txn_id}")[1]
        print(f"{txn_id}: {len(rows)} row(s) -> {rows[0]['status']} "
              f"(created {rows[0]['created_at']}, processed {rows[0]['processed_at']})")


if __name__ == "__main__":
    main()
