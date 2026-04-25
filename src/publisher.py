import argparse
import json
import random
import time
import uuid
import sys
from datetime import datetime, timezone
from typing import List

try:
    import requests
except ImportError:
    print("Install requests: pip install requests")
    sys.exit(1)

BASE_URL = "http://localhost:8000"
TOPICS = ["auth.login", "payment.processed", "order.created", "user.signup", "error.critical"]

def make_event(topic: str = None, event_id: str = None, source: str = "publisher-sim") -> dict:
    return{
        "topic": topic or random.choice(TOPICS),
        "event_id": event_id or f"evt-{uuid.uuid4().hex[:16]}",
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": source,
        "payload": {
            "level": random.choice(["INFO", "WARNING", "ERROR"]),
            "message": f"Simulated event at {time.time():.3f}",
            "user_id": random.randint(1000, 9999),
        },
    }

def publish_batch(events: List[dict]) -> dict:
    print(f"[DEBUG] Mengirim {len(events)} events ke {BASE_URL}/publish")
    try:
        resp = requests.post(f"{BASE_URL}/publish", json={"events": events}, timeout=30)
        resp.raise_for_status()
        print(f"[DEBUG] Response: {resp.status_code}")
        return resp.json()
    except Exception as e:
        print(f"[ERROR] Failed to publish batch: {e}")
        raise

def run_demo():
    print("== DEMO MODE ==\n")
    events = [make_event() for _ in range(5)]
    print(f"Mengirim 5 event unik...")
    result = publish_batch(events)
    print(f" -> {result}\n")
    time.sleep(0.5)

    duplicates = events[:3]
    print(f"Mengirim ulang 3 event yang sama (simulasi at-least-once)...")
    result = publish_batch(duplicates)
    print(f" -> {result}\n")
    time.sleep(0.5)

    stats = requests.get(f"{BASE_URL}/stats").json()
    print(f"Stats: {json.dumps(stats, indent=2)}\n")

    for topic in TOPICS[:2]:
        ev = requests.get(f"{BASE_URL}/events", params={"topic": topic}).json()
        print(f"Topic '{topic}': {ev['count']} event unik")

def run_stress(total: int = 5000, dup_rate: float = 0.25, batch_size: int = 100):
    print(f"=== Stress Test: {total} events, {dup_rate*100:.0f}% dup_rate ===")
    print(f"[DEBUG] BASE_URL: {BASE_URL}")
    print(f"[DEBUG] batch_size: {batch_size}\n")

    unique_count = int(total * (1 - dup_rate))
    dup_count = total - unique_count
    pool = [make_event() for _ in range(unique_count)]
    duplicates = [random.choice(pool).copy() for _ in range(dup_count)]

    for d in duplicates:
        d["timestamp"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    all_events = pool + duplicates
    random.shuffle(all_events)

    print(f"Total events: {len(all_events)} ({unique_count} unik, {dup_count} Duplikat)")
    start = time.time()
    sent = 0
    for i in range(0, len(all_events), batch_size):
        batch = all_events[i:i + batch_size]
        try:
            publish_batch(batch)
            sent += len(batch)
            if sent % 1000 == 0:
                elapsed = time.time() - start
                print(f" Sent {sent}/{len(all_events)}{elapsed:.1f}s")
        except Exception as e:
            print(f"Error saat mengirim batch: {e}")
    elapsed = time.time() - start
    throughput = sent / elapsed
    print(f"\nSelesai: {sent} events dalam {elapsed:.2f}s (throughput: {throughput:.0f} ev/s)")

    time.sleep(1)
    stats = requests.get(f"{BASE_URL}/stats").json()
    print(f"Stats setelah stress test: {json.dumps(stats, indent=2)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pub-Sub Log Aggregator Publisher Simulator")
    parser.add_argument("--mode", choices=["demo", "stress"], default="demo")
    parser.add_argument("--count", type=int, default=5000)
    parser.add_argument("--dup-rate", type=float, default=0.25, dest="dup_rate")
    parser.add_argument("--batch-size", type=int, default=100, dest="batch_size")
    parser.add_argument("--url", type=str, default="http://localhost:8000")
    args = parser.parse_args()

    BASE_URL = args.url

    if args.mode == "demo":
        run_demo()
    elif args.mode == "stress":
        run_stress(total=args.count, dup_rate=args.dup_rate, batch_size=args.batch_size)
            