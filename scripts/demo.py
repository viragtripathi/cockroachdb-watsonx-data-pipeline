#!/usr/bin/env python3
"""Dynamic demo data generator for the CockroachDB CDC to watsonx.data pipeline.

Sends realistic CDC events (inserts, updates, deletes) to the webhook endpoint
with current timestamps and randomized data. Each run produces unique data.

Usage:
    uv run python scripts/demo.py [--url http://localhost:5002] [--batch-pause 15]
"""

import argparse
import random
import time
import uuid
from datetime import datetime, timezone

import requests

MERCHANTS = [
    ("Starbucks", "Coffee", 3.50, 8.50),
    ("Whole Foods", "Groceries", 25.00, 200.00),
    ("Uber", "Transport", 8.00, 65.00),
    ("Shell", "Transport", 30.00, 70.00),
    ("Italian Bistro", "Restaurant", 25.00, 120.00),
    ("Chipotle", "Restaurant", 10.00, 25.00),
    ("Netflix", "Entertainment", 15.99, 22.99),
    ("Amazon", "Shopping", 15.00, 300.00),
    ("Planet Fitness", "Health", 10.00, 50.00),
    ("Best Buy", "Electronics", 50.00, 500.00),
    ("Target", "Shopping", 20.00, 150.00),
    ("CVS Pharmacy", "Health", 5.00, 80.00),
    ("Spotify", "Entertainment", 9.99, 15.99),
    ("Costco", "Groceries", 50.00, 350.00),
    ("Lyft", "Transport", 10.00, 45.00),
]

PAYMENT_METHODS = ["Debit Card", "Credit Card", "Apple Pay", "Google Pay"]
USERS = ["user-1", "user-2", "user-3"]


def make_expense(expense_id=None):
    merchant, category, min_amt, max_amt = random.choice(MERCHANTS)
    amount = round(random.uniform(min_amt, max_amt), 2)
    now = datetime.now(timezone.utc)
    return {
        "expense_id": expense_id or str(uuid.uuid4())[:12],
        "user_id": random.choice(USERS),
        "description": f"{category} at {merchant}",
        "merchant": merchant,
        "expense_amount": amount,
        "expense_date": now.strftime("%Y-%m-%d"),
        "shopping_type": category,
        "payment_method": random.choice(PAYMENT_METHODS),
        "recurring": random.random() < 0.3,
    }


def send_events(url, events, label):
    try:
        resp = requests.post(f"{url}/cdc/events", json=events, timeout=10)
        resp.raise_for_status()
        print(f"  {label}: {len(events)} events -> {resp.json()}")
    except Exception as e:
        print(f"  {label}: FAILED -> {e}")


def run_demo(url, batch_pause):
    print(f"\n{'='*60}")
    print("  CockroachDB CDC -> watsonx.data Demo")
    print(f"  Pipeline: {url}")
    print(f"  Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*60}\n")

    # Check pipeline is running
    try:
        stats = requests.get(f"{url}/cdc/stats", timeout=5).json()
        print(f"Pipeline status: {stats}\n")
    except Exception:
        print(f"ERROR: Pipeline not reachable at {url}")
        print("Start it with: uv run crdb-wxd-pipeline webhook")
        return

    # Phase 1: INSERTS (10 expenses)
    print("--- Phase 1: INSERTS (10 new expenses) ---")
    inserts = []
    expense_ids = []
    for _ in range(10):
        exp = make_expense()
        expense_ids.append(exp["expense_id"])
        inserts.append({"after": exp})
    send_events(url, inserts, "INSERT")

    print(f"\nWaiting {batch_pause}s for batch flush...")
    time.sleep(batch_pause)

    # Phase 2: UPDATES (modify 4 of the 10)
    print("\n--- Phase 2: UPDATES (4 expenses modified) ---")
    updates = []
    update_ids = random.sample(expense_ids, 4)
    for eid in update_ids:
        original = next(e["after"] for e in inserts if e["after"]["expense_id"] == eid)
        updated = original.copy()
        updated["expense_amount"] = round(original["expense_amount"] * random.uniform(0.5, 2.0), 2)
        updated["description"] = f"{original['description']} (corrected)"
        updated["payment_method"] = random.choice(PAYMENT_METHODS)
        updates.append({
            "before": {"expense_id": eid, "expense_amount": original["expense_amount"]},
            "after": updated,
        })
    send_events(url, updates, "UPDATE")

    print(f"\nWaiting {batch_pause}s for batch flush...")
    time.sleep(batch_pause)

    # Phase 3: DELETES (remove 2 of the 10)
    print("\n--- Phase 3: DELETES (2 expenses removed) ---")
    remaining = [eid for eid in expense_ids if eid not in update_ids]
    delete_ids = random.sample(remaining, min(2, len(remaining)))
    deletes = []
    for eid in delete_ids:
        original = next(e["after"] for e in inserts if e["after"]["expense_id"] == eid)
        deletes.append({"before": original, "after": None})
    send_events(url, deletes, "DELETE")

    print(f"\nWaiting {batch_pause}s for batch flush...")
    time.sleep(batch_pause)

    # Summary
    stats = requests.get(f"{url}/cdc/stats", timeout=5).json()
    print(f"\n{'='*60}")
    print("  Demo complete!")
    print(f"  Total events: {stats['total_events']}")
    print(f"  Total flushes: {stats['total_flushes']} (= Iceberg snapshots)")
    print(f"{'='*60}")
    print("\nRun these in watsonx.data Query workspace:\n")
    print("  -- All CDC events")
    print("  SELECT expense_id, merchant, expense_amount, cdc_operation, cdc_timestamp")
    print("  FROM iceberg_data.banko.expenses ORDER BY cdc_timestamp DESC;\n")
    print("  -- CDC operation counts")
    print("  SELECT cdc_operation, COUNT(*) FROM iceberg_data.banko.expenses GROUP BY cdc_operation;\n")
    print("  -- Time travel snapshots")
    print("  SELECT * FROM iceberg_data.banko.\"expenses$snapshots\" ORDER BY committed_at DESC;\n")
    print("  -- Updated expenses")
    for eid in update_ids:
        print(f"  -- {eid} (updated)")
    print("\n  -- Deleted expenses")
    for eid in delete_ids:
        print(f"  -- {eid} (deleted)")


def main():
    parser = argparse.ArgumentParser(description="CDC demo data generator")
    parser.add_argument("--url", default="http://localhost:5002", help="Pipeline webhook URL")
    parser.add_argument("--batch-pause", type=int, default=15, help="Seconds to wait between batches for flush")
    args = parser.parse_args()
    run_demo(args.url, args.batch_pause)


if __name__ == "__main__":
    main()
