from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import uuid
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

CHANNELS = ["BRANCH", "ATM", "MOBILE", "INTERNET_BANKING", "CARD", "WALLET"]
TXN_TYPES = [
    ("DEP", "Deposit", "credit"),
    ("WDR", "Withdrawal", "debit"),
    ("TRF", "Transfer", "mixed"),
    ("POS", "Card Purchase", "debit"),
    ("FEE", "Bank Fee", "debit"),
    ("REV", "Reversal", "mixed"),
]
ACCOUNT_TYPES = ["SAVINGS", "CURRENT", "SALARY", "LOAN", "WALLET"]
CUSTOMER_SEGMENTS = ["RETAIL", "SME", "PREMIUM", "STUDENT", "SENIOR"]
ACCOUNT_STATUS = ["ACTIVE", "DORMANT", "CLOSED", "BLOCKED"]
TXN_STATUS = ["SUCCESS", "FAILED", "REVERSED", "PENDING"]
PROVINCES = [
    "Koshi",
    "Madhesh",
    "Bagmati",
    "Gandaki",
    "Lumbini",
    "Karnali",
    "Sudurpashchim",
]

MCCS = [
    ("5411", "Grocery Stores", "LOW"),
    ("5812", "Restaurants", "LOW"),
    ("6011", "ATM Cash Withdrawal", "MEDIUM"),
    ("5732", "Electronics", "MEDIUM"),
    ("7995", "Gaming", "HIGH"),
    ("4829", "Money Transfer", "HIGH"),
    ("4111", "Transport", "LOW"),
    ("5912", "Pharmacy", "LOW"),
]

FIRST_NAMES = [
    "Aarav",
    "Anika",
    "Subash",
    "Maya",
    "Ramesh",
    "Nisha",
    "Suman",
    "Rita",
    "Bikash",
    "Puja",
    "Kiran",
    "Asmita",
]
LAST_NAMES = [
    "Shrestha",
    "Karki",
    "Rai",
    "Gurung",
    "Thapa",
    "Maharjan",
    "Tamang",
    "Yadav",
    "Poudel",
    "Bista",
]


def ensure_dirs(base: Path) -> dict[str, Path]:
    paths = {
        "reference": base / "reference",
        "customers": base / "batch" / "customers",
        "accounts": base / "batch" / "accounts",
        "transactions": base / "batch" / "transactions",
        "balances": base / "batch" / "balances",
        "settlements": base / "batch" / "settlements",
        "streaming": base / "streaming",
        "manifests": base / "manifests",
    }
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    return paths


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def money(rng: random.Random, min_amt: float, max_amt: float) -> float:
    return round(rng.uniform(min_amt, max_amt), 2)


def random_ts(rng: random.Random, d: date) -> str:
    seconds = rng.randint(0, 86399)
    return (
        datetime.combine(d, datetime.min.time()) + timedelta(seconds=seconds)
    ).isoformat(timespec="seconds")


def generate_reference(
    paths: dict[str, Path], rng: random.Random, branch_count: int = 40
) -> tuple[list[dict], list[dict], list[dict]]:
    branches = []
    for i in range(1, branch_count + 1):
        province = rng.choice(PROVINCES)
        branches.append(
            {
                "branch_id": f"BR{i:04d}",
                "branch_name": f"{province} Branch {i:03d}",
                "province": province,
                "region": "REGION_" + str((i % 5) + 1),
                "opened_date": str(
                    date(2010 + (i % 12), ((i % 12) + 1), min((i % 27) + 1, 28))
                ),
                "is_active": "true",
            }
        )
    tx_types = [
        {"transaction_type_code": c, "transaction_type_name": n, "balance_direction": b}
        for c, n, b in TXN_TYPES
    ]
    mccs = [
        {"merchant_category_code": c, "merchant_category_name": n, "risk_category": r}
        for c, n, r in MCCS
    ]
    write_csv(paths["reference"] / "branches.csv", branches, list(branches[0].keys()))
    write_csv(
        paths["reference"] / "transaction_types.csv", tx_types, list(tx_types[0].keys())
    )
    write_csv(
        paths["reference"] / "merchant_categories.csv", mccs, list(mccs[0].keys())
    )
    return branches, tx_types, mccs


def generate_customers(
    paths: dict[str, Path], rng: random.Random, as_of: date, count: int
) -> list[dict]:
    rows = []
    for i in range(1, count + 1):
        segment = rng.choice(CUSTOMER_SEGMENTS)
        rows.append(
            {
                "customer_id": f"CUST{i:08d}",
                "customer_name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
                "segment": segment,
                "age_band": rng.choice(["18-24", "25-34", "35-44", "45-54", "55+"]),
                "province": rng.choice(PROVINCES),
                "risk_band": rng.choice(["LOW", "LOW", "MEDIUM", "HIGH"]),
                "created_date": str(as_of - timedelta(days=rng.randint(30, 3000))),
                "snapshot_date": str(as_of),
                "is_active": rng.choice(["true", "true", "true", "false"]),
            }
        )
    write_csv(
        paths["customers"] / f"customer_snapshot_{as_of}.csv",
        rows,
        list(rows[0].keys()),
    )
    return rows


def generate_accounts(
    paths: dict[str, Path],
    rng: random.Random,
    as_of: date,
    customers: list[dict],
    branches: list[dict],
    count: int,
) -> list[dict]:
    rows = []
    for i in range(1, count + 1):
        cust = rng.choice(customers)
        branch = rng.choice(branches)
        status = rng.choices(ACCOUNT_STATUS, weights=[88, 7, 3, 2])[0]
        rows.append(
            {
                "account_id": f"ACC{i:010d}",
                "customer_id": cust["customer_id"],
                "branch_id": branch["branch_id"],
                "account_type": rng.choice(ACCOUNT_TYPES),
                "account_status": status,
                "opened_date": str(as_of - timedelta(days=rng.randint(1, 2500))),
                "currency": "NPR",
                "snapshot_date": str(as_of),
            }
        )
    write_csv(
        paths["accounts"] / f"account_snapshot_{as_of}.csv", rows, list(rows[0].keys())
    )
    return rows


def generate_transactions_for_day(
    paths: dict[str, Path],
    rng: random.Random,
    d: date,
    accounts: list[dict],
    mccs: list[dict],
    count: int,
) -> list[dict]:
    active_accounts = [
        a for a in accounts if a["account_status"] == "ACTIVE"
    ] or accounts
    rows = []
    for i in range(count):
        account = rng.choice(active_accounts)
        txn_type = rng.choices(
            ["DEP", "WDR", "TRF", "POS", "FEE"], weights=[20, 20, 25, 30, 5]
        )[0]
        channel = rng.choice(CHANNELS)
        status = rng.choices(TXN_STATUS, weights=[90, 5, 3, 2])[0]
        amount = money(rng, 10, 50000)
        if rng.random() < 0.002:
            amount = 0
        merchant = (
            rng.choice(mccs)
            if txn_type in ["POS", "TRF"]
            else {"merchant_category_code": "", "risk_category": ""}
        )
        original_id = ""
        if status == "REVERSED" and rows:
            original_id = rng.choice(rows)["transaction_id"]
        txn_id = f"TXN{d.strftime('%Y%m%d')}{i:09d}"
        rows.append(
            {
                "transaction_id": txn_id,
                "account_id": account["account_id"],
                "customer_id": account["customer_id"],
                "branch_id": account["branch_id"],
                "transaction_ts": random_ts(rng, d),
                "transaction_date": str(d),
                "transaction_type_code": txn_type,
                "channel": channel,
                "merchant_category_code": merchant["merchant_category_code"],
                "amount": amount,
                "fee_amount": round(amount * rng.choice([0, 0.001, 0.002, 0.005]), 2),
                "status": status,
                "currency": "NPR",
                "original_transaction_id": original_id,
                "source_system": rng.choice(
                    ["CORE_BANKING", "CARD_SWITCH", "MOBILE_BANKING"]
                ),
            }
        )
    # intentional duplicates
    for dup in rng.sample(rows, k=max(1, count // 500)):
        duplicated = dict(dup)
        rows.append(duplicated)
    rng.shuffle(rows)
    write_csv(
        paths["transactions"] / f"transactions_{d}.csv", rows, list(rows[0].keys())
    )
    return rows


def generate_balances_for_day(
    paths: dict[str, Path],
    rng: random.Random,
    d: date,
    accounts: list[dict],
    txns: list[dict],
) -> list[dict]:
    by_account = defaultdict(lambda: {"debit": 0.0, "credit": 0.0})
    for t in txns:
        if t["status"] != "SUCCESS":
            continue
        amt = float(t["amount"])
        if t["transaction_type_code"] == "DEP":
            by_account[t["account_id"]]["credit"] += amt
        else:
            by_account[t["account_id"]]["debit"] += amt
    rows = []
    for a in accounts:
        opening = money(rng, 1000, 500000)
        debit = round(by_account[a["account_id"]]["debit"], 2)
        credit = round(by_account[a["account_id"]]["credit"], 2)
        closing = round(opening + credit - debit, 2)
        rows.append(
            {
                "balance_date": str(d),
                "account_id": a["account_id"],
                "opening_balance": opening,
                "credit_total": credit,
                "debit_total": debit,
                "closing_balance": closing,
                "currency": "NPR",
            }
        )
    write_csv(
        paths["balances"] / f"account_balances_{d}.csv", rows, list(rows[0].keys())
    )
    return rows


def generate_settlement_for_day(
    paths: dict[str, Path], rng: random.Random, d: date, txns: list[dict]
) -> list[dict]:
    grouped = defaultdict(lambda: {"count": 0, "amount": 0.0, "fee": 0.0})
    for t in txns:
        if t["status"] != "SUCCESS":
            continue
        key = (t["channel"], t["transaction_type_code"])
        grouped[key]["count"] += 1
        grouped[key]["amount"] += float(t["amount"])
        grouped[key]["fee"] += float(t["fee_amount"])
    rows = []
    for (channel, txn_type), g in grouped.items():
        amount = round(g["amount"], 2)
        # intentional small mismatches
        if rng.random() < 0.12:
            amount = round(amount + rng.choice([-1, 1]) * money(rng, 10, 500), 2)
        rows.append(
            {
                "settlement_date": str(d),
                "channel": channel,
                "transaction_type_code": txn_type,
                "settled_transaction_count": g["count"],
                "settled_gross_amount": amount,
                "settled_fee_amount": round(g["fee"], 2),
                "currency": "NPR",
                "settlement_batch_id": f"SETTLE-{d.strftime('%Y%m%d')}-{channel}-{txn_type}",
            }
        )
    write_csv(paths["settlements"] / f"settlement_{d}.csv", rows, list(rows[0].keys()))
    return rows


def write_streaming_events(
    paths: dict[str, Path], rng: random.Random, d: date, txns: list[dict]
) -> None:
    event_path = paths["streaming"] / f"transaction_events_{d}.jsonl"
    malformed_path = paths["streaming"] / f"malformed_events_{d}.jsonl"
    with event_path.open("w", encoding="utf-8") as f:
        sample_size = min(len(txns), max(100, len(txns) // 3))
        for t in rng.sample(txns, k=sample_size):
            # Some events arrive late or out of order.
            event_ts = datetime.fromisoformat(t["transaction_ts"])
            if rng.random() < 0.05:
                event_ts += timedelta(hours=rng.randint(2, 48))
            event = {
                "event_id": str(uuid.uuid4()),
                "event_type": "TRANSACTION_CREATED",
                "event_ts": event_ts.isoformat(timespec="seconds"),
                "transaction_id": t["transaction_id"],
                "account_id": (
                    t["account_id"] if rng.random() > 0.003 else "ACC_UNKNOWN"
                ),
                "customer_id": t["customer_id"],
                "branch_id": t["branch_id"],
                "transaction_type_code": t["transaction_type_code"],
                "channel": t["channel"],
                "amount": float(t["amount"]),
                "fee_amount": float(t["fee_amount"]),
                "status": t["status"],
                "currency": "NPR",
                "merchant_category_code": t["merchant_category_code"],
                "source_system": t["source_system"],
            }
            f.write(json.dumps(event) + "\n")
            if rng.random() < 0.01:
                f.write(json.dumps(event) + "\n")
    with malformed_path.open("w", encoding="utf-8") as f:
        f.write(
            '{"event_id": "bad-1", "transaction_id": null, "amount": "not-a-number"}\n'
        )
        f.write("{this is not valid json}\n")
        f.write(
            json.dumps({"event_id": "bad-3", "event_type": "TRANSACTION_CREATED"})
            + "\n"
        )


def write_manifest(paths: dict[str, Path]) -> None:
    rows = []
    base = paths["manifests"].parent
    for p in sorted(base.rglob("*")):
        if p.is_file() and "manifests" not in p.parts:
            rows.append(
                {
                    "relative_path": str(p.relative_to(base)),
                    "file_size_bytes": p.stat().st_size,
                    "sha256": file_hash(p),
                    "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
                }
            )
    if rows:
        write_csv(
            paths["manifests"] / "source_manifest.csv", rows, list(rows[0].keys())
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic banking source datasets."
    )
    parser.add_argument("--output-dir", default="output", help="Output folder")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--customers", type=int, default=5000)
    parser.add_argument("--accounts", type=int, default=8000)
    parser.add_argument("--transactions-per-day", type=int, default=20000)
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--seed", type=int, default=14)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    base = Path(args.output_dir)
    paths = ensure_dirs(base)
    start = date.fromisoformat(args.start_date)

    branches, _, mccs = generate_reference(paths, rng)
    customers = generate_customers(paths, rng, start, args.customers)
    accounts = generate_accounts(paths, rng, start, customers, branches, args.accounts)

    for offset in range(args.days):
        d = start + timedelta(days=offset)
        txns = generate_transactions_for_day(
            paths, rng, d, accounts, mccs, args.transactions_per_day
        )
        generate_balances_for_day(paths, rng, d, accounts, txns)
        generate_settlement_for_day(paths, rng, d, txns)
        write_streaming_events(paths, rng, d, txns)

    write_manifest(paths)
    print(f"Generated banking source datasets under: {base.resolve()}")
    print(
        "Intentional issues are included for data-quality and reconciliation exercises."
    )


if __name__ == "__main__":
    main()
