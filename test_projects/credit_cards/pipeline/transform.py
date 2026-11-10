"""Turn the scraper's per-company JSON into one common row format, and build the monthly summary."""

from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

ISRAEL = ZoneInfo("Asia/Jerusalem")

COMPANY_LABELS = {"visaCal": "Cal", "max": "Max", "isracard": "Isracard"}

# Column order used everywhere: SQLite table, "Transactions" sheet tab.
COLUMNS = [
    "date",             # purchase date (Israel time), YYYY-MM-DD
    "processed_date",   # billing / charge date, YYYY-MM-DD
    "company",          # Cal / Max / Isracard
    "card",             # card / account number as the site shows it
    "description",      # merchant
    "amount",           # charged amount: positive = you paid, negative = refund/credit
    "currency",         # currency of `amount`
    "original_amount",  # amount in the original purchase currency (same sign convention)
    "original_currency",
    "installment",      # e.g. "3/12", empty for regular purchases
    "status",           # completed / pending
    "category",
    "memo",
    "type",             # normal / installments
    "id",               # stable id used for de-duplication
]


def _israel_date(iso: str | None) -> str:
    """'2026-11-09T22:00:00.000Z' -> '2026-11-10' (the sites send Israel midnight as UTC)."""
    if not iso:
        return ""
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    if dt.tzinfo is not None:
        dt = dt.astimezone(ISRAEL)
    return dt.date().isoformat()


def _flip(value) -> float | None:
    # Scrapers report purchases as negative numbers; a spending sheet reads better with them positive.
    return None if value is None else round(-float(value), 2)


def normalize(raw: dict) -> list[dict]:
    """Convert one scraper output file into a list of rows with the keys in COLUMNS."""
    company = COMPANY_LABELS.get(raw["company"], raw["company"])
    rows: list[dict] = []
    seen: Counter[str] = Counter()

    for account in raw.get("accounts", []):
        card = str(account.get("accountNumber") or "")
        for txn in account.get("txns", []):
            inst = txn.get("installments") or {}
            row = {
                "date": _israel_date(txn.get("date")),
                "processed_date": _israel_date(txn.get("processedDate")),
                "company": company,
                "card": card,
                "description": (txn.get("description") or "").strip(),
                "amount": _flip(txn.get("chargedAmount")),
                "currency": txn.get("chargedCurrency") or "ILS",
                "original_amount": _flip(txn.get("originalAmount")),
                "original_currency": txn.get("originalCurrency") or "",
                "installment": f"{inst['number']}/{inst['total']}" if inst.get("number") else "",
                "status": txn.get("status") or "",
                "category": txn.get("category") or "",
                "memo": (txn.get("memo") or "").strip(),
                "type": txn.get("type") or "",
            }
            key = "|".join(
                str(x)
                for x in (
                    company, card, txn.get("identifier") or "", row["date"], row["processed_date"],
                    row["description"], row["amount"], row["original_amount"], row["installment"],
                )
            )
            # Two genuinely identical purchases (same coffee, same day) must stay two rows.
            seen[key] += 1
            digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
            row["id"] = f"{digest}-{seen[key]}"
            rows.append(row)
    return rows


MONTHLY_HEADER = ["month", "company", "card", "total_amount", "transactions"]


def monthly_summary(rows: list[dict]) -> list[list]:
    """Totals per billing month, company and card, newest month first (ILS charges only)."""
    totals: dict[tuple, list] = defaultdict(lambda: [0.0, 0])
    for r in rows:
        if r.get("currency", "ILS") != "ILS" or r.get("amount") is None:
            continue
        month = (r.get("processed_date") or r.get("date") or "")[:7]
        bucket = totals[(month, r["company"], r["card"])]
        bucket[0] += r["amount"]
        bucket[1] += 1
    keys = sorted(totals, key=lambda k: (k[0], k[1], k[2]), reverse=True)
    return [MONTHLY_HEADER] + [[m, c, card, round(totals[(m, c, card)][0], 2), totals[(m, c, card)][1]] for m, c, card in keys]
