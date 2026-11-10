"""Local SQLite store: the full transaction history, so the sheet can always be rebuilt from it."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

from .transform import COLUMNS

_TABLE = "CREATE TABLE IF NOT EXISTS transactions ({}, PRIMARY KEY (id))".format(
    ", ".join(f"{c} {'REAL' if c in ('amount', 'original_amount') else 'TEXT'}" for c in COLUMNS)
)
_LOG = "CREATE TABLE IF NOT EXISTS scrape_log (company TEXT PRIMARY KEY, last_success TEXT, rows INTEGER)"


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(_TABLE)
    conn.execute(_LOG)
    return conn


def replace_window(db_path: Path, company: str, start_date: str, rows: list[dict], scraped_at: str) -> None:
    """Replace everything we hold for `company` from `start_date` on with the fresh scrape.

    Pending transactions change (amount, charge date) once they're settled, so instead of trying to
    match old and new versions we drop the scraped window and insert what the site shows today.
    """
    placeholders = ", ".join("?" for _ in COLUMNS)
    with closing(_connect(db_path)) as conn, conn:
        conn.execute("DELETE FROM transactions WHERE company = ? AND date >= ?", (company, start_date))
        conn.executemany(
            f"INSERT OR REPLACE INTO transactions ({', '.join(COLUMNS)}) VALUES ({placeholders})",
            [tuple(r[c] for c in COLUMNS) for r in rows],
        )
        conn.execute(
            "INSERT OR REPLACE INTO scrape_log (company, last_success, rows) VALUES (?, ?, ?)",
            (company, scraped_at, len(rows)),
        )


def all_transactions(db_path: Path) -> list[dict]:
    with closing(_connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            f"SELECT {', '.join(COLUMNS)} FROM transactions ORDER BY date DESC, processed_date DESC, company, card"
        )
        return [dict(r) for r in cur]


def scrape_log(db_path: Path) -> dict[str, tuple[str, int]]:
    with closing(_connect(db_path)) as conn:
        return {c: (ts, n) for c, ts, n in conn.execute("SELECT company, last_success, rows FROM scrape_log")}
