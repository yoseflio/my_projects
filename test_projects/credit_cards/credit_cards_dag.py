"""Daily credit card pipeline: scrape Cal / Max / Isracard -> SQLite history -> Google Sheet.

    scrape_visaCal ──┐
    scrape_max ──────┼──> publish_to_sheet
    scrape_isracard ─┘

Setup (Airflow UI → Admin): see credit_cards/README.md.
  Connections  credit_card_cal, credit_card_max, credit_card_isracard  (a missing one is skipped)
  Variable     credit_cards_sheet_id
  File         credit_cards/secrets/google_service_account.json
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pendulum
from airflow.sdk import Connection, Param, TriggerRule, Variable, dag, task
from airflow.sdk.exceptions import AirflowNotFoundException, AirflowSkipException

PROJECT = Path(__file__).resolve().parent
DATA = PROJECT / "data"
DB_PATH = DATA / "transactions.db"
SCRAPER = PROJECT / "scraper" / "scrape.js"
GOOGLE_KEY = PROJECT / "secrets" / "google_service_account.json"

# scraper company id -> Airflow connection id
COMPANIES = {
    "visaCal": "credit_card_cal",
    "max": "credit_card_max",
    "isracard": "credit_card_isracard",
}


def _run_dir(run_id: str) -> Path:
    return DATA / "raw" / re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def _credentials(company_id: str, conn: Connection) -> dict:
    if company_id == "isracard":
        extra = conn.extra_dejson
        card6 = str(extra.get("card6Digits", "")).strip()
        if not re.fullmatch(r"\d{6}", card6):
            raise ValueError('Connection credit_card_isracard needs Extra like {"card6Digits": "123456"}')
        return {"id": conn.login, "card6Digits": card6, "password": conn.password}
    return {"username": conn.login, "password": conn.password}


@dag(
    dag_id="credit_cards_daily",
    schedule="0 7 * * *",  # every day at 07:00 Israel time
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Jerusalem"),
    catchup=False,
    max_active_runs=1,
    tags=["finance"],
    params={
        "days_back": Param(45, type="integer", minimum=1, maximum=365,
                           description="How many days back to fetch. Use 365 once for the full history."),
    },
)
def credit_cards_daily():
    @task(retries=2, retry_delay=timedelta(minutes=15), execution_timeout=timedelta(minutes=15))
    def scrape(company_id: str, params=None, run_id=None) -> str:
        try:
            conn = Connection.get(COMPANIES[company_id])
        except AirflowNotFoundException:
            raise AirflowSkipException(f"No connection {COMPANIES[company_id]} configured - skipping {company_id}")

        start = (date.today() - timedelta(days=int(params["days_back"]))).isoformat()
        out = _run_dir(run_id) / f"{company_id}.json"
        cmd = [
            "node", str(SCRAPER),
            "--company", company_id,
            "--start-date", start,
            "--out", str(out),
            "--profile-dir", str(DATA / "browser-profiles" / company_id),
            "--screenshot", str(_run_dir(run_id) / f"{company_id}-failure.png"),
        ]
        env = {**os.environ, "SCRAPER_CREDENTIALS": json.dumps(_credentials(company_id, conn))}
        # Output goes straight to the task log. Credentials are passed only through env.
        result = subprocess.run(cmd, env=env, check=False)
        if result.returncode != 0:
            raise RuntimeError(f"Scraper for {company_id} failed (exit code {result.returncode}); see log above")
        return str(out)

    @task(trigger_rule=TriggerRule.ALL_DONE)  # publish whatever succeeded, even if one company failed
    def publish_to_sheet(run_id=None, params=None) -> None:
        from credit_cards.pipeline import sheets, store, transform

        run_dir = _run_dir(run_id)
        files = sorted(run_dir.glob("*.json")) if run_dir.exists() else []
        if not files:
            raise RuntimeError("No company was scraped successfully in this run - nothing to publish")

        updated = {}
        for f in files:
            raw = json.loads(f.read_text(encoding="utf-8"))
            rows = transform.normalize(raw)
            company = transform.COMPANY_LABELS[raw["company"]]
            store.replace_window(DB_PATH, company, raw["startDate"], rows, raw["scrapedAt"])
            updated[company] = len(rows)
            print(f"{company}: {len(rows)} transactions since {raw['startDate']}")

        transactions = store.all_transactions(DB_PATH)
        now = datetime.now(timezone.utc).astimezone(transform.ISRAEL).strftime("%Y-%m-%d %H:%M")
        status = [["company", "this run", "last successful update", "rows in last update"]]
        log = store.scrape_log(DB_PATH)
        for company_id in COMPANIES:
            label = transform.COMPANY_LABELS[company_id]
            last = log.get(label)
            status.append([
                label,
                "updated" if label in updated else "NOT updated - check Airflow logs",
                last[0] if last else "never",
                last[1] if last else 0,
            ])
        status += [[], ["sheet refreshed at", now], ["total transactions", len(transactions)]]

        sheet_id = Variable.get("credit_cards_sheet_id")
        url = sheets.publish(sheet_id, GOOGLE_KEY, transactions, transform.monthly_summary(transactions), status)
        print(f"Published {len(transactions)} transactions to {url}")

    scrapes = [scrape.override(task_id=f"scrape_{c}")(c) for c in COMPANIES]
    scrapes >> publish_to_sheet()


credit_cards_daily()
