# Credit cards → Google Sheet (daily, with Airflow)

Every day at 07:00 Israel time, Airflow logs in to Cal, Max and Isracard, downloads your transactions,
keeps the full history in a local SQLite file, and rewrites a Google Sheet with three tabs:

| Tab | What's in it |
|---|---|
| **Transactions** | Every transaction, newest first. `amount` > 0 = you paid, < 0 = refund |
| **Monthly** | Total per billing month, company and card (ILS) |
| **Status** | Which companies updated in the last run, and when each last succeeded |

```
scrape_visaCal ──┐
scrape_max ──────┼──> publish_to_sheet      (a company without a connection is skipped;
scrape_isracard ─┘                           if one fails, the others are still published)
```

## Files

| Path | Role |
|---|---|
| `credit_cards_dag.py` | The Airflow DAG |
| `scraper/scrape.js` | Node script using [israeli-bank-scrapers](https://github.com/eshaham/israeli-bank-scrapers) |
| `pipeline/transform.py` | Converts each company's JSON to one row format + monthly summary |
| `pipeline/store.py` | SQLite history (`data/transactions.db`) |
| `pipeline/sheets.py` | Writes the tabs to Google Sheets |
| `Dockerfile` | Airflow image with Node.js, Chromium and gspread added |
| `data/`, `secrets/` | Created at runtime, **git-ignored**: raw scrapes, DB, browser profiles, Google key |

## One-time setup

### 1. Use the custom Airflow image

```bash
docker build -t airflow-credit-cards:3.4.0 ~/IdeaProjects/my_projects/test_projects/credit_cards
```

Add this line to `~/airflow/.env`, then restart Airflow:

```
AIRFLOW_IMAGE_NAME=airflow-credit-cards:3.4.0
```

```bash
cd ~/airflow && docker compose up -d
```

Rebuild only when `Dockerfile` or `scraper/package.json` changes. Edits to `.py` / `.js` files are picked up live.

### 2. Google service account (lets Airflow write to your sheet)

1. In [Google Cloud Console](https://console.cloud.google.com/): create a project → **APIs & Services → Enable APIs** → enable **Google Sheets API**.
2. **IAM & Admin → Service Accounts → Create**. No roles needed. Open it → **Keys → Add key → JSON**.
3. Save the downloaded file as `test_projects/credit_cards/secrets/google_service_account.json`.
4. Create an empty Google Sheet and **Share** it (Editor) with the service account's email
   (`something@your-project.iam.gserviceaccount.com`, it's inside the JSON file).
5. Copy the sheet id from its URL: `https://docs.google.com/spreadsheets/d/<THIS PART>/edit`.

### 3. Airflow settings (UI at http://localhost:8080)

**Admin → Variables → +**: key `credit_cards_sheet_id`, value = the sheet id.

**Admin → Connections → +** (type *Generic*), one per card company you have:

| Connection Id | Login | Password | Extra |
|---|---|---|---|
| `credit_card_cal` | Cal username | Cal password | |
| `credit_card_max` | Max username | Max password | |
| `credit_card_isracard` | Teudat zehut (ID) | Isracard password | `{"card6Digits": "123456"}` (last 6 digits of the card) |

Airflow stores connection passwords encrypted and hides them in logs. Never put them in the code.

### 4. First run

1. Unpause **credit_cards_daily**, then **Trigger DAG w/ config** with `days_back = 365` to load a year of history.
2. Check each task's log. Once it's green, the daily 07:00 runs fetch the last 45 days (to catch
   pending transactions that settle later).

## When something fails

- **Log in to the site in a normal browser first** and get past any "update details / accept terms" screen.
  The scraper can't click through those.
- **SMS / OTP prompts:** each company has a saved Chromium profile in `data/browser-profiles/`, so the site
  should see the same "device" every day. If a site insists on OTP every time, that company can't run
  unattended. Disable its connection and use a manual export for it.
- **Failure screenshot:** `data/raw/<run_id>/<company>-failure.png` shows the page where it got stuck.
- **Site changed / scraper broke:** bump `israeli-bank-scrapers` in `scraper/package.json` to the latest
  version and rebuild the image (step 1).
- `INVALID_PASSWORD` / `CHANGE_PASSWORD` / `ACCOUNT_BLOCKED`: fix it on the site. Don't let retries
  hammer a blocked account.

Note: this automates access to your own accounts for personal use. Keep it to once a day.
