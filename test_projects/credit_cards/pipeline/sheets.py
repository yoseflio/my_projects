"""Write the transactions to a Google Sheet with a service account (rewrites the tabs each run)."""

from __future__ import annotations

from pathlib import Path

from .transform import COLUMNS


def _write_tab(spreadsheet, title: str, values: list[list]) -> None:
    import gspread

    rows, cols = max(len(values), 2), max(len(values[0]), 1)
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)
    ws.clear()
    ws.resize(rows=rows, cols=cols)
    # RAW: values go in exactly as given, so a merchant name starting with "=" is never run as a formula.
    ws.update(values=values, range_name="A1", value_input_option="RAW")
    ws.freeze(rows=1)


def publish(sheet_id: str, key_file: Path, transactions: list[dict], monthly: list[list], status: list[list]) -> str:
    """Rewrite the Transactions, Monthly and Status tabs. Returns the spreadsheet URL."""
    import gspread

    client = gspread.service_account(filename=str(key_file))
    spreadsheet = client.open_by_key(sheet_id)

    tx_values = [COLUMNS] + [["" if r[c] is None else r[c] for c in COLUMNS] for r in transactions]
    _write_tab(spreadsheet, "Transactions", tx_values)
    _write_tab(spreadsheet, "Monthly", monthly)
    _write_tab(spreadsheet, "Status", status)
    return spreadsheet.url
