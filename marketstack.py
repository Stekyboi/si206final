import sqlite3
import requests
from datetime import datetime, timedelta

def get_api_key(filepath):
    with open(filepath, 'r', encoding='utf-8') as file:
        api_key = file.readline().strip()
        if not api_key:
            raise Exception("API key file is empty.")
        return api_key
    
def fetch_weekly_eod(
    api_key: str,
    ticker: str,
    db_name: str,
    table_name: str,
    start_date: str,
    weeks: int = 25
):
    """
    Fetches weekly end-of-day data for `ticker` from Marketstack v2 and stores it in SQLite.
    Uses the query endpoint to get one bulk of daily data, then samples weekly points.
    """
    print(f"🔗 Connecting to database `{db_name}`…")
    conn = sqlite3.connect(db_name)
    cur  = conn.cursor()

    # ensure we have a state table
    cur.execute("""
      CREATE TABLE IF NOT EXISTS fetch_state (
        ticker TEXT PRIMARY KEY,
        last_date TEXT
      )
    """)

    # figure out where to resume
    cur.execute("SELECT last_date FROM fetch_state WHERE ticker = ?", (ticker,))
    row = cur.fetchone()
    if row and row[0]:
        date_from = datetime.fromisoformat(row[0]) - timedelta(days=1)
        print(f"▶️ Resuming from day after last_date {row[0]} → {date_from.date()}")
    else:
        date_from = datetime.strptime(start_date, "%Y-%m-%d")
        print(f"▶️ No prior state: starting from provided start_date {date_from.date()}")

    # create your EOD table (matching the schema you pasted)
    cur.execute(f"""
      CREATE TABLE IF NOT EXISTS {table_name} (
        date TEXT PRIMARY KEY,
        open REAL, high REAL, low REAL, close REAL, volume REAL,
        adj_open REAL, adj_high REAL, adj_low REAL, adj_close REAL, adj_volume REAL
      )
    """)

    # compute the end of our full window (roughly weeks*7 days)
    date_to = date_from - timedelta(weeks=weeks)
    print(f"📅 Fetching daily data from {date_from.date()} to {date_to.date()}…")

    # ensure date_from <= date_to for the API call
    start, end = sorted([date_from, date_to])
    print(f"📅 Requesting data from {start.date()} → {end.date()}")

    url = "https://api.marketstack.com/v2/eod"
    params = {
        "access_key": api_key,
        "symbols":    ticker,
        "date_from":  start.strftime("%Y-%m-%d"),
        "date_to":    end.strftime("%Y-%m-%d"),
        "limit":      1000,
        "sort":       "ASC"    # or "DESC" if you prefer reverse order
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    all_data = resp.json().get("data", [])

    if not all_data:
        print("⚠️  No data returned for that entire range!  ")
    else:
        # subsample roughly once per week: take every 5th trading day
        weekly = all_data[::5]
        print(f"✅ Retrieved {len(all_data)} daily records; sampling {len(weekly)} weekly points.")

        for rec in weekly:
            cur.execute(f"""
              INSERT OR IGNORE INTO {table_name}
              (date, open, high, low, close, volume,
               adj_open, adj_high, adj_low, adj_close, adj_volume)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rec["date"],      rec["open"],      rec["high"],     rec["low"],
                rec["close"],     rec["volume"],
                rec["adj_open"],  rec["adj_high"],  rec["adj_low"],  rec["adj_close"], rec["adj_volume"]
            ))
            print(f"   • inserted {rec['date']}")

        # update our state to the last inserted date
        last_to_save = start.strftime("%Y-%m-%d")
        cur.execute("""
                REPLACE INTO fetch_state (ticker, last_date)
                VALUES (?, ?)
                """, (ticker, last_to_save))
        print(f"🔄 Updated fetch_state: {ticker} → {last_to_save}")

    conn.commit()
    conn.close()
    print("🔒 Done; database closed.")

if __name__ == "__main__":
    # Example invocation — replace with your real values!
    fetch_weekly_eod(
        api_key=get_api_key('marketstack_api.txt'),
        ticker="SPY",
        db_name="test.db",
        table_name="SPY_test",
        start_date="2025-03-01",
        weeks=25
    )
