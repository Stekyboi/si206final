import sqlite3
import requests


def get_api_key(filepath):
    """
    Read the Alpha Vantage API key from a plaintext file.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        key = f.readline().strip()
        if not key:
            raise ValueError("API key file is empty.")
        return key


def create_db_and_table(db_name, table_name):
    """
    Create the main data table and a fetch_state table to track progress.
    """
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # Main time series table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adjusted_close REAL,
            volume INTEGER,
            dividend_amount REAL
        );
    """
    )

    # State table to remember last date processed
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fetch_state_AV (
            table_name TEXT PRIMARY KEY,
            last_date_processed TEXT
        );
    """
    )

    conn.commit()
    conn.close()


def pull_weekly_data_from_api(api_key, symbol):
    """
    Fetch JSON from Alpha Vantage's TIME_SERIES_WEEKLY_ADJUSTED endpoint.
    """
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_WEEKLY_ADJUSTED",
        "symbol": symbol,
        "apikey": api_key,
        "datatype": "json"
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"API request failed: {response.status_code} - {response.text}")

    data = response.json()
    if "Weekly Adjusted Time Series" not in data:
        raise KeyError("Expected 'Weekly Adjusted Time Series' in API response.")
    return data


def insert_weekly_data(db_name, table_name, data, n):
    """
    Insert the next n weekly data points based on most recent, tracking progress in fetch_state.
    """
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # Ensure state table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fetch_state_AV (
            table_name TEXT PRIMARY KEY,
            last_date_processed TEXT
        );
    """
    )

    # Retrieve last processed date
    cur.execute("SELECT last_date_processed FROM fetch_state_AV WHERE table_name = ?", (table_name,))
    row = cur.fetchone()
    last_date = row[0] if row else None

    ts = data["Weekly Adjusted Time Series"]
    dates = sorted(ts.keys(), reverse=True)

    if last_date and last_date in dates:
        start_idx = dates.index(last_date) + 1
    elif last_date:
        print(f"Warning: Last processed date {last_date} not found. Resetting to most recent.")
        start_idx = 0
    else:
        start_idx = 0

    # Determine chunk to insert
    chunk = dates[start_idx:start_idx + n]
    if not chunk:
        print("No new data to insert.")
        conn.close()
        return

    # Insert records
    for date in chunk:
        record = ts[date]
        cur.execute(f"""
            INSERT OR IGNORE INTO {table_name}
            (date, open, high, low, close, adjusted_close, volume, dividend_amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """.strip(), (
            date,
            float(record["1. open"]),
            float(record["2. high"]),
            float(record["3. low"]),
            float(record["4. close"]),
            float(record["5. adjusted close"]),
            int(record["6. volume"]),
            float(record["7. dividend amount"])
        ))

    # Update fetch_state
    new_last_date = chunk[-1]
    if row:
        cur.execute(
            "UPDATE fetch_state_AV SET last_date_processed = ? WHERE table_name = ?",
            (new_last_date, table_name)
        )
    else:
        cur.execute(
            "INSERT INTO fetch_state_AV (table_name, last_date_processed) VALUES (?, ?)",
            (table_name, new_last_date)
        )

    conn.commit()
    conn.close()
    print(f"Inserted {len(chunk)} records from {chunk[0]} to {chunk[-1]}")


if __name__ == "__main__":
    api_key = get_api_key('api_key.txt')
    symbol = 'SPY'
    db_name = 'test.db'
    table_name = f"{symbol}_weekly_adjusted"

    create_db_and_table(db_name, table_name)
    data = pull_weekly_data_from_api(api_key, symbol)
    insert_weekly_data(db_name, table_name, data, n=25)
