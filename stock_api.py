"""
Stock data API module for fetching stock market data from Alpha Vantage.
This module handles fetching, processing, and storing stock data in SQLite database.
"""
import sqlite3
import requests
from api_keys import get_alpha_vantage_key

# Database information
DB_NAME = 'stock_data.db'
TABLE_NAME_TEMPLATE = "{}_weekly_adjusted"

def create_stock_tables(db_name, ticker):
    """
    Create the necessary tables in the database for stock data and tracking.
    
    Args:
        db_name (str): Name of the SQLite database file.
        ticker (str): Stock ticker symbol.
        
    Returns:
        str: The name of the created table.
    """
    table_name = TABLE_NAME_TEMPLATE.format(ticker)
    
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
    """)

    # State table to remember last date processed
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fetch_state_stocks (
            table_name TEXT PRIMARY KEY,
            last_date_processed TEXT
        );
    """)

    conn.commit()
    conn.close()
    
    return table_name

def fetch_stock_data(ticker, api_key_path):
    """
    Fetch weekly adjusted time series data from Alpha Vantage API.
    
    Args:
        ticker (str): Stock ticker symbol.
        api_key_path (str): Path to file containing API key.
        
    Returns:
        dict: JSON data from the API or raises an exception on failure.
    """
    api_key = get_alpha_vantage_key(api_key_path)
    
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_WEEKLY_ADJUSTED",
        "symbol": ticker,
        "apikey": api_key,
        "datatype": "json"
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"API request failed: {response.status_code} - {response.text}")

    data = response.json()
    if "Weekly Adjusted Time Series" not in data:
        if "Note" in data:
            # API limit reached
            raise Exception(f"API limit reached: {data['Note']}")
        raise KeyError("Expected 'Weekly Adjusted Time Series' in API response.")
    
    return data

def insert_stock_data(data, db_name, ticker, max_items):
    """
    Insert up to max_items weekly data points into the database.
    Tracks progress to ensure we don't insert duplicates and
    can continue on subsequent runs.
    
    Args:
        data (dict): API response data containing time series.
        db_name (str): Database file name.
        ticker (str): Stock ticker symbol.
        max_items (int): Maximum number of items to insert in one run.
        
    Returns:
        int: Number of new records inserted.
    """
    table_name = TABLE_NAME_TEMPLATE.format(ticker)
    
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # Retrieve last processed date
    cur.execute("SELECT last_date_processed FROM fetch_state_stocks WHERE table_name = ?", 
                (table_name,))
    row = cur.fetchone()
    last_date = row[0] if row else None

    ts = data["Weekly Adjusted Time Series"]
    dates = sorted(ts.keys(), reverse=True)

    # Determine starting point
    if last_date and last_date in dates:
        start_idx = dates.index(last_date) + 1
    else:
        start_idx = 0

    # Determine chunk to insert
    chunk = dates[start_idx:start_idx + max_items]  
    if not chunk:
        print("No new data to insert.")
        conn.close()
        return 0
        
    # Insert records
    inserted = 0
    for date in chunk:
        record = ts[date]
        cur.execute(f"""
            INSERT INTO {table_name}
            (date, open, high, low, close, adjusted_close, volume, dividend_amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
            date,
            float(record["1. open"]),
            float(record["2. high"]),
            float(record["3. low"]),
            float(record["4. close"]),
            float(record["5. adjusted close"]),
            int(record["6. volume"]),
            float(record["7. dividend amount"])
        ))
        inserted += 1

    # Update fetch_state
    new_last_date = chunk[-1]
    if row:
        cur.execute(
            "UPDATE fetch_state_stocks SET last_date_processed = ? WHERE table_name = ?",
            (new_last_date, table_name)
        )
    else:
        cur.execute(
            "INSERT INTO fetch_state_stocks (table_name, last_date_processed) VALUES (?, ?)",
            (table_name, new_last_date)
        )

    conn.commit()
    conn.close()
    
    return inserted

def get_stock_data(ticker, max_items, db_name, api_key_path):
    """
    Main function to fetch and store stock data.
    Creates tables if needed, fetches data, and inserts it into the database.
    
    Args:
        ticker (str): Stock ticker symbol.
        max_items (int): Maximum number of items to insert in one run.
        db_name (str): Database file name.
        api_key_path (str): Path to file containing API key.
        
    Returns:
        int: Number of new records inserted.
    """
    table_name = create_stock_tables(db_name, ticker)
    data = fetch_stock_data(ticker, api_key_path)
    inserted = insert_stock_data(data, db_name, ticker, max_items)
    
    return inserted

def count_stock_records(ticker, db_name):
    """
    Count the total number of stock records for a ticker.
    
    Args:
        ticker (str): Stock ticker symbol.
        db_name (str): Database file name.
        
    Returns:
        int: Total number of records.
    """
    table_name = TABLE_NAME_TEMPLATE.format(ticker)
    
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
    except sqlite3.OperationalError:
        # Table doesn't exist
        count = 0
        
    conn.close()
    return count

if __name__ == "__main__":
    ticker = "SPY"
    db_name = DB_NAME
    api_key_path = 'api_key.txt'
    max_items = 25
    
    inserted = get_stock_data(ticker, max_items, db_name, api_key_path)
    total = count_stock_records(ticker, db_name)
    
    print(f"Inserted {inserted} new stock records for {ticker}.")
    print(f"Total records for {ticker}: {total}") 