import sqlite3
import json
import requests
import matplotlib.pyplot as plt
import pandas as pd

def get_api_key(filepath):
    with open(filepath, 'r', encoding='utf-8') as file:
        api_key = file.readline().strip()
        if not api_key:
            raise Exception("API key file is empty.")
        return api_key
    
def create_db_and_table(db_name, table_name):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    create_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                adjusted_close REAL,
                volume INTEGER,
                dividend_amount REAL);
                """
    cur.execute(create_query)
    conn.commit()
    conn.close()

def pull_monthly_data_from_api(api_key, symbol, data_type):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY_ADJUSTED&symbol={symbol}&apikey={api_key}&datatype={data_type}'
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}")
    return response.json()

def pull_weekly_data_from_api(api_key, symbol, data_type):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol={symbol}&apikey={api_key}&datatype={data_type}'
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}")
    return response.json()

def insert_monthly_data_into_db(db_name, table_name, data):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    for date in data['Monthly Adjusted Time Series']:
        day = date
        day_data = data['Monthly Adjusted Time Series'][date]
        open_price = day_data['1. open']
        high_price = day_data['2. high']
        low_price = day_data['3. low']
        close_price = day_data['4. close']
        adjusted_close_price = day_data['5. adjusted close']
        volume = day_data['6. volume']
        dividend_amount = day_data['7. dividend amount']
        
        cur.execute(f"""
                    INSERT OR IGNORE INTO {table_name} 
                    (date, open, high, low, close, adjusted_close, volume, dividend_amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, 
                    (day, open_price, high_price, low_price, close_price, adjusted_close_price, volume, dividend_amount))
    conn.commit()
    conn.close()

# NEW LIMITED INSERT FUNCTION
def insert_weekly_data_into_db_limited(db_name, table_name, data, max_new_items=25):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    count = 0
    # Iterate through the dates in ascending order (oldest first)
    for date in sorted(data['Weekly Adjusted Time Series'].keys()):
        # Stop once we've inserted 25 new rows
        if count >= max_new_items:
            break

        # Check whether this week has already been inserted
        cur.execute(f"SELECT 1 FROM {table_name} WHERE date = ?", (date,))
        if cur.fetchone():  # if the date already exists, skip it
            continue

        # Otherwise, extract the data and insert it.
        day_data = data['Weekly Adjusted Time Series'][date]
        open_price = day_data['1. open']
        high_price = day_data['2. high']
        low_price = day_data['3. low']
        close_price = day_data['4. close']
        adjusted_close_price = day_data['5. adjusted close']
        volume = day_data['6. volume']
        dividend_amount = day_data['7. dividend amount']
        
        cur.execute(f"""
                    INSERT INTO {table_name} 
                    (date, open, high, low, close, adjusted_close, volume, dividend_amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, 
                    (date, open_price, high_price, low_price, close_price, adjusted_close_price, volume, dividend_amount))
        count += 1

    conn.commit()
    conn.close()
    print(f"Inserted {count} new items into '{table_name}'.")

def insert_weekly_data_into_db(db_name, table_name, data):
    # The original unrestricted function, kept here for reference if needed.
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    for date in data['Weekly Adjusted Time Series']:
        day = date
        day_data = data['Weekly Adjusted Time Series'][date]
        open_price = day_data['1. open']
        high_price = day_data['2. high']
        low_price = day_data['3. low']
        close_price = day_data['4. close']
        adjusted_close_price = day_data['5. adjusted close']
        volume = day_data['6. volume']
        dividend_amount = day_data['7. dividend amount']
        
        cur.execute(f"""
                    INSERT OR IGNORE INTO {table_name} 
                    (date, open, high, low, close, adjusted_close, volume, dividend_amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, 
                    (day, open_price, high_price, low_price, close_price, adjusted_close_price, volume, dividend_amount))
    conn.commit()
    conn.close()

def visualize_data(db_name, table_name):
    conn = sqlite3.connect(db_name)
    query = f"SELECT date, close FROM {table_name}"
    df = pd.read_sql_query(query, conn)
    conn.close()

    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    plt.figure(figsize=(10, 5))
    plt.plot(df.index, df['close'], label='Close Price')
    plt.title(f'{table_name} Close Price Over Time')
    plt.xlabel('Date')
    plt.ylabel('Close Price')
    plt.legend()
    plt.show()

if __name__ == "__main__":
    api_key = get_api_key('api_key.txt')
    print("API key retrieved successfully.")

    ticker = 'SPY'
    data_type = 'json'
    db_name = 'stock_data.db'
    
    # Uncomment or comment as needed for monthly vs weekly data processing
    # table_name = 'SPY_data'
    # create_db_and_table(db_name, table_name)
    # print(f"Database '{db_name}' and table '{table_name}' created successfully.")
    # insert_monthly_data_into_db(db_name, table_name, pull_monthly_data_from_api(api_key, ticker, data_type))
    # print(f"Data for {ticker} inserted into '{table_name}' successfully.")
    # visualize_data(db_name, table_name)
    # print(f"Data visualization for '{table_name}' completed successfully.")

    # Set up the weekly table and process data in batches of 25.
    weekly_table = 'SPY_weekly'
    create_db_and_table(db_name, weekly_table)
    print("Successfully created or verified existence of 'SPY_weekly' table")

    # Call the limited insertion function: it will insert the 25 oldest weeks that are not yet stored.
    weekly_data = pull_weekly_data_from_api(api_key, ticker, data_type)
    insert_weekly_data_into_db_limited(db_name, weekly_table, weekly_data, max_new_items=25)
    print(f"Weekly data for {ticker} processed in limited batch (max 25 new items).")

    visualize_data(db_name, weekly_table)
    print(f"Data visualization for '{weekly_table}' completed successfully.")
