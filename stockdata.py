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

def pull_weekly_data_from_api(api_key, symbol, data_type='json'):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol={symbol}&apikey={api_key}&datatype={data_type}'
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}")
    return response.json()

def insert_weekly_data(db_name, table_name, data, n):
    # Process the data in chunks of n
    dates = list(data['Weekly Adjusted Time Series']) 
    total_dates = len(dates) 
    start_index = 0
    while start_index < total_dates:
    # Select a chunk of up to n dates.
        chunk_dates = dates[start_index:start_index + n]

        # Open a new connection to the database for this chunk.
        conn = sqlite3.connect(db_name)
        cur = conn.cursor()

        # Loop over the current n data points and insert into the table.
        for date in chunk_dates:
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
            """, (date, open_price, high_price, low_price, close_price, adjusted_close_price, volume, dividend_amount))
        
        # Commit and close the connection.
        conn.commit()
        conn.close()

        # Note the nth (or last) data point for the chunk.
        last_date = chunk_dates[-1]
        print(f"Inserted records up to: {last_date}")

        # Move the start index forward by n.
        start_index += n


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

    ticker = 'DIA'
    data_type = 'json'
    db_name = 'news_articles.db'
    table_name = 'DJI_Avg_weekly'

    create_db_and_table(db_name, table_name)
    print(f"Successfully created {table_name} table")

    insert_weekly_data(db_name, table_name, pull_weekly_data_from_api(api_key, ticker, data_type), 25)
    print(f"Weekly data for {ticker} inserted into {table_name}, successfully.")

    # visualize_data(db_name, table_name)
    # print(f"Data visualization for {table_name} completed successfully.")
