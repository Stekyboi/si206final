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
    import requests
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY_ADJUSTED&symbol={symbol}&apikey={api_key}&datatype={data_type}'
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}")
    return response.json()

def pull_weekly_data_from_api(api_key, symbol, data_type):
    import requests
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

def insert_weekly_data_into_db(db_name, table_name, data):
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
    table_name = 'SPY_data'

    # create_db_and_table(db_name, table_name)
    # print(f"Database '{db_name}' and table '{table_name}' created successfully.")

    # insert_monthly_data_into_db(db_name, table_name, pull_monthly_data_from_api(api_key, ticker, data_type))
    # print(f"Data for {ticker} inserted into '{table_name}' successfully.")

    # visualize_data(db_name, table_name)
    # print(f"Data visualization for '{table_name}' completed successfully.")

    create_db_and_table(db_name, 'SPY_weekly')
    print("Successfully created 'SPY_weekly' table")

    insert_weekly_data_into_db(db_name, 'SPY_weekly', pull_weekly_data_from_api(api_key, ticker, data_type))
    print(f"Weekly data for {ticker} inserted into 'SPY_weekly', successfully.")

    visualize_data(db_name, 'SPY_weekly')
    print(f"Data visualization for 'SPY_weekly' completed successfully.")
