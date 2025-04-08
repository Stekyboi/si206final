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
    
def create_news_db_and_table(db_path, table_name):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    create_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                pub_date TEXT,
                language TEXT,
                sent_pos REAL,
                sent_neg REAL,
                sent_neu REAL);
                """
    cur.execute(create_query)
    conn.commit()
    conn.close()

def pull_data_from_api(api_key):
    BASE_URL = 'https://api.newsdatahub.com/v1/news'

    headers = { 
        'X-Api-Key': api_key,
        'User-Agent': 'YourApp/1.0'
    }

    params = {'q': 'politics AND business AND finance AND society'}
    response = requests.get(BASE_URL, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}")
    return response.json()

def insert_news_data_into_db(db_path, table_name, data):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    for article in data.get("data", []):
        title = article.get("title")
        pub_date = article.get("pub_date")
        language = article.get("language")
        sentiment = article.get("sentiment", {})

        sent_pos = sentiment.get("pos", 0.0)
        sent_neg = sentiment.get("neg", 0.0)
        sent_neu = sentiment.get("neu", 0.0)

        cur.execute(f"""
            INSERT INTO {table_name} 
            (title, pub_date, language, sent_pos, sent_neg, sent_neu)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (title, pub_date, language, sent_pos, sent_neg, sent_neu))

    conn.commit()
    conn.close()

# def visualize_data(db_name, table_name):
#     conn = sqlite3.connect(db_name)
#     query = f"SELECT date, close FROM {table_name}"
#     df = pd.read_sql_query(query, conn)
#     conn.close()

#     df['date'] = pd.to_datetime(df['date'])
#     df.set_index('date', inplace=True)

#     plt.figure(figsize=(10, 5))
#     plt.plot(df.index, df['close'], label='Close Price')
#     plt.title(f'{table_name} Close Price Over Time')
#     plt.xlabel('Date')
#     plt.ylabel('Close Price')
#     plt.legend()
#     plt.show()

if __name__ == "__main__":
    api_key = get_api_key('api_key_news.txt')
    print("API key retrieved successfully.")

    db_path = 'news_data.db'
    table_name = 'articles'

    create_news_db_and_table(db_path, table_name)
    print(f"Database '{db_path}' and table '{table_name}' created.")

    news_data = pull_data_from_api(api_key)
    insert_news_data_into_db(db_path, table_name, news_data)
    print(f"News data inserted into '{table_name}' successfully.")
