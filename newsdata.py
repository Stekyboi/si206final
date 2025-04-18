import sqlite3
import requests
import json
import os
from datetime import datetime, timedelta
import time
from tqdm import tqdm

# --- Constants ---
PROGRESS_FILE = "news_backfill_progress.json"
LOG_FILE = "news_backfill.log"
DB_PATH = "news_articles.db"
API_KEY_FILE = "api_key_news.txt"
ARTICLES_PER_INSERT = 25
API_CALLS_PER_MONTH = 2
TOTAL_API_CALL_LIMIT = 300
BASE_URL = "https://api.newsdatahub.com/v1/news"
QUERY = "politics OR business OR finance OR technology OR government OR international OR economy"

# --- Logging ---
def log(message):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{timestamp}] {message}\n")
    print(message)

# --- API Key ---
def get_api_key(filepath):
    with open(filepath, 'r', encoding='utf-8') as file:
        return file.readline().strip()

# --- Database Setup ---
def create_db_tables():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            article_uuid TEXT    UNIQUE,
            title        TEXT    NOT NULL,
            pub_date     TEXT    NOT NULL,
            year         INTEGER NOT NULL,
            month        INTEGER NOT NULL,
            day          INTEGER NOT NULL,
            language     TEXT,
            UNIQUE(title, pub_date)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sentiment (
            article_id INTEGER PRIMARY KEY,
            sent_pos   REAL,
            sent_neg   REAL,
            sent_neu   REAL,
            score      REAL,
            magnitude  REAL,
            FOREIGN KEY(article_id) REFERENCES articles(id)
                             ON DELETE CASCADE
        )
    """)
    conn.commit()
    conn.close()

# --- Insertion ---
def insert_news_data_into_db(db_path, articles):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    inserted_total = 0

    for i in range(0, len(articles), ARTICLES_PER_INSERT):
        chunk = articles[i:i+ARTICLES_PER_INSERT]
        inserted = 0

        for article in chunk:
            uuid = article.get("id")
            title = (article.get("title") or "").strip()
            pub_date = article.get("pub_date")
            lang = article.get("language")
            sent = article.get("sentiment", {})

            try:
                dt = datetime.fromisoformat(pub_date)
            except Exception as e:
                log(f"Date parse error: {e}")
                continue

            cur.execute("""
                INSERT OR IGNORE INTO articles 
                (article_uuid, title, pub_date, year, month, day, language)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (uuid, title, pub_date, dt.year, dt.month, dt.day, lang))

            cur.execute("SELECT id FROM articles WHERE title=? AND pub_date=?;", (title, pub_date))
            (article_pk,) = cur.fetchone()

            cur.execute("""
                INSERT INTO sentiment (article_id, sent_pos, sent_neg, sent_neu)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(article_id) DO UPDATE SET
                    sent_pos = excluded.sent_pos,
                    sent_neg = excluded.sent_neg,
                    sent_neu = excluded.sent_neu;
            """, (
                article_pk,
                sent.get("pos"),
                sent.get("neg"),
                sent.get("neu")
            ))
            inserted += 1

        conn.commit()
        inserted_total += inserted
        log(f"Inserted {inserted} articles in this batch.")

    conn.close()
    log(f"Inserted {inserted_total} total articles this month.")

# --- Fetching ---
def fetch_articles(api_key, start_date, end_date, query, cursor=None):
    headers = {'X-Api-Key': api_key}
    params = {
        'q': query,
        'start_date': start_date,
        'end_date': end_date,
        'language': 'en',
    }
    if cursor:
        params['cursor'] = cursor

    response = requests.get(BASE_URL, headers=headers, params=params)
    if response.status_code == 429:
        log(f"Error {response.status_code}: {response.text}")
        return [], None, 429
    elif response.status_code != 200:
        log(f"Error {response.status_code}: {response.text}")
        return [], None, response.status_code

    data = response.json()
    return data.get("data", []), data.get("next_cursor"), None

# --- Progress Tracking ---
def save_progress(year, month):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"last_year": year, "last_month": month}, f)

def load_progress(default_year, default_month):
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            progress = json.load(f)
            return progress.get("last_year", default_year), progress.get("last_month", default_month)
    return default_year, default_month

# --- Main Orchestrator ---
def run_backfill(api_key, db_path, query, end_year=2015, start_year=2025, start_month=3):
    year, month = load_progress(start_year, start_month)
    current = datetime(year, month, 1)
    end = datetime(end_year, 1, 1)
    api_calls = 0

    while current >= end and api_calls + API_CALLS_PER_MONTH <= TOTAL_API_CALL_LIMIT:
        month_start = current.replace(day=1)
        month_end = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

        log(f"Fetching month: {current.strftime('%Y-%m')}")

        cursor = None
        all_articles = []

        for _ in range(API_CALLS_PER_MONTH):
            if api_calls >= TOTAL_API_CALL_LIMIT:
                log("Reached API call limit.")
                return
            articles, cursor, error = fetch_articles(
                api_key,
                month_start.strftime('%Y-%m-%d'),
                month_end.strftime('%Y-%m-%d'),
                query,
                cursor
            )
            api_calls += 1

            #checking for error 429
            if error == 429:
                log("Encountered rate limiting (HTTP 429). Saving progress and stopping further requests.")
                save_progress(current.year, current.month)
                return

            all_articles.extend(articles)
            time.sleep(1)

        if all_articles:
            insert_news_data_into_db(db_path, all_articles)
        else:
            log("No articles retrieved for this month.")

        save_progress(current.year, current.month)
        # Move one month earlier
        current = (current.replace(day=1) - timedelta(days=1)).replace(day=1)

    log(f"Backfill finished. Total API calls: {api_calls}")

# --- Main Execution ---
if __name__ == "__main__":
    api_key = get_api_key(API_KEY_FILE)
    create_db_tables()
    run_backfill(api_key, DB_PATH, QUERY)

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