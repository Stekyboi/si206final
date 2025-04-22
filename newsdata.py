import os
import json
import time
import sqlite3
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


# --- Constants ---
API_KEY_FILE  = "api_key_thenewsapi.txt"
DB_PATH       = "news_articles.db"
PROGRESS_FILE = "thenewsapi_progress.json"

DAILY_LIMIT    = 100               # free plan limit
ARTICLES_PER_INSERT = 25           # commit chunk size
CALLS_PER_MONTH     = 1            # one API call per month (3 articles)

START_YEAR    = 2021
END_YEAR      = 2024

BASE_URL      = "https://api.thenewsapi.com/v1/news/top"

# --- Database Setup ---
def create_db_tables():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # Articles table with composite uniqueness on title and pub_date
    cur.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            article_uuid TEXT    UNIQUE,
            title        TEXT    NOT NULL,
            pub_date     TEXT    NOT NULL,
            year         INTEGER NOT NULL,
            month        INTEGER NOT NULL,
            day          INTEGER NOT NULL,
            description  TEXT,
            snippet      TEXT,
            language     TEXT,
            UNIQUE(title, pub_date)
        );
    """)
    # Sentiment table keyed 1‑to‑1 by article_id
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sentiment (
            article_id INTEGER PRIMARY KEY,
            score      REAL,
            magnitude  REAL,
            FOREIGN KEY(article_id) REFERENCES articles(id) ON DELETE CASCADE
        );
    """)
    conn.commit()
    conn.close()

# --- Progress State ---
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        return json.load(open(PROGRESS_FILE))
    return {"phase": "initial", "run": 1}

def save_progress(state):
    json.dump(state, open(PROGRESS_FILE, "w"))

# --- Fetching Top Stories ---
def fetch_top_stories(api_token, year, month):
    frm = datetime(year, month, 1).strftime("%Y-%m-%d")
    to_date = (datetime(year, month, 1) + relativedelta(months=1) - timedelta(days=1))
    params = {
        "api_token": api_token,
        "categories": "business,politics,tech",
        "language": "en",
        "published_after": frm,
        "published_before": to_date.strftime("%Y-%m-%d"),
    }
    print(params)
    resp = requests.get(BASE_URL, params=params)
    print(resp)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", [])

# --- Insertion ---
def insert_articles(articles):
    print(articles)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    inserted = 0
    for i in range(0, len(articles), ARTICLES_PER_INSERT):
        chunk = articles[i:i+ARTICLES_PER_INSERT]
        for art in chunk:
            uuid = art["uuid"]
            title = (art["title"] or "").strip()
            pub = art["published_at"]
            dt = datetime.fromisoformat(pub)
            y, m, d = dt.year, dt.month, dt.day
            desc = art["description"]
            snip = art["snippet"]
            print(f"uuid: {uuid}, title: {title}, pub: {pub}")
            cur.execute("""
                INSERT OR IGNORE INTO articles
                (article_uuid, title, pub_date, year, month, day, description, snippet, language)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (uuid, title, pub, y, m, d, desc, snip, art.get("language", "en")))

            cur.execute("SELECT id FROM articles WHERE article_uuid = ?", (uuid,))
            row = cur.fetchone()
            if not row:
                # No article row found (perhaps duplicate), so skip sentiment insert
                continue
            pk = row[0]

            cur.execute("INSERT OR IGNORE INTO sentiment (article_id) VALUES (?)", (pk,))
            inserted += 1
        conn.commit()
        print(f"Committed {len(chunk)} rows (total {inserted}).")
    conn.close()

# --- Main Orchestrator ---
def main():
    create_db_tables()
    api_token = open(API_KEY_FILE).read().strip()
    prog = load_progress()

    # INITIAL PHASE: one month per run, 3 articles each, for months 1–4 (Jan–Apr 2021)
    if prog["phase"] == "initial":
        month = prog["run"]
        print(f"Initial run: fetching up to 3 top stories for 2021-{month:02d}")
        arts = fetch_top_stories(api_token, START_YEAR, month)
        insert_articles(arts)

        # advance run/month
        prog["run"] += 1
        if prog["run"] > 4:
            # move to final phase, start at May 2021
            prog = {"phase": "final", "year": START_YEAR, "month": 5}
        save_progress(prog)
        return

    # FINAL PHASE: accumulate all remaining months into memory, insert at once
    if prog["phase"] == "final":
        year = prog["year"]
        month = prog["month"]
        calls = 0

        while calls < DAILY_LIMIT and (year < END_YEAR or (year == END_YEAR and month <= 12)):
            print(f"Fetching top stories for {year}-{month:02d}")
            arts = fetch_top_stories(api_token, year, month)
            insert_articles(arts)
            calls += CALLS_PER_MONTH

            # advance month/year
            month += 1
            if month > 12:
                month = 1
                year += 1

            # save after each month
            prog["year"], prog["month"] = year, month
            save_progress(prog)
            time.sleep(1)

        print(f"Done. Used {calls}/{DAILY_LIMIT} calls this run.")
        return

if __name__ == "__main__":
    main()