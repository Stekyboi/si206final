"""
News data API module for fetching news articles from TheNewsAPI.
This module handles fetching, processing, and storing news data in SQLite database.
"""
import os
import json
import sqlite3
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from api_keys import get_news_api_key

# Database and API information
DB_NAME = 'stock_and_news.db'
PROGRESS_FILE = 'news_fetch_progress.json'
BASE_URL = "https://api.thenewsapi.com/v1/news/top"
MAX_ITEMS_PER_RUN = 25  # Maximum items to insert per run

def create_news_tables(db_name):
    """
    Create the necessary tables in the database for news articles.
    
    Args:
        db_name (str): Name of the SQLite database file.
    """
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    # Articles table with composite uniqueness on title and pub_date
    cur.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_uuid TEXT UNIQUE,
            title TEXT NOT NULL,
            pub_date TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL,
            description TEXT,
            snippet TEXT,
            UNIQUE(title, pub_date)
        );
    """)
    
    # Sentiment table keyed 1-to-1 by article_id
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sentiment (
            article_id INTEGER PRIMARY KEY,
            score REAL,
            magnitude REAL,
            FOREIGN KEY(article_id) REFERENCES articles(id) ON DELETE CASCADE
        );
    """)
    
    conn.commit()
    conn.close()

def load_progress():
    """
    Load the progress tracking state from a JSON file.
    
    Returns:
        dict: Progress state dictionary.
    """
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {"current_year": 2021, "current_month": 1, "offset": 0}

def save_progress(state):
    """
    Save the progress tracking state to a JSON file.
    
    Args:
        state (dict): Progress state dictionary.
    """
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(state, f)

def fetch_news_articles(year, month, api_key_path):
    """
    Fetch articles from TheNewsAPI for a specific year and month.
    
    Args:
        year (int): Year to fetch articles for.
        month (int): Month to fetch articles for.
        api_key_path (str): Path to file containing API key.
        
    Returns:
        list: List of article dictionaries.
    """
    api_token = get_news_api_key(api_key_path)
    
    # Calculate date range for the month
    from_date = datetime(year, month, 1)
    to_date = from_date + relativedelta(months=1) - timedelta(days=1)
    
    params = {
        "api_token": api_token,
        "categories": "business,politics,tech",
        "language": "en",
        "published_after": from_date.strftime("%Y-%m-%d"),
        "published_before": to_date.strftime("%Y-%m-%d"),
    }
    
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()
    
    return data.get("data", [])

def insert_news_articles(articles, db_name, max_items):
    """
    Insert up to max_items articles into the database.
    
    Args:
        articles (list): List of article dictionaries.
        db_name (str): Database file name.
        max_items (int): Maximum number of items to insert.
        
    Returns:
        int: Number of articles inserted.
    """
    if not articles:
        return 0
        
    # Limit to max_items
    articles_to_insert = articles[:max_items]
    
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    inserted = 0
    for article in articles_to_insert:
        uuid = article["uuid"]
        title = (article.get("title") or "").strip()
        pub_date = article.get("published_at")
        
        # Skip if missing critical data
        if not (uuid and title and pub_date):
            continue
            
        try:
            dt = datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
            year, month, day = dt.year, dt.month, dt.day
            
            # Insert article
            cur.execute("""
                INSERT OR IGNORE INTO articles
                (article_uuid, title, pub_date, year, month, day, description, snippet)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                uuid, 
                title, 
                pub_date, 
                year, 
                month, 
                day, 
                article.get("description", ""), 
                article.get("snippet", "")
            ))
            
            # Check if article was inserted (not a duplicate)
            if cur.rowcount > 0:
                # Get the article ID
                cur.execute("SELECT id FROM articles WHERE article_uuid = ?", (uuid,))
                article_id = cur.fetchone()[0]
                
                # Initialize the sentiment record
                cur.execute("INSERT OR IGNORE INTO sentiment (article_id) VALUES (?)", (article_id,))
                inserted += 1
                
        except Exception as e:
            pass
    
    conn.commit()
    conn.close()
    
    return inserted

def get_news_data(max_items, db_name, api_key_path):
    """
    Main function to fetch and store news articles.
    Manages progress tracking across runs to fetch 25 items at a time.
    
    Args:
        max_items (int): Maximum number of items to insert per run.
        db_name (str): Database file name.
        api_key_path (str): Path to file containing API key.
        
    Returns:
        int: Number of new articles inserted.
    """
    create_news_tables(db_name)
    
    # Load progress
    progress = load_progress()
    year = progress["current_year"]
    month = progress["current_month"]
    
    # Keep track of total inserted this run
    total_inserted = 0
    
    # Continue fetching until we have max_items or can't fetch more
    while total_inserted < max_items:
        # Fetch articles for the current year/month
        articles = fetch_news_articles(year, month, api_key_path)
        
        # If no articles for this month, move to next month
        if not articles:
            month += 1
            if month > 12:
                month = 1
                year += 1
            progress["current_month"] = month
            progress["current_year"] = year
            progress["offset"] = 0
            save_progress(progress)
            continue
        
        # Calculate how many more we need
        remaining = max_items - total_inserted
        
        # Insert articles into the database
        inserted = insert_news_articles(articles, db_name, remaining)
        total_inserted += inserted
        
        # If we didn't get as many as we wanted, move to next month
        if inserted < remaining and inserted < len(articles):
            # Move to next month
            month += 1
            if month > 12:
                month = 1
                year += 1
            progress["current_month"] = month
            progress["current_year"] = year
            progress["offset"] = 0
        else:
            # Same month, just update offset
            progress["offset"] += inserted
        
        # Save progress
        save_progress(progress)
        
        # If we didn't insert anything and moved to a new month/year, 
        # but still haven't reached max_items, continue the loop
        if inserted == 0 and total_inserted < max_items:
            continue
        
        # If we got what we wanted or couldn't get more, break
        if total_inserted >= max_items or inserted == 0:
            break
    
    return total_inserted

def count_news_records(db_name):
    """
    Count the total number of news records.
    
    Args:
        db_name (str): Database file name.
        
    Returns:
        int: Total number of records.
    """
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT COUNT(*) FROM articles")
        count = cur.fetchone()[0]
    except sqlite3.OperationalError:
        # Table doesn't exist
        count = 0
        
    conn.close()
    return count

if __name__ == "__main__":
    # Example usage
    db_name = DB_NAME
    max_items = MAX_ITEMS_PER_RUN
    api_key_path = "api_key_thenewsapi.txt"
    
    created = create_news_tables(db_name)
    inserted = get_news_data(max_items, db_name, api_key_path)
    
    print(f"Inserted {inserted} new articles into the database.") 