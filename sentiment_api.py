"""
Sentiment analysis API module for analyzing news article sentiment.
Uses Google Cloud Natural Language API to analyze sentiment.
"""
import os
import json
import sqlite3
import google.auth
from google.cloud import language_v2

# Database information
DB_NAME = 'news_data.db'
PROGRESS_FILE = 'sentiment_progress.json'
MAX_ITEMS_PER_RUN = 25  # Maximum items to process per run

def load_progress():
    """
    Load sentiment analysis progress from JSON file.
    
    Returns:
        dict: Progress state dictionary.
    """
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {"last_processed_id": 0}

def save_progress(state):
    """
    Save sentiment analysis progress to JSON file.
    
    Args:
        state (dict): Progress state dictionary.
    """
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(state, f)

def build_text_for_analysis(title, description, snippet):
    """
    Build full text for sentiment analysis from article parts.
    
    Args:
        title (str): Article title.
        description (str): Article description.
        snippet (str): Article snippet.
        
    Returns:
        str: Combined text for analysis.
    """
    parts = []
    if title and title.strip():
        parts.append(title.strip())
    if description and description.strip():
        parts.append(description.strip())
    if snippet and snippet.strip():
        parts.append(snippet.strip())
    
    # Join with double newlines
    return "\n\n".join(parts)

def analyze_sentiment(text):
    """
    Analyze sentiment of text using Google Cloud Natural Language API.
    
    Args:
        text (str): Text to analyze.
        
    Returns:
        tuple: (score, magnitude) sentiment values.
        
    Note:
        Requires Google Cloud credentials set up in environment.
    """
    try:
        # Initialize the client
        client = language_v2.LanguageServiceClient()
        
        # Prepare the document
        document = language_v2.Document(
            content=text,
            type_=language_v2.Document.Type.PLAIN_TEXT,
            language_code="en"
        )
        
        # Analyze sentiment
        response = client.analyze_sentiment(
            request={"document": document, "encoding_type": language_v2.EncodingType.UTF8}
        )
        
        # Get document sentiment
        sentiment = response.document_sentiment
        return sentiment.score, sentiment.magnitude
        
    except Exception as e:
        print(f"Error analyzing sentiment: {e}")
        return 0.0, 0.0  # Neutral sentiment on failure

def process_sentiment(db_name, max_items):
    """
    Process sentiment for news articles without sentiment scores.
    
    Args:
        db_name (str): Database file name.
        max_items (int): Maximum number of items to process.
        
    Returns:
        int: Number of articles processed.
    """
    # Load progress
    progress = load_progress()
    last_id = progress.get("last_processed_id", 0)
    
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    # Find articles that need sentiment analysis
    cur.execute("""
        SELECT a.id, a.title, a.description, a.snippet 
        FROM articles a
        JOIN sentiment s ON a.id = s.article_id
        WHERE s.score IS NULL AND a.id > ?
        ORDER BY a.id
        LIMIT ?
    """, (last_id, max_items))
    
    articles = cur.fetchall()
    processed = 0
    
    for article in articles:
        article_id, title, description, snippet = article
        
        # Build text for analysis
        text = build_text_for_analysis(title, description, snippet)
        
        if not text:
            # Skip if no text to analyze
            continue
            
        try:
            # Analyze sentiment
            score, magnitude = analyze_sentiment(text)
            
            # Update sentiment in database
            cur.execute("""
                UPDATE sentiment 
                SET score = ?, magnitude = ?
                WHERE article_id = ?
            """, (score, magnitude, article_id))
            
            processed += 1
            
            # Update progress
            progress["last_processed_id"] = article_id
            
        except Exception as e:
            print(f"Error processing article {article_id}: {e}")
    
    conn.commit()
    conn.close()
    
    # Save progress
    save_progress(progress)
    
    return processed

def count_sentiment_records(db_name):
    """
    Count articles with sentiment analysis completed.
    
    Args:
        db_name (str): Database file name.
        
    Returns:
        tuple: (analyzed_count, total_count) counts.
    """
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    try:
        # Count total articles
        cur.execute("SELECT COUNT(*) FROM articles")
        total = cur.fetchone()[0]
        
        # Count articles with sentiment
        cur.execute("SELECT COUNT(*) FROM sentiment WHERE score IS NOT NULL")
        analyzed = cur.fetchone()[0]
        
    except sqlite3.OperationalError:
        # Tables don't exist
        total = 0
        analyzed = 0
        
    conn.close()
    return analyzed, total

if __name__ == "__main__":
    # Example usage
    db_name = DB_NAME
    max_items = MAX_ITEMS_PER_RUN
    
    processed = process_sentiment(db_name, max_items)
    analyzed, total = count_sentiment_records(db_name)
    
    print(f"Processed sentiment for {processed} articles.")
    print(f"Total: {analyzed}/{total} articles have sentiment analysis.") 