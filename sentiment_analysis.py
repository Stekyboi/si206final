import sqlite3
from google.cloud import language_v2
import os

# Constants
DB_PATH = "news_articles.db"

def analyze_text_sentiment(text):
    """
    Uses Google Cloud Natural Language API to perform sentiment analysis on the provided text.
    Returns the sentiment score and magnitude.
    """
    client = language_v2.LanguageServiceClient()
    document_type_in_plain_text = language_v2.Document.Type.PLAIN_TEXT

    language_code = "en"
    document = {
        "content": text,
        "type_": document_type_in_plain_text,
        "language_code": language_code,
    }

    encoding_type = language_v2.EncodingType.UTF8

    document = language_v2.Document(content=text, type_=language_v2.Document.Type.PLAIN_TEXT)
    response = client.analyze_sentiment(
        request={"document": document, "encoding_type": encoding_type}
    )
    print(f"Document sentiment score: {response.document_sentiment.score}")
    print(f"Document sentiment magnitude: {response.document_sentiment.magnitude}")
    sentiment = response.document_sentiment
    return sentiment.score, sentiment.magnitude

def update_sentiment_in_db(article_id, score, magnitude):
    """
    Updates the sentiment analysis results for a specific article in the database.
    If a row for the article already exists in the sentiment table, it updates it;
    otherwise, it inserts a new row.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # Check if the sentiment data for this article already exists
    cur.execute("SELECT id FROM sentiment WHERE article_id = ?", (article_id,))
    row = cur.fetchone()
    if row:
        cur.execute("""
            UPDATE sentiment
            SET score = ?, magnitude = ?
            WHERE article_id = ?
        """, (score, magnitude, article_id))
    else:
        cur.execute("""
            INSERT INTO sentiment (article_id, score, magnitude)
            VALUES (?, ?, ?)
        """, (article_id, score, magnitude))
    conn.commit()
    conn.close()

def main():
    # Connect to the SQLite database
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Select articles that have no sentiment data yet.
    # (We use a LEFT JOIN to check if there is an existing row in the sentiment table.)
    cur.execute("""
        SELECT a.id, a.title
        FROM articles a
        LEFT JOIN sentiment s ON a.id = s.article_id
    """)
    articles = cur.fetchall()
    conn.close()

    if not articles:
        print("No new articles require sentiment analysis.")
        return

    # Process each article for sentiment analysis
    for article_id, title in articles:
        try:
            print(f"Analyzing sentiment for article {article_id} with title: {title}")

            score, magnitude = analyze_text_sentiment(title)

            # Update the database with the sentiment analysis results
            update_sentiment_in_db(article_id, score, magnitude)
            print(f"Updated article {article_id} sentiment: score={score}, magnitude={magnitude}")
        except Exception as e:
            print(f"Error processing article {article_id}: {e}")

if __name__ == '__main__':
    main()