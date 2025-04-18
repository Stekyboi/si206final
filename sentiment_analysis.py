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
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE sentiment
               SET score     = ?,
                   magnitude = ?
             WHERE article_id = ?;
        """, (score, magnitude, article_id))
        conn.commit()

def main():
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        cur.execute("""
            SELECT a.id, a.title
            FROM   articles  AS a
            JOIN   sentiment AS s
              ON   a.id = s.article_id
        """)
        todo = cur.fetchall()

    # Process each article for sentiment analysis
    for pk, title in todo:
        try:
            score, mag = analyze_text_sentiment(title)
            update_sentiment_in_db(pk, score, mag)
        except Exception as err:
            print(f"[{pk}] {err}")

if __name__ == '__main__':
    main()