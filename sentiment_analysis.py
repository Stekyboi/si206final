import os
import json
import sqlite3
from google.cloud import language_v2

def load_progress():
    prog_file = "sentiment_progress.json"
    if os.path.exists(prog_file):
        return json.load(open(prog_file))
    return {"phase": "initial", "run": 1}

def save_progress(state):
    json.dump(state, open("sentiment_progress.json", "w"))

# Constants
DB_PATH         = "news_articles.db"
MAX_PER_INSERT  = 25

# Initialize Google NLP client once
client = language_v2.LanguageServiceClient()

# Build full text for sentiment
def build_full_text(article_row):
    # article_row: (id, title, description, snippet)
    _, title, desc, snip = article_row
    parts = [title]
    if desc:
        parts.append(desc)
    if snip:
        parts.append(snip)
    return "\n\n".join(parts)

# Analyze sentiment and return (score, magnitude)
def analyze_sentiment(text: str):
    doc = language_v2.Document(
        content=text,
        type_=language_v2.Document.Type.PLAIN_TEXT,
        language_code="en"
    )
    resp = client.analyze_sentiment(
        request={"document": doc, "encoding_type": language_v2.EncodingType.UTF8}
    )
    sent = resp.document_sentiment
    return sent.score, sent.magnitude

# Main processing
def main():
    prog = load_progress()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Select articles
    cur.execute("""
        SELECT a.id, a.title, a.description, a.snippet
        FROM articles a
        JOIN sentiment s ON a.id = s.article_id
    """)
    todo = cur.fetchall()

    # INITIAL PHASE: first 4 runs, up to 25 articles each
    if prog['phase'] == 'initial':
        batch = todo[(prog['run']-1)*MAX_PER_INSERT : prog['run']*MAX_PER_INSERT]
        print(f"Initial sentiment run #{prog['run']} processing {len(batch)} articles")
        for row in batch:
            art_id = row[0]
            text = build_full_text(row)
            try:
                score, mag = analyze_sentiment(text)
                cur.execute("""
                    INSERT INTO sentiment(article_id, score, magnitude)
                    VALUES(?, ?, ?)
                    ON CONFLICT(article_id) DO UPDATE SET
                        score     = excluded.score,
                        magnitude = excluded.magnitude
                """, (art_id, score, mag))
            except Exception as e:
                print(f"Error on article {art_id}: {e}")
        conn.commit()
        prog['run'] += 1
        if prog['run'] > 4:
            prog = {'phase': 'final'}
        save_progress(prog)
        print("Completed initial sentiment batch.")
        return

    # FINAL PHASE: process all remaining at once
    if prog['phase'] == 'final':
        print(f"Final sentiment run processing {len(todo)} articles")
        for row in todo:
            art_id = row[0]
            text = build_full_text(row)
            try:
                score, mag = analyze_sentiment(text)
                cur.execute("""
                    INSERT INTO sentiment(article_id, score, magnitude)
                    VALUES(?, ?, ?)
                    ON CONFLICT(article_id) DO UPDATE SET
                        score     = excluded.score,
                        magnitude = excluded.magnitude
                """, (art_id, score, mag))
            except Exception as e:
                print(f"Error on article {art_id}: {e}")
        conn.commit()
        save_progress({'phase': 'done'})
        print("All sentiment analysis complete.")
        return

if __name__ == '__main__':
    main()