import sqlite3
import sys

DB_PATH = "news_articles.db"

def migrate(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    # 1. Create new articles table with UNIQUE(title, pub_date)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS articles_new (
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


    # 2. Copy DISTINCT headlines in order of old id → keeps earliest on dup
    cur.execute("""
    INSERT OR IGNORE INTO articles_new
      (article_uuid, title, pub_date, year, month, day, language)
    SELECT article_uuid, title, pub_date, year, month, day, language
      FROM articles
     ORDER BY id;
    """)

    # 3. Create new sentiment table with article_id as PRIMARY KEY
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sentiment_new (
        article_id INTEGER PRIMARY KEY,
        sent_pos   REAL,
        sent_neg   REAL,
        sent_neu   REAL,
        score      REAL,    -- Google NLP
        magnitude  REAL,    -- Google NLP
        FOREIGN KEY(article_id) REFERENCES articles_new(id)
            ON DELETE CASCADE
    );
    """)

    print(3)

    # 4. Copy old sentiment into new, matching on article_uuid → new article_id
    cur.execute("""
    INSERT OR IGNORE INTO sentiment_new (article_id, sent_pos, sent_neg, sent_neu)
    SELECT n.id, s.sent_pos, s.sent_neg, s.sent_neu
      FROM sentiment  AS s
      JOIN articles  AS o   ON s.article_id = o.id
      JOIN articles_new AS n ON o.article_uuid = n.article_uuid
    ;
    """)

    # 5. Drop old tables and rename the news ones
    cur.execute("DROP TABLE sentiment;")
    cur.execute("DROP TABLE articles;")
    cur.execute("ALTER TABLE articles_new   RENAME TO articles;")
    cur.execute("ALTER TABLE sentiment_new  RENAME TO sentiment;")

    conn.commit()
    conn.close()
    print("Migration complete! 🎉  Your DB now has the new schema and no dup headlines.")

if __name__ == "__main__":
    try:
        migrate()
    except Exception as e:
        print("Error during migration:", e, file=sys.stderr)
        sys.exit(1)