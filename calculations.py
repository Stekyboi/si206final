"""
Simplified calculations module for analyzing stock and news data.
Adapted for first-year university level.
"""
import os
import sqlite3
import pandas as pd

# Output directory for calculation results
OUTPUT_DIR = 'output'

def ensure_output_dir():
    """Create output directory if it doesn't exist."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def calculate_stock_summary(db_name, ticker_table):
    """
    Calculate basic summary statistics on stock data.
    
    Args:
        db_name (str): Database file name.
        ticker_table (str): Stock ticker table name.
        
    Returns:
        dict: Dictionary with basic statistics.
    """
    # Connect to database
    conn = sqlite3.connect(db_name)
    
    # Query data
    query = f"""
        SELECT date, open, high, low, close, volume
        FROM {ticker_table}
        ORDER BY date
    """
    
    # Load data into pandas DataFrame
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        return None
    
    # Calculate basic statistics
    stats = {
        'count': len(df),
        'close_avg': df['close'].mean(),
        'close_min': df['close'].min(),
        'close_max': df['close'].max(),
        'volume_avg': df['volume'].mean(),
        'start_date': df['date'].min(),
        'end_date': df['date'].max()
    }
    
    return stats

def calculate_news_summary(db_name):
    """
    Calculate a simple summary of news sentiment data.
    
    Args:
        db_name (str): Database file name.
        
    Returns:
        dict: Dictionary with sentiment summary.
    """
    # Connect to database
    conn = sqlite3.connect(db_name)
    
    # Query sentiment data
    query = """
        SELECT s.score
        FROM articles a
        JOIN sentiment s ON a.id = s.article_id
        WHERE s.score IS NOT NULL
    """
    
    # Load data into pandas DataFrame
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        return None
    
    # Calculate simple summary statistics
    stats = {
        'count': len(df),
        'score_avg': df['score'].mean(),
        'score_min': df['score'].min(),
        'score_max': df['score'].max()
    }
    
    return stats

def run_all_calculations(db_name, ticker):
    """
    Run calculations on stock and sentiment data.
    
    Args:
        db_name (str): Database file name.
        ticker (str): Stock ticker symbol.
        
    Returns:
        list: List of output file paths.
    """
    ensure_output_dir()
    output_files = []
    stock_table = f"{ticker}_weekly_adjusted"
    
    # 1. Calculate basic stock statistics
    stock_stats = calculate_stock_summary(db_name, stock_table)
    if stock_stats:
        output_file = f"{OUTPUT_DIR}/{ticker}_stock_statistics.txt"
        with open(output_file, 'w') as f:
            f.write(f"Stock Summary for {ticker}\n")
            f.write("-" * 40 + "\n")
            f.write(f"Total Records: {stock_stats['count']}\n")
            f.write(f"Date Range: {stock_stats['start_date']} to {stock_stats['end_date']}\n\n")
            f.write(f"Average Close Price: ${stock_stats['close_avg']:.2f}\n")
            f.write(f"Minimum Close Price: ${stock_stats['close_min']:.2f}\n")
            f.write(f"Maximum Close Price: ${stock_stats['close_max']:.2f}\n\n")
            f.write(f"Average Volume: {stock_stats['volume_avg']:.0f}\n")
        output_files.append(output_file)
    
    # 2. Calculate sentiment statistics (only for one ticker to avoid duplication)
    if ticker == "SPY":  # Only calculate once for SPY
        sentiment_stats = calculate_news_summary(db_name)
        if sentiment_stats:
            output_file = f"{OUTPUT_DIR}/news_sentiment_summary.txt"
            with open(output_file, 'w') as f:
                f.write(f"News Sentiment Summary\n")
                f.write("-" * 40 + "\n")
                f.write(f"Total Articles Analysed: {sentiment_stats['count']}\n\n")
                f.write(f"Average Sentiment Score: {sentiment_stats['score_avg']:.4f}\n")
                f.write(f"Minimum Score: {sentiment_stats['score_min']:.4f}\n")
                f.write(f"Maximum Score: {sentiment_stats['score_max']:.4f}\n")
            output_files.append(output_file)
    
    return output_files

if __name__ == "__main__":
    # Example usage
    db_name = "stock_and_news.db"
    ticker = "SPY"
    
    output_files = run_all_calculations(db_name, ticker)
    print("Calculations complete. Output files:")
    for file in output_files:
        print(f"- {file}") 