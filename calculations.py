"""
Simplified calculations module for analyzing stock and news data.
"""
import os
import sqlite3
import pandas as pd
import numpy as np

# Output directory for calculation results
OUTPUT_DIR = 'output'

def ensure_output_dir():
    """Create output directory if it doesn't exist."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def calculate_basic_stats(db_name, ticker_table):
    """
    Calculate basic statistics on stock data.
    
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
    
    # Calculate basic statistics
    stats = {
        'count': len(df),
        'close_mean': df['close'].mean(),
        'close_min': df['close'].min(),
        'close_max': df['close'].max(),
        'close_variance': df['close'].var(),
        'volume_mean': df['volume'].mean(),
        'volume_variance': df['volume'].var(),
        'date_min': df['date'].min(),
        'date_max': df['date'].max()
    }
    
    return stats

def calculate_sentiment_stats(db_name):
    """
    Calculate statistics on news sentiment data.
    
    Args:
        db_name (str): Database file name.
        
    Returns:
        dict: Dictionary with sentiment statistics.
    """
    # Connect to database
    conn = sqlite3.connect(db_name)
    
    # Query data
    query = """
        SELECT s.score, s.magnitude
        FROM articles a
        JOIN sentiment s ON a.id = s.article_id
        WHERE s.score IS NOT NULL
    """
    
    # Load data into pandas DataFrame
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        return None
    
    # Calculate basic statistics
    stats = {
        'count': len(df),
        'score_mean': df['score'].mean(),
        'score_min': df['score'].min(),
        'score_max': df['score'].max(),
        'score_variance': df['score'].var(),
        'magnitude_mean': df['magnitude'].mean(),
        'magnitude_min': df['magnitude'].min(),
        'magnitude_max': df['magnitude'].max(),
        'magnitude_variance': df['magnitude'].var()
    }
    
    # Add combined sentiment (score * magnitude)
    df['combined'] = df['score'] * df['magnitude']
    stats['combined_mean'] = df['combined'].mean()
    
    return stats

def calculate_correlation(db_name, ticker_table):
    """
    Calculate correlation between stock prices and sentiment.
    
    Args:
        db_name (str): Database file name.
        ticker_table (str): Stock ticker table name.
        
    Returns:
        dict: Dictionary with correlation data.
    """
    # Connect to database
    conn = sqlite3.connect(db_name)
    
    # Query stock data by month
    stock_query = f"""
        SELECT 
            substr(date, 1, 7) as month,
            AVG(close) as avg_price,
            AVG(volume) as avg_volume
        FROM {ticker_table}
        GROUP BY substr(date, 1, 7)
        ORDER BY month
    """
    stock_df = pd.read_sql_query(stock_query, conn)
    
    # Query sentiment data by month
    sentiment_query = """
        SELECT 
            a.year || '-' || CASE WHEN a.month < 10 THEN '0' || a.month ELSE a.month END as month,
            AVG(s.score) as avg_score,
            AVG(s.magnitude) as avg_magnitude,
            AVG(s.score * s.magnitude) as combined_sentiment,
            COUNT(*) as article_count
        FROM articles a
        JOIN sentiment s ON a.id = s.article_id
        WHERE s.score IS NOT NULL
        GROUP BY a.year, a.month
        ORDER BY a.year, a.month
    """
    sentiment_df = pd.read_sql_query(sentiment_query, conn)
    conn.close()
    
    # Merge the data
    merged_df = pd.merge(stock_df, sentiment_df, on='month', how='inner')
    
    if merged_df.empty:
        return None
    
    # Calculate correlations
    corr = {
        'price_score_corr': merged_df['avg_price'].corr(merged_df['avg_score']),
        'price_magnitude_corr': merged_df['avg_price'].corr(merged_df['avg_magnitude']),
        'price_combined_corr': merged_df['avg_price'].corr(merged_df['combined_sentiment']),
        'volume_score_corr': merged_df['avg_volume'].corr(merged_df['avg_score']),
        'volume_magnitude_corr': merged_df['avg_volume'].corr(merged_df['avg_magnitude']),
        'count': len(merged_df)
    }
    
    return corr

def run_all_calculations(db_name, ticker):
    """
    Run the most important calculations on stock and sentiment data.
    
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
    stock_stats = calculate_basic_stats(db_name, stock_table)
    if stock_stats:
        output_file = f"{OUTPUT_DIR}/stock_statistics.txt"
        with open(output_file, 'w') as f:
            f.write(f"Stock Statistics for {ticker}\n")
            f.write("-" * 40 + "\n")
            f.write(f"Total Records: {stock_stats['count']}\n")
            f.write(f"Date Range: {stock_stats['date_min']} to {stock_stats['date_max']}\n\n")
            f.write(f"Average Close Price: ${stock_stats['close_mean']:.2f}\n")
            f.write(f"Minimum Close Price: ${stock_stats['close_min']:.2f}\n")
            f.write(f"Maximum Close Price: ${stock_stats['close_max']:.2f}\n")
            f.write(f"Close Price Variance: {stock_stats['close_variance']:.2f}\n\n")
            f.write(f"Average Volume: {stock_stats['volume_mean']:.0f}\n")
            f.write(f"Volume Variance: {stock_stats['volume_variance']:.0f}\n")
        output_files.append(output_file)
    
    # 2. Calculate sentiment statistics
    sentiment_stats = calculate_sentiment_stats(db_name)
    if sentiment_stats:
        output_file = f"{OUTPUT_DIR}/sentiment_statistics.txt"
        with open(output_file, 'w') as f:
            f.write("Sentiment Statistics\n")
            f.write("-" * 40 + "\n")
            f.write(f"Total Articles Analyzed: {sentiment_stats['count']}\n\n")
            f.write(f"Average Sentiment Score: {sentiment_stats['score_mean']:.4f}\n")
            f.write(f"Minimum Score: {sentiment_stats['score_min']:.4f}\n")
            f.write(f"Maximum Score: {sentiment_stats['score_max']:.4f}\n")
            f.write(f"Score Variance: {sentiment_stats['score_variance']:.4f}\n\n")
            f.write(f"Average Sentiment Magnitude: {sentiment_stats['magnitude_mean']:.4f}\n")
            f.write(f"Magnitude Variance: {sentiment_stats['magnitude_variance']:.4f}\n\n")
            f.write(f"Average Combined Sentiment: {sentiment_stats['combined_mean']:.4f}\n")
        output_files.append(output_file)
    
    # 3. Calculate correlation between stock prices and sentiment
    correlation_data = calculate_correlation(db_name, stock_table)
    if correlation_data:
        output_file = f"{OUTPUT_DIR}/stock_sentiment_correlation.txt"
        with open(output_file, 'w') as f:
            f.write(f"Stock Price vs. Sentiment Correlation\n")
            f.write("-" * 40 + "\n")
            f.write(f"Number of data points: {correlation_data['count']}\n\n")
            f.write(f"Stock price vs sentiment score correlation: {correlation_data['price_score_corr']:.4f}\n")
            f.write(f"Stock price vs sentiment magnitude correlation: {correlation_data['price_magnitude_corr']:.4f}\n")
            f.write(f"Stock price vs combined sentiment correlation: {correlation_data['price_combined_corr']:.4f}\n\n")
            f.write(f"Volume vs sentiment score correlation: {correlation_data['volume_score_corr']:.4f}\n")
            f.write(f"Volume vs sentiment magnitude correlation: {correlation_data['volume_magnitude_corr']:.4f}\n")
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