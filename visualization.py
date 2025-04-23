"""
Visualization module for creating charts and graphs from stock and news data.
"""
import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np

# Output directory for visualizations and data
OUTPUT_DIR = 'output'

def ensure_output_dir():
    """Create output directory if it doesn't exist."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def visualize_stock_prices(db_name, ticker):
    """
    Create stock price visualization.
    
    Args:
        db_name (str): Database file name.
        ticker (str): Stock ticker symbol.
        
    Returns:
        str: Path to saved visualization file.
    """
    ensure_output_dir()
    
    # Connect to database
    conn = sqlite3.connect(db_name)
    
    # Get table name
    table_name = f"{ticker}_weekly_adjusted"
    
    # Query data
    query = f"""
        SELECT date, open, high, low, close, adjusted_close, volume
        FROM {table_name}
        ORDER BY date
    """
    
    # Load data into pandas DataFrame
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    
    # Create visualization
    plt.figure(figsize=(12, 6))
    
    # Plot stock prices
    plt.subplot(2, 1, 1)
    plt.plot(df.index, df['close'], label='Close Price', color='blue')
    plt.title(f'{ticker} Stock Price')
    plt.ylabel('Price ($)')
    plt.grid(True, alpha=0.3)
    plt.legend()
    
    # Plot volume as a bar chart
    plt.subplot(2, 1, 2)
    plt.bar(df.index, df['volume'], color='green', alpha=0.7)
    plt.title(f'{ticker} Trading Volume')
    plt.ylabel('Volume')
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save visualization
    filename = f"{OUTPUT_DIR}/{ticker}_stock_price.png"
    plt.savefig(filename)
    plt.close()
    
    return filename

def visualize_news_sentiment(db_name):
    """
    Create news sentiment visualization.
    
    Args:
        db_name (str): Database file name.
        
    Returns:
        str: Path to saved visualization file.
    """
    ensure_output_dir()
    
    # Connect to database
    conn = sqlite3.connect(db_name)
    
    # Query data
    query = """
        SELECT a.year, a.month, AVG(s.score) as avg_score, AVG(s.magnitude) as avg_magnitude
        FROM articles a
        JOIN sentiment s ON a.id = s.article_id
        WHERE s.score IS NOT NULL
        GROUP BY a.year, a.month
        ORDER BY a.year, a.month
    """
    
    # Load data into pandas DataFrame
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("No sentiment data available.")
        return None
    
    # Create date column
    df['date'] = pd.to_datetime(df.apply(lambda row: f"{int(row['year'])}-{int(row['month'])}-01", axis=1))
    
    # Create visualization
    plt.figure(figsize=(12, 6))
    
    # Plot sentiment score
    plt.subplot(2, 1, 1)
    plt.plot(df['date'], df['avg_score'], marker='o', linestyle='-', color='blue')
    plt.axhline(y=0, color='r', linestyle='-', alpha=0.3)  # Neutral sentiment line
    plt.title('Average News Sentiment Score by Month')
    plt.ylabel('Sentiment Score\n(-1 to 1)')
    plt.grid(True, alpha=0.3)
    
    # Plot sentiment magnitude
    plt.subplot(2, 1, 2)
    plt.plot(df['date'], df['avg_magnitude'], marker='o', linestyle='-', color='purple')
    plt.title('Average News Sentiment Magnitude by Month')
    plt.ylabel('Magnitude')
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save visualization
    filename = f"{OUTPUT_DIR}/news_sentiment.png"
    plt.savefig(filename)
    plt.close()
    
    return filename

def visualize_correlation(db_name, ticker):
    """
    Create visualization showing correlation between stock prices and news sentiment.
    
    Args:
        db_name (str): Database file name.
        ticker (str): Stock ticker symbol.
        
    Returns:
        str: Path to saved visualization file.
    """
    ensure_output_dir()
    
    # Connect to database
    conn = sqlite3.connect(db_name)
    
    # Get stock data
    table_name = f"{ticker}_weekly_adjusted"
    stock_query = f"""
        SELECT 
            substr(date, 1, 7) as month,
            AVG(close) as avg_close,
            AVG((close - open) / open) * 100 as avg_daily_return
        FROM {table_name}
        GROUP BY substr(date, 1, 7)
        ORDER BY month
    """
    stock_df = pd.read_sql_query(stock_query, conn)
    
    # Get sentiment data
    sentiment_query = """
        SELECT 
            a.year || '-' || CASE WHEN a.month < 10 THEN '0' || a.month ELSE a.month END as month,
            AVG(s.score) as avg_sentiment,
            COUNT(*) as article_count
        FROM articles a
        JOIN sentiment s ON a.id = s.article_id
        WHERE s.score IS NOT NULL
        GROUP BY a.year, a.month
        ORDER BY a.year, a.month
    """
    sentiment_df = pd.read_sql_query(sentiment_query, conn)
    conn.close()
    
    # Merge dataframes
    merged_df = pd.merge(stock_df, sentiment_df, on='month', how='inner')
    
    if merged_df.empty:
        print("No overlapping data between stock prices and news sentiment.")
        return None
    
    # Create visualization
    plt.figure(figsize=(12, 10))
    
    # Plot 1: Stock price vs. sentiment
    plt.subplot(3, 1, 1)
    plt.plot(pd.to_datetime(merged_df['month'] + '-01'), merged_df['avg_close'], label='Avg. Close Price', color='blue')
    plt.title(f'{ticker} Price vs. News Sentiment')
    plt.ylabel('Price ($)')
    plt.legend(loc='upper left')
    
    plt.twinx()  # Create second y-axis
    plt.plot(pd.to_datetime(merged_df['month'] + '-01'), merged_df['avg_sentiment'], label='Sentiment Score', color='red', linestyle='--')
    plt.ylabel('Sentiment Score')
    plt.axhline(y=0, color='gray', linestyle='-', alpha=0.3)  # Neutral sentiment line
    plt.legend(loc='upper right')
    plt.grid(True, alpha=0.3)
    
    # Plot 2: Daily return vs. sentiment
    plt.subplot(3, 1, 2)
    plt.plot(pd.to_datetime(merged_df['month'] + '-01'), merged_df['avg_daily_return'], label='Avg. Daily Return', color='green')
    plt.title(f'{ticker} Returns vs. News Sentiment')
    plt.ylabel('Daily Return (%)')
    plt.legend(loc='upper left')
    
    plt.twinx()  # Create second y-axis
    plt.plot(pd.to_datetime(merged_df['month'] + '-01'), merged_df['avg_sentiment'], label='Sentiment Score', color='red', linestyle='--')
    plt.ylabel('Sentiment Score')
    plt.axhline(y=0, color='gray', linestyle='-', alpha=0.3)  # Neutral sentiment line
    plt.legend(loc='upper right')
    plt.grid(True, alpha=0.3)
    
    # Plot 3: Scatter plot of returns vs. sentiment
    plt.subplot(3, 1, 3)
    plt.scatter(merged_df['avg_sentiment'], merged_df['avg_daily_return'], alpha=0.7)
    plt.title('Correlation: News Sentiment vs. Stock Returns')
    plt.xlabel('Sentiment Score')
    plt.ylabel('Daily Return (%)')
    
    # Add regression line
    m, b = np.polyfit(merged_df['avg_sentiment'], merged_df['avg_daily_return'], 1)
    plt.plot(merged_df['avg_sentiment'], m*merged_df['avg_sentiment'] + b, color='red')
    
    # Add correlation coefficient
    correlation = merged_df['avg_sentiment'].corr(merged_df['avg_daily_return'])
    plt.annotate(f"Correlation: {correlation:.4f}", 
                 xy=(0.05, 0.95),
                 xycoords='axes fraction',
                 fontsize=10,
                 backgroundcolor='white',
                 bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8))
    
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save visualization
    filename = f"{OUTPUT_DIR}/{ticker}_correlation.png"
    plt.savefig(filename)
    plt.close()
    
    return filename

def visualize_sentiment_distribution(db_name):
    """
    Create visualization of sentiment distribution.
    
    Args:
        db_name (str): Database file name.
        
    Returns:
        str: Path to saved visualization file.
    """
    ensure_output_dir()
    
    # Connect to database
    conn = sqlite3.connect(db_name)
    
    # Query data
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
        print("No sentiment data available.")
        return None
    
    # Create visualization
    plt.figure(figsize=(10, 6))
    
    # Create histogram of sentiment scores
    sns.histplot(df['score'], bins=20, kde=True)
    plt.title('Distribution of News Sentiment Scores')
    plt.xlabel('Sentiment Score (-1 to 1)')
    plt.ylabel('Frequency')
    
    # Add vertical lines for sentiment categories
    plt.axvline(x=-0.25, color='r', linestyle='--', alpha=0.7, label='Negative Threshold')
    plt.axvline(x=0.25, color='g', linestyle='--', alpha=0.7, label='Positive Threshold')
    
    # Add counts and percentages
    total = len(df)
    positive = df[df['score'] > 0.25].shape[0]
    negative = df[df['score'] < -0.25].shape[0]
    neutral = total - positive - negative
    
    plt.annotate(f"Positive: {positive} ({positive/total*100:.1f}%)", 
                 xy=(0.7, 0.9),
                 xycoords='axes fraction',
                 fontsize=10,
                 backgroundcolor='white',
                 bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8))
                 
    plt.annotate(f"Neutral: {neutral} ({neutral/total*100:.1f}%)", 
                 xy=(0.7, 0.8),
                 xycoords='axes fraction',
                 fontsize=10,
                 backgroundcolor='white',
                 bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8))
                 
    plt.annotate(f"Negative: {negative} ({negative/total*100:.1f}%)", 
                 xy=(0.7, 0.7),
                 xycoords='axes fraction',
                 fontsize=10,
                 backgroundcolor='white',
                 bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8))
    
    plt.grid(True, alpha=0.3)
    plt.legend()
    
    # Save visualization
    filename = f"{OUTPUT_DIR}/sentiment_distribution.png"
    plt.savefig(filename)
    plt.close()
    
    return filename

def generate_all_visualizations(db_name, ticker):
    """
    Generate all visualizations and return paths to output files.
    
    Args:
        db_name (str): Database file name.
        ticker (str): Stock ticker symbol.
        
    Returns:
        list: Paths to visualization files.
    """
    ensure_output_dir()
    output_files = []
    
    # Stock price visualization
    print("Generating stock price visualization...")
    viz_file = visualize_stock_prices(db_name, ticker)
    if viz_file:
        output_files.append(viz_file)
    
    # News sentiment visualization
    print("Generating news sentiment visualization...")
    viz_file = visualize_news_sentiment(db_name)
    if viz_file:
        output_files.append(viz_file)
    
    # Correlation visualization
    print("Generating correlation visualization...")
    viz_file = visualize_correlation(db_name, ticker)
    if viz_file:
        output_files.append(viz_file)
    
    # Sentiment distribution visualization
    print("Generating sentiment distribution visualization...")
    viz_file = visualize_sentiment_distribution(db_name)
    if viz_file:
        output_files.append(viz_file)
    
    return output_files

if __name__ == "__main__":
    # Example usage
    db_name = "stock_and_news.db"
    ticker = "SPY"
    
    visualizations = generate_all_visualizations(db_name, ticker)
    
    print("Visualizations generated:")
    for viz in visualizations:
        print(f" - {viz}") 