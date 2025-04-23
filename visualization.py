"""
Visualization module for creating charts and graphs from stock and news data.
"""
import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

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
        db_name (str): Stock database file name.
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
        db_name (str): News database file name.
        
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

def visualize_correlation(stock_db, news_db, ticker):
    """
    Create visualization showing correlation between stock prices and news sentiment.
    
    Args:
        stock_db (str): Stock database file name.
        news_db (str): News database file name.
        ticker (str): Stock ticker symbol.
        
    Returns:
        str: Path to saved visualization file.
    """
    ensure_output_dir()
    
    # Get stock data
    stock_conn = sqlite3.connect(stock_db)
    table_name = f"{ticker}_weekly_adjusted"
    stock_query = f"""
        SELECT 
            substr(date, 1, 7) as month,
            AVG(close) as avg_close,
            AVG((close - open) / open) as avg_daily_return
        FROM {table_name}
        GROUP BY substr(date, 1, 7)
        ORDER BY month
    """
    stock_df = pd.read_sql_query(stock_query, stock_conn)
    stock_conn.close()
    
    # Get sentiment data
    news_conn = sqlite3.connect(news_db)
    news_query = """
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
    sentiment_df = pd.read_sql_query(news_query, news_conn)
    news_conn.close()
    
    # Merge dataframes
    merged_df = pd.merge(stock_df, sentiment_df, on='month', how='inner')
    
    if merged_df.empty:
        print("No overlapping data between stock prices and news sentiment.")
        return None
    
    # Calculate correlation
    correlation = merged_df['avg_sentiment'].corr(merged_df['avg_daily_return'])
    
    # Create visualization
    plt.figure(figsize=(12, 10))
    
    # Scatter plot
    plt.subplot(2, 1, 1)
    plt.scatter(merged_df['avg_sentiment'], merged_df['avg_daily_return'], 
                alpha=0.7, c=merged_df['article_count'], cmap='viridis')
    
    # Add regression line
    sns.regplot(x='avg_sentiment', y='avg_daily_return', data=merged_df, 
                scatter=False, ci=None, line_kws={"color": "red"})
    
    plt.title(f'Correlation between News Sentiment and {ticker} Stock Returns')
    plt.xlabel('Average Sentiment Score')
    plt.ylabel('Average Daily Return (%)')
    plt.colorbar(label='Number of Articles')
    plt.grid(True, alpha=0.3)
    plt.annotate(f'Correlation: {correlation:.2f}', 
                 xy=(0.05, 0.95), xycoords='axes fraction',
                 bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8))
    
    # Time series comparison
    plt.subplot(2, 1, 2)
    fig, ax1 = plt.subplots(figsize=(12, 5))
    
    # Convert month to datetime for better x-axis
    merged_df['date'] = pd.to_datetime(merged_df['month'] + '-01')
    
    # Plot stock returns
    color = 'tab:blue'
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Stock Returns', color=color)
    ax1.plot(merged_df['date'], merged_df['avg_daily_return'], color=color)
    ax1.tick_params(axis='y', labelcolor=color)
    
    # Create second y-axis for sentiment
    ax2 = ax1.twinx()
    color = 'tab:red'
    ax2.set_ylabel('Sentiment Score', color=color)
    ax2.plot(merged_df['date'], merged_df['avg_sentiment'], color=color)
    ax2.tick_params(axis='y', labelcolor=color)
    
    plt.title(f'{ticker} Stock Returns vs. News Sentiment Over Time')
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save visualization
    filename = f"{OUTPUT_DIR}/{ticker}_sentiment_correlation.png"
    plt.savefig(filename)
    plt.close()
    
    # Write correlation data to file
    data_filename = f"{OUTPUT_DIR}/{ticker}_sentiment_correlation.txt"
    with open(data_filename, 'w') as f:
        f.write(f"Correlation Analysis: {ticker} Stock Returns vs. News Sentiment\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Correlation coefficient: {correlation:.4f}\n\n")
        f.write("Monthly Data:\n")
        f.write(merged_df.to_string(index=False))
    
    return filename

def generate_all_visualizations(stock_db, news_db, ticker):
    """
    Generate all visualizations and return list of files created.
    
    Args:
        stock_db (str): Stock database file name.
        news_db (str): News database file name.
        ticker (str): Stock ticker symbol.
        
    Returns:
        list: List of visualization files created.
    """
    ensure_output_dir()
    
    files = []
    
    # Stock price visualization
    stock_file = visualize_stock_prices(stock_db, ticker)
    if stock_file:
        files.append(stock_file)
    
    # News sentiment visualization
    sentiment_file = visualize_news_sentiment(news_db)
    if sentiment_file:
        files.append(sentiment_file)
    
    # Correlation visualization
    correlation_file = visualize_correlation(stock_db, news_db, ticker)
    if correlation_file:
        files.append(correlation_file)
    
    return files

if __name__ == "__main__":
    # Example usage
    stock_db = 'stock_data.db'
    news_db = 'news_data.db'
    ticker = 'SPY'
    
    generated_files = generate_all_visualizations(stock_db, news_db, ticker)
    
    print("Generated visualization files:")
    for file in generated_files:
        print(f" - {file}") 