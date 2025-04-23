"""
Visualization module for creating charts and graphs from stock and news data.
"""
import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

# Output directory for visualizations and data
OUTPUT_DIR = 'output'

def ensure_output_dir():
    """Create output directory if it doesn't exist."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def visualize_stock_price(db_name, ticker):
    """
    Create stock price visualization over time.
    
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
        SELECT date, close FROM {table_name}
        ORDER BY date
    """
    
    # Load data into pandas DataFrame
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    
    # Create visualization
    plt.figure(figsize=(10, 5))
    plt.plot(df.index, df['close'], label='Close Price')
    plt.title(f'{ticker} Close Price Over Time')
    plt.xlabel('Date')
    plt.ylabel('Close Price')
    plt.grid(True, alpha=0.3)
    plt.legend()
    
    # Save visualization
    filename = f"{OUTPUT_DIR}/{ticker}_stock_price.png"
    plt.savefig(filename)
    plt.close()
    
    return filename

def visualize_stock_sentiment_scatter(db_name, ticker):
    """
    Create scatter plot of stock price vs sentiment (score * magnitude).
    
    Args:
        db_name (str): Database file name.
        ticker (str): Stock ticker symbol.
        
    Returns:
        str: Path to saved visualization file.
    """
    ensure_output_dir()
    
    # Connect to database
    conn = sqlite3.connect(db_name)
    
    # Get stock data by month
    table_name = f"{ticker}_weekly_adjusted"
    stock_query = f"""
        SELECT 
            substr(date, 1, 7) as month,
            AVG(close) as avg_close
        FROM {table_name}
        GROUP BY substr(date, 1, 7)
        ORDER BY month
    """
    stock_df = pd.read_sql_query(stock_query, conn)
    
    # Get sentiment data by month
    sentiment_query = """
        SELECT 
            a.year || '-' || CASE WHEN a.month < 10 THEN '0' || a.month ELSE a.month END as month,
            AVG(s.score * s.magnitude) as sentiment_value,
            AVG(s.score) as avg_score,
            AVG(s.magnitude) as avg_magnitude,
            COUNT(*) as article_count
        FROM articles a
        JOIN sentiment s ON a.id = s.article_id
        WHERE s.score IS NOT NULL AND s.magnitude IS NOT NULL
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
    plt.figure(figsize=(10, 6))
    
    # Scatter plot with point size based on article count
    scatter = plt.scatter(merged_df['sentiment_value'], 
                          merged_df['avg_close'], 
                          alpha=0.7,
                          s=merged_df['article_count'] * 5,  # Size based on number of articles
                          c=merged_df['avg_score'],  # Color based on sentiment score
                          cmap='RdYlGn')
    
    # Add color bar
    cbar = plt.colorbar(scatter)
    cbar.set_label('Sentiment Score')
    
    # Add labels and title
    plt.title(f'{ticker} Stock Price vs News Sentiment')
    plt.xlabel('Sentiment Value (Score × Magnitude)')
    plt.ylabel('Average Stock Price')
    
    # Add regression line
    if len(merged_df) > 1:  # Need at least 2 points for regression
        m, b = np.polyfit(merged_df['sentiment_value'], merged_df['avg_close'], 1)
        plt.plot(merged_df['sentiment_value'], m*merged_df['sentiment_value'] + b, 
                 color='red', linestyle='--', label='Trend Line')
    
        # Add correlation coefficient
        correlation = merged_df['sentiment_value'].corr(merged_df['avg_close'])
        plt.annotate(f"Correlation: {correlation:.4f}", 
                     xy=(0.05, 0.95),
                     xycoords='axes fraction',
                     fontsize=10,
                     backgroundcolor='white',
                     bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8))
    
    plt.grid(True, alpha=0.3)
    plt.legend()
    
    # Save visualization
    filename = f"{OUTPUT_DIR}/{ticker}_sentiment_scatter.png"
    plt.savefig(filename)
    plt.close()
    
    return filename

def generate_all_visualizations(db_name, ticker):
    """
    Generate all visualizations for the data.
    
    Args:
        db_name (str): Database file name.
        ticker (str): Stock ticker symbol.
        
    Returns:
        list: Paths to generated visualizations.
    """
    # Make sure output directory exists
    if not os.path.exists('output'):
        os.makedirs('output')
    
    # List to collect paths to visualizations
    visualization_files = []
    
    # Generate the scatter plot
    scatter_file = visualize_stock_sentiment_scatter(db_name, ticker)
    if scatter_file:
        visualization_files.append(scatter_file)
    
    # Generate the time series plot
    time_series_file = visualize_stock_price(db_name, ticker)
    if time_series_file:
        visualization_files.append(time_series_file)
    
    return visualization_files

if __name__ == "__main__":
    # Example usage
    db_name = "stock_and_news.db"
    ticker = "SPY"
    
    visualizations = generate_all_visualizations(db_name, ticker)
    
    print("Visualizations generated:")
    for viz in visualizations:
        print(f" - {viz}") 