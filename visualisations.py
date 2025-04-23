"""
Visualisation module for creating charts and graphs from stock and news data.
"""
import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Output directory for visualisations and data
OUTPUT_DIR = 'output'

def ensure_output_dir():
    """Create output directory if it doesn't exist."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def visualise_stock_price(db_name, ticker):
    """
    Create a simple line chart of stock price over time.
    
    Args:
        db_name (str): Database file name.
        ticker (str): Stock ticker symbol.
        
    Returns:
        str: Path to saved visualisation file.
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
    
    # Create visualisation
    plt.figure(figsize=(10, 5))
    plt.plot(df['date'], df['close'])
    plt.title(f'{ticker} Stock Close Price Over Time')
    plt.xlabel('Date')
    plt.ylabel('Close Price ($)')
    plt.grid(True, alpha=0.3)
    
    # Format the x-axis to show fewer dates (to avoid overcrowding)
    plt.xticks(rotation=45)
    
    # Ensure layout fits well
    plt.tight_layout()
    
    # Save visualisation
    filename = f"{OUTPUT_DIR}/{ticker}_stock_price.png"
    plt.savefig(filename)
    plt.close()
    
    return filename

def visualise_score_vs_stock(db_name, ticker):
    """
    Create a comparison between sentiment scores and stock prices.
    
    Args:
        db_name (str): Database file name.
        ticker (str): Stock ticker symbol.
        
    Returns:
        str: Path to saved visualisation file.
    """
    ensure_output_dir()
    
    # Connect to database
    conn = sqlite3.connect(db_name)
    
    # First, get the date range of sentiment data
    date_query = """
        SELECT MIN(a.year) as min_year, MAX(a.year) as max_year
        FROM articles a
        JOIN sentiment s ON a.id = s.article_id
        WHERE s.score IS NOT NULL
    """
    date_range = pd.read_sql_query(date_query, conn)
    
    if date_range.empty or pd.isna(date_range['min_year'].iloc[0]):
        print("No sentiment data available.")
        conn.close()
        return None
    
    min_year = int(date_range['min_year'].iloc[0])
    max_year = int(date_range['max_year'].iloc[0])
    
    # Get stock data for the relevant years 
    stock_query = f"""
        SELECT 
            date,
            close
        FROM {ticker}_weekly_adjusted
        WHERE date >= '{min_year}-01-01' AND date <= '{max_year}-12-31'
        ORDER BY date
    """
    stock_df = pd.read_sql_query(stock_query, conn)
    
    # Get sentiment data
    sentiment_query = """
        SELECT 
            a.year,
            a.month,
            AVG(s.score) as avg_score
        FROM articles a
        JOIN sentiment s ON a.id = s.article_id
        WHERE s.score IS NOT NULL
        GROUP BY a.year, a.month
        ORDER BY a.year, a.month
    """
    sentiment_df = pd.read_sql_query(sentiment_query, conn)
    conn.close()
    
    # Check if we have data
    if stock_df.empty or sentiment_df.empty:
        print("Not enough data for visualisation.")
        return None
    
    # Create a date column in the sentiment dataframe
    sentiment_df['date'] = pd.to_datetime(sentiment_df.apply(
        lambda row: f"{int(row['year'])}-{int(row['month']):02d}-15", axis=1
    ))
    
    # Convert stock dates to datetime
    stock_df['date'] = pd.to_datetime(stock_df['date'])
    
    # Create the line chart
    plt.figure(figsize=(10, 5))
    
    # Plot stock price
    plt.subplot(2, 1, 1)
    plt.plot(stock_df['date'], stock_df['close'], 'b-', label='Stock Price')
    plt.ylabel('Stock Price ($)')
    plt.title(f'{ticker} Stock Price and News Sentiment')
    plt.grid(True, alpha=0.3)
    plt.legend()
    
    # Plot sentiment score
    plt.subplot(2, 1, 2)
    plt.plot(sentiment_df['date'], sentiment_df['avg_score'], 'r-', label='Sentiment Score')
    plt.axhline(y=0, color='gray', linestyle='--', alpha=0.7)
    plt.ylabel('Sentiment Score')
    plt.xlabel('Date')
    plt.grid(True, alpha=0.3)
    plt.legend()
    
    # Ensure layout fits well
    plt.tight_layout()
    
    # Save visualisation
    filename = f"{OUTPUT_DIR}/{ticker}_price_vs_sentiment.png"
    plt.savefig(filename)
    plt.close()
    
    return filename

def generate_all_visualisations(db_name, ticker):
    """
    Generate all visualisations for the data.
    
    Args:
        db_name (str): Database file name.
        ticker (str): Stock ticker symbol.
        
    Returns:
        list: Paths to generated visualisations.
    """
    # Make sure output directory exists
    ensure_output_dir()
    
    # List to collect paths to visualisations
    visualisation_files = []
    
    # Generate the stock price line chart
    stock_price_file = visualise_stock_price(db_name, ticker)
    if stock_price_file:
        visualisation_files.append(stock_price_file)
    
    # Generate the stock price vs sentiment score comparison
    comparison_file = visualise_score_vs_stock(db_name, ticker)
    if comparison_file:
        visualisation_files.append(comparison_file)
    
    return visualisation_files

if __name__ == "__main__":
    # Example usage
    db_name = "stock_and_news.db"
    ticker = "SPY"
    
    visualisations = generate_all_visualisations(db_name, ticker)
    
    print("Visualisations generated:")
    for viz in visualisations:
        print(f" - {viz}") 