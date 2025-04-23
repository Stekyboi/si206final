"""
Main module for SI 206 Final Project - Stock Market and News Sentiment Analysis.

This module coordinates all operations, including:
1. Fetching stock data
2. Fetching news data
3. Analyzing sentiment
4. Calculating statistics
5. Generating visualizations

Run this file to process the next batch of data (25 items per API).
"""
import os
import sys
import time
from datetime import datetime

# Import project modules
import stock_api
import news_api
import sentiment_api
import calculations
import visualization

# Default settings
STOCK_TICKER = "SPY"
UNIFIED_DB = "stock_and_news.db"  # Unified database
MAX_ITEMS_PER_RUN = 25  # Maximum items to process per run
STOCK_API_KEY_PATH = "api_key.txt"
NEWS_API_KEY_PATH = "api_key_thenewsapi.txt"

def create_required_directories():
    """Create output directory if it doesn't exist."""
    if not os.path.exists('output'):
        os.makedirs('output')

def check_api_keys():
    """Check if necessary API keys exist."""
    required_files = [
        (STOCK_API_KEY_PATH, 'Alpha Vantage API key'),
        (NEWS_API_KEY_PATH, 'The News API key')
    ]
    
    missing = []
    for file, description in required_files:
        if not os.path.exists(file) or os.path.getsize(file) == 0:
            missing.append(f"Missing {description} in '{file}'")
    
    return missing

def get_current_counts():
    """Get current counts of items in the database."""
    stock_count = stock_api.count_stock_records(STOCK_TICKER, UNIFIED_DB)
    news_count = news_api.count_news_records(UNIFIED_DB)
    sentiment_count, sentiment_total = sentiment_api.count_sentiment_records(UNIFIED_DB)
    
    return {
        'stock': stock_count,
        'news': news_count,
        'sentiment_analyzed': sentiment_count,
        'sentiment_total': sentiment_total
    }

def print_status(before_counts, after_counts):
    """
    Print status information.
    
    Args:
        before_counts (dict): Counts before processing.
        after_counts (dict): Counts after processing.
    """
    if before_counts and after_counts:
        # Print progress
        print("\n" + "="*60)
        print(f"PROGRESS REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        print(f"Stock data: {before_counts['stock']} → {after_counts['stock']} records (+{after_counts['stock'] - before_counts['stock']})")
        print(f"News articles: {before_counts['news']} → {after_counts['news']} articles (+{after_counts['news'] - before_counts['news']})")
        print(f"Sentiment analysis: {before_counts['sentiment_analyzed']}/{before_counts['sentiment_total']} → {after_counts['sentiment_analyzed']}/{after_counts['sentiment_total']} articles")
        print("="*60)
    else:
        # Print current status
        counts = get_current_counts()
        print("\n" + "="*60)
        print(f"CURRENT STATUS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        print(f"Stock data: {counts['stock']} records")
        print(f"News articles: {counts['news']} articles")
        print(f"Sentiment analysis: {counts['sentiment_analyzed']}/{counts['sentiment_total']} articles")
        print("="*60)

def process_data():
    """
    Main function to process the next batch of data.
    Returns True if processing was successful, False otherwise.
    """
    # Check API keys
    missing_keys = check_api_keys()
    if missing_keys:
        print("Error: Missing API keys")
        for message in missing_keys:
            print(f" - {message}")
        return False
    
    # Get counts before processing
    before_counts = get_current_counts()
    
    # Process stock data
    print("\nProcessing stock data...")
    try:
        stock_api.get_stock_data(
            ticker=STOCK_TICKER,
            max_items=MAX_ITEMS_PER_RUN,
            db_name=UNIFIED_DB,
            api_key_path=STOCK_API_KEY_PATH
        )
        print("Stock data processed successfully.")
    except Exception as e:
        print(f"Error processing stock data: {e}")
    
    # Process news data
    print("\nProcessing news data...")
    try:
        news_api.get_news_data(
            max_items=MAX_ITEMS_PER_RUN,
            db_name=UNIFIED_DB,
            api_key_path=NEWS_API_KEY_PATH
        )
        print("News data processed successfully.")
    except Exception as e:
        print(f"Error processing news data: {e}")
    
    # Process sentiment analysis
    print("\nProcessing sentiment analysis...")
    try:
        sentiment_api.process_sentiment(
            db_name=UNIFIED_DB,
            max_items=MAX_ITEMS_PER_RUN
        )
        print("Sentiment analysis processed successfully.")
    except Exception as e:
        print(f"Error processing sentiment analysis: {e}")
    
    # Get counts after processing
    after_counts = get_current_counts()
    
    # Print progress
    print_status(before_counts, after_counts)
    
    return True

def generate_results():
    """Generate calculations and visualizations from the data."""
    print("\nGenerating calculations...")
    calculation_files = calculations.run_all_calculations(
        db_name=UNIFIED_DB,
        ticker=STOCK_TICKER
    )
    
    print("\nGenerating visualizations...")
    visualization_files = visualization.generate_all_visualizations(
        db_name=UNIFIED_DB,
        ticker=STOCK_TICKER
    )
    
    print("\nResults generated successfully:")
    print("Calculation files:")
    for file in calculation_files:
        print(f" - {file}")
    
    print("\nVisualization files:")
    for file in visualization_files:
        print(f" - {file}")

def main():
    """Main function."""
    print("SI 206 Final Project - Stock Market and News Sentiment Analysis")
    print("="*60)
    
    create_required_directories()
    
    # Print current status
    print_status(None, None)
    
    # Process data
    if process_data():
        # Generate results if needed
        total_count = get_current_counts()
        if total_count['stock'] >= 100 and total_count['news'] >= 100:
            print("\nSufficient data collected. Generating results...")
            generate_results()
        else:
            print(f"\nData collection still in progress.")
            print(f"Stock data: {total_count['stock']}/100 records")
            print(f"News data: {total_count['news']}/100 articles")
            print("\nPlease run this script again to collect more data.")
    
    print("\nDone!")

if __name__ == "__main__":
    main() 