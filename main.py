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
import visualisations

# Default settings
STOCK_TICKERS = ["DIA", "SPY"]  # Process both DIA and SPY tickers
UNIFIED_DB = "stock_and_news.db"  # Unified database
MAX_ITEMS_PER_RUN = 25  # Maximum items to process per run
STOCK_API_KEY_PATH = "api_key_stocks.txt"
NEWS_API_KEY_PATH = "api_key_news.txt"

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
    stock_counts = {}
    for ticker in STOCK_TICKERS:
        stock_counts[ticker] = stock_api.count_stock_records(ticker, UNIFIED_DB)
    
    news_count = news_api.count_news_records(UNIFIED_DB)
    sentiment_count, sentiment_total = sentiment_api.count_sentiment_records(UNIFIED_DB)
    
    return {
        'stock': stock_counts,
        'news': news_count,
        'sentiment_analysed': sentiment_count,
        'sentiment_total': sentiment_total
    }

def print_status(before_counts, after_counts):
    """Print status information."""
    if before_counts and after_counts:
        print("\nPROGRESS:")
        for ticker in STOCK_TICKERS:
            print(f"Stock {ticker}: {before_counts['stock'][ticker]} -> {after_counts['stock'][ticker]} (+{after_counts['stock'][ticker] - before_counts['stock'][ticker]})")
        print(f"News {before_counts['news']} -> {after_counts['news']} (+{after_counts['news'] - before_counts['news']})")
        print(f"Sentiment {before_counts['sentiment_analysed']}/{before_counts['sentiment_total']} -> {after_counts['sentiment_analysed']}/{after_counts['sentiment_total']}")
        
        print("\nCurrent Progress:")
        for ticker in STOCK_TICKERS:
            print(f"Stock {ticker}: {after_counts['stock'][ticker]}")
        print(f"News: {after_counts['news']}")
        print(f"Sentiment: {after_counts['sentiment_analysed']}/{after_counts['sentiment_total']}")
    else:
        counts = get_current_counts()
        print("\nCURRENT:")
        for ticker in STOCK_TICKERS:
            print(f"Stock {ticker}: {counts['stock'][ticker]}")
        print(f"News: {counts['news']}")
        print(f"Sentiment: {counts['sentiment_analysed']}/{counts['sentiment_total']}")

def process_data():
    """Process the next batch of data."""
    # Check API keys
    missing_keys = check_api_keys()
    if missing_keys:
        print("Error: Missing API keys")
        for message in missing_keys:
            print(f" - {message}")
        return False
    
    # Get counts before processing
    before_counts = get_current_counts()
    
    # Process stock data for each ticker
    print("\nProcessing stock data...")
    for ticker in STOCK_TICKERS:
        try:
            stock_inserted = stock_api.get_stock_data(
                ticker=ticker,
                max_items=MAX_ITEMS_PER_RUN,
                db_name=UNIFIED_DB,
                api_key_path=STOCK_API_KEY_PATH
            )
            print(f"Stock {ticker}: +{stock_inserted} records")
        except Exception as e:
            print(f"Error processing stock data for {ticker}: {e}")
    
    # Process news data
    print("\nProcessing news data...")
    try:
        news_inserted = news_api.get_news_data(
            max_items=MAX_ITEMS_PER_RUN,
            db_name=UNIFIED_DB,
            api_key_path=NEWS_API_KEY_PATH
        )
        print(f"News: +{news_inserted} articles")
    except Exception as e:
        print(f"Error processing news data: {e}")
    
    # Process sentiment analysis
    print("\nProcessing sentiment analysis...")
    try:
        sentiment_processed = sentiment_api.process_sentiment(
            db_name=UNIFIED_DB,
            max_items=MAX_ITEMS_PER_RUN
        )
        print(f"Sentiment: +{sentiment_processed} articles")
    except Exception as e:
        print(f"Error processing sentiment analysis: {e}")
    
    # Get counts after processing
    after_counts = get_current_counts()
    
    # Print progress
    print_status(before_counts, after_counts)
    
    return True

def generate_results():
    """Generate calculations and visualizations from the data."""
    print("\nGenerating calculations and visualizations...")
    
    all_calculation_files = []
    all_visualisation_files = []
    
    # Generate results for each ticker
    for ticker in STOCK_TICKERS:
        print(f"Processing {ticker}...")
        calculation_files = calculations.run_all_calculations(
            db_name=UNIFIED_DB,
            ticker=ticker
        )
        all_calculation_files.extend(calculation_files)
        
        visualisation_files = visualisations.generate_all_visualisations(
            db_name=UNIFIED_DB,
            ticker=ticker
        )
        all_visualisation_files.extend(visualisation_files)
    
    print("Results generated:")
    print("Calculation files:")
    for file in all_calculation_files:
        print(f"- {file}")
    
    print("Visualization files:")
    for file in all_visualisation_files:
        print(f"- {file}")

def main():
    """Main function."""
    print("SI 206 Final Project")
    
    create_required_directories()
    print_status(None, None)
    
    # Process data
    if process_data():
        # Get the run count from any of the modules to determine if this is the fourth run
        # We'll use the sentiment module's run count
        import json
        if os.path.exists('sentiment_progress.json'):
            with open('sentiment_progress.json', 'r') as f:
                progress = json.load(f)
                run_count = progress.get("run_count", 0)
        else:
            run_count = 0
            
        # Generate results (on fourth run or later)
        if run_count >= 4:
            generate_results()
            print(f"\nData collected and visualizations generated successfully.")
        else:
            print(f"\nData collection in progress. Run again to collect more data.")
    
    print("Done!")

if __name__ == "__main__":
    main() 