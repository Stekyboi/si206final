"""
Calculations module for analyzing stock and news data.
This module performs calculations on data from both databases
and writes results to output files.
"""
import os
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

# Output directory for calculation results
OUTPUT_DIR = 'output'

def ensure_output_dir():
    """Create output directory if it doesn't exist."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def calculate_stock_statistics(db_name, ticker):
    """
    Calculate statistics on stock data.
    
    Args:
        db_name (str): Stock database file name.
        ticker (str): Stock ticker symbol.
        
    Returns:
        tuple: (DataFrame with statistics, output file path)
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
    
    # Calculate daily returns
    df['daily_return'] = (df['close'] - df['open']) / df['open'] * 100
    
    # Calculate weekly returns (from previous row)
    df['weekly_return'] = df['close'].pct_change() * 100
    
    # Calculate volatility (rolling 4-week standard deviation of returns)
    df['volatility'] = df['weekly_return'].rolling(window=4).std()
    
    # Calculate statistics by month
    df['month'] = df['date'].dt.strftime('%Y-%m')
    
    monthly_stats = df.groupby('month').agg({
        'close': ['mean', 'min', 'max'],
        'volume': ['mean', 'min', 'max', 'sum'],
        'daily_return': ['mean', 'min', 'max', 'std'],
        'weekly_return': ['mean', 'min', 'max', 'std'],
        'volatility': 'mean'
    }).reset_index()
    
    # Flatten the column hierarchy
    monthly_stats.columns = ['_'.join(col).strip('_') for col in monthly_stats.columns.values]
    
    # Write to file
    output_file = f"{OUTPUT_DIR}/{ticker}_statistics.txt"
    with open(output_file, 'w') as f:
        f.write(f"Stock Statistics: {ticker}\n")
        f.write("=" * 50 + "\n\n")
        
        # Overall statistics
        f.write("OVERALL STATISTICS\n")
        f.write("-" * 50 + "\n")
        f.write(f"Date Range: {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}\n")
        f.write(f"Number of Weeks: {len(df)}\n")
        f.write(f"Average Close Price: ${df['close'].mean():.2f}\n")
        f.write(f"Highest Close Price: ${df['close'].max():.2f} on {df.loc[df['close'].idxmax(), 'date'].strftime('%Y-%m-%d')}\n")
        f.write(f"Lowest Close Price: ${df['close'].min():.2f} on {df.loc[df['close'].idxmin(), 'date'].strftime('%Y-%m-%d')}\n")
        f.write(f"Average Daily Return: {df['daily_return'].mean():.2f}%\n")
        f.write(f"Average Weekly Return: {df['weekly_return'].mean():.2f}%\n")
        f.write(f"Average Volatility: {df['volatility'].mean():.2f}\n\n")
        
        # Monthly statistics
        f.write("MONTHLY STATISTICS\n")
        f.write("-" * 50 + "\n")
        f.write(monthly_stats.to_string(index=False))
    
    return monthly_stats, output_file

def calculate_sentiment_statistics(db_name):
    """
    Calculate statistics on news sentiment data.
    
    Args:
        db_name (str): News database file name.
        
    Returns:
        tuple: (DataFrame with statistics, output file path)
    """
    ensure_output_dir()
    
    # Connect to database
    conn = sqlite3.connect(db_name)
    
    # Query data
    query = """
        SELECT 
            a.id, a.title, a.pub_date, a.year, a.month, a.day,
            s.score, s.magnitude
        FROM articles a
        JOIN sentiment s ON a.id = s.article_id
        WHERE s.score IS NOT NULL
        ORDER BY a.pub_date
    """
    
    # Load data into pandas DataFrame
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("No sentiment data available.")
        return None, None
    
    # Create month field for grouping
    df['month_year'] = df.apply(lambda row: f"{row['year']}-{row['month']:02d}", axis=1)
    
    # Calculate statistics by month
    monthly_stats = df.groupby('month_year').agg({
        'id': 'count',
        'score': ['mean', 'min', 'max', 'std'],
        'magnitude': ['mean', 'min', 'max', 'std']
    }).reset_index()
    
    # Flatten the column hierarchy
    monthly_stats.columns = ['_'.join(col).strip('_') for col in monthly_stats.columns.values]
    
    # Rename count column
    monthly_stats.rename(columns={'id_count': 'article_count'}, inplace=True)
    
    # Write to file
    output_file = f"{OUTPUT_DIR}/sentiment_statistics.txt"
    with open(output_file, 'w') as f:
        f.write("News Sentiment Statistics\n")
        f.write("=" * 50 + "\n\n")
        
        # Overall statistics
        f.write("OVERALL SENTIMENT STATISTICS\n")
        f.write("-" * 50 + "\n")
        f.write(f"Total Articles: {len(df)}\n")
        f.write(f"Date Range: {min(df['pub_date'])} to {max(df['pub_date'])}\n")
        f.write(f"Average Sentiment Score: {df['score'].mean():.4f}\n")
        f.write(f"Average Sentiment Magnitude: {df['magnitude'].mean():.4f}\n")
        
        # Distribution of sentiment
        positive = df[df['score'] > 0.25].shape[0]
        negative = df[df['score'] < -0.25].shape[0]
        neutral = df[(df['score'] >= -0.25) & (df['score'] <= 0.25)].shape[0]
        
        f.write(f"Positive Articles (score > 0.25): {positive} ({positive/len(df)*100:.1f}%)\n")
        f.write(f"Neutral Articles (-0.25 <= score <= 0.25): {neutral} ({neutral/len(df)*100:.1f}%)\n")
        f.write(f"Negative Articles (score < -0.25): {negative} ({negative/len(df)*100:.1f}%)\n\n")
        
        # Monthly statistics
        f.write("MONTHLY SENTIMENT STATISTICS\n")
        f.write("-" * 50 + "\n")
        f.write(monthly_stats.to_string(index=False))
    
    return monthly_stats, output_file

def calculate_correlation(stock_db, news_db, ticker):
    """
    Calculate correlation between stock returns and news sentiment.
    
    Args:
        stock_db (str): Stock database file name.
        news_db (str): News database file name.
        ticker (str): Stock ticker symbol.
        
    Returns:
        tuple: (DataFrame with correlation data, output file path)
    """
    ensure_output_dir()
    
    # Get stock data by month
    stock_conn = sqlite3.connect(stock_db)
    table_name = f"{ticker}_weekly_adjusted"
    stock_query = f"""
        SELECT 
            substr(date, 1, 7) as month,
            AVG(close) as avg_close,
            AVG(high - low) as avg_range,
            AVG((close - open) / open) * 100 as avg_daily_return,
            AVG(volume) as avg_volume
        FROM {table_name}
        GROUP BY substr(date, 1, 7)
        ORDER BY month
    """
    stock_df = pd.read_sql_query(stock_query, stock_conn)
    stock_conn.close()
    
    # Get sentiment data by month
    news_conn = sqlite3.connect(news_db)
    news_query = """
        SELECT 
            a.year || '-' || CASE WHEN a.month < 10 THEN '0' || a.month ELSE a.month END as month,
            COUNT(*) as article_count,
            AVG(s.score) as avg_sentiment,
            AVG(s.magnitude) as avg_magnitude,
            SUM(CASE WHEN s.score > 0.25 THEN 1 ELSE 0 END) as positive_count,
            SUM(CASE WHEN s.score < -0.25 THEN 1 ELSE 0 END) as negative_count
        FROM articles a
        JOIN sentiment s ON a.id = s.article_id
        WHERE s.score IS NOT NULL
        GROUP BY a.year, a.month
        ORDER BY a.year, a.month
    """
    sentiment_df = pd.read_sql_query(news_query, news_conn)
    news_conn.close()
    
    # Calculate positive/negative ratio
    sentiment_df['pos_neg_ratio'] = sentiment_df['positive_count'] / sentiment_df['negative_count'].replace(0, 1)
    
    # Merge dataframes
    merged_df = pd.merge(stock_df, sentiment_df, on='month', how='inner')
    
    if merged_df.empty:
        print("No overlapping data between stock prices and news sentiment.")
        return None, None
    
    # Calculate correlations
    correlations = {
        'Sentiment vs. Daily Return': merged_df['avg_sentiment'].corr(merged_df['avg_daily_return']),
        'Sentiment vs. Close Price': merged_df['avg_sentiment'].corr(merged_df['avg_close']),
        'Sentiment vs. Trading Range': merged_df['avg_sentiment'].corr(merged_df['avg_range']),
        'Sentiment vs. Volume': merged_df['avg_sentiment'].corr(merged_df['avg_volume']),
        'Magnitude vs. Trading Range': merged_df['avg_magnitude'].corr(merged_df['avg_range']),
        'Positive/Negative Ratio vs. Daily Return': merged_df['pos_neg_ratio'].corr(merged_df['avg_daily_return'])
    }
    
    # Create correlation dataframe
    corr_df = pd.DataFrame(list(correlations.items()), columns=['Correlation Pair', 'Coefficient'])
    
    # Write to file
    output_file = f"{OUTPUT_DIR}/{ticker}_sentiment_correlation_analysis.txt"
    with open(output_file, 'w') as f:
        f.write(f"Correlation Analysis: {ticker} Stock vs. News Sentiment\n")
        f.write("=" * 60 + "\n\n")
        
        f.write("CORRELATION COEFFICIENTS\n")
        f.write("-" * 60 + "\n")
        for pair, coef in correlations.items():
            f.write(f"{pair}: {coef:.4f}\n")
        
        f.write("\n\nMONTHLY DATA USED FOR CORRELATION\n")
        f.write("-" * 60 + "\n")
        f.write(merged_df.to_string(index=False))
    
    return merged_df, output_file

def run_all_calculations(stock_db, news_db, ticker):
    """
    Run all calculations and return list of output files.
    
    Args:
        stock_db (str): Stock database file name.
        news_db (str): News database file name.
        ticker (str): Stock ticker symbol.
        
    Returns:
        list: List of output files created.
    """
    ensure_output_dir()
    
    output_files = []
    
    # Stock statistics
    _, stock_file = calculate_stock_statistics(stock_db, ticker)
    if stock_file:
        output_files.append(stock_file)
    
    # Sentiment statistics
    _, sentiment_file = calculate_sentiment_statistics(news_db)
    if sentiment_file:
        output_files.append(sentiment_file)
    
    # Correlation analysis
    _, correlation_file = calculate_correlation(stock_db, news_db, ticker)
    if correlation_file:
        output_files.append(correlation_file)
    
    return output_files

if __name__ == "__main__":
    # Example usage
    stock_db = 'stock_data.db'
    news_db = 'news_data.db'
    ticker = 'SPY'
    
    output_files = run_all_calculations(stock_db, news_db, ticker)
    
    print("Generated calculation files:")
    for file in output_files:
        print(f" - {file}") 