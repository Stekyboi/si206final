# SI 206 Final Project: Stock Market and News Sentiment Analysis

This project analyzes the relationship between news sentiment and stock market performance. It collects stock market data and news articles, performs sentiment analysis on the news articles, and visualizes the correlations between the two.

## Project Structure

- `main.py`: Main entry point that orchestrates all operations
- `api_keys.py`: Central place for API key management
- `stock_api.py`: Functions for fetching and storing stock data
- `news_api.py`: Functions for fetching and storing news articles
- `sentiment_api.py`: Functions for analyzing sentiment of news articles
- `calculations.py`: Functions for calculating statistics and correlations
- `visualization.py`: Functions for visualizing the data

## Requirements

- Python 3.8 or higher
- Required Python packages:
  - requests
  - sqlite3
  - pandas
  - matplotlib
  - seaborn
  - dateutil
  - google-cloud-language (for sentiment analysis)

## API Keys

You need to set up the following API keys in separate files:
- Alpha Vantage API key: Store in `api_key.txt`
- TheNewsAPI key: Store in `api_key_thenewsapi.txt`

For Google Cloud Natural Language API, you need to set up Google Cloud credentials.

## Running the Project

1. Make sure all API keys are properly configured
2. Run the main script:
   ```
   python main.py
   ```
3. The script will process the next batch of data (25 items from each API)
4. Run the script multiple times (at least 4) to collect enough data (100+ items per API)
5. Once sufficient data is collected, the script will automatically generate calculations and visualizations

## Project Requirements Met

This project satisfies the following requirements:

1. Data collection from multiple APIs:
   - Alpha Vantage API for stock market data
   - TheNewsAPI for news articles
   - Google Cloud Natural Language API for sentiment analysis

2. Database storage:
   - Stock data in a SQLite database with tables for different tickers
   - News articles in a SQLite database with tables for articles and sentiment
   - Proper foreign key relationships between tables

3. Data processing:
   - Limiting data storage to 25 items per run
   - Calculation of statistics from the data
   - Database join to correlate stock data with news sentiment

4. Visualization:
   - Stock price and volume charts
   - News sentiment over time
   - Correlation between stock returns and news sentiment

5. Documentation:
   - Comprehensive docstrings for all functions
   - README.md with project overview and instructions
   - Inline comments for complex operations

## Output Files

All output files will be stored in the `output` directory:
- Text files with calculated statistics and correlations
- Visualization image files (PNG format)

## Contributors

[Your Name(s)]

## License

This project is for educational purposes as part of SI 206 at the University of Michigan. 