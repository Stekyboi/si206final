"""
Centralized management of API keys for the project.
All API key loading functions are located here.
"""

def get_alpha_vantage_key(filepath):
    """Read the Alpha Vantage API key from the specified file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        key = f.readline().strip()
        if not key:
            raise ValueError(f"API key file '{filepath}' is empty.")
        return key

def get_news_api_key(filepath):
    """Read TheNewsAPI key from the specified file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        key = f.readline().strip()
        if not key:
            raise ValueError(f"API key file '{filepath}' is empty.")
        return key

def get_marketstack_key(filepath):
    """Read the Marketstack API key from the specified file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        key = f.readline().strip()
        if not key:
            raise ValueError(f"API key file '{filepath}' is empty.")
        return key 