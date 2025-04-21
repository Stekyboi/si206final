import matplotlib.pyplot as plt
import pandas as pd
import sqlite3

def visualize_data(db_name, table_name):
    

    conn = sqlite3.connect(db_name)
    query = f"SELECT date, close FROM {table_name}"
    df = pd.read_sql_query(query, conn)
    conn.close()

    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    plt.figure(figsize=(10, 5))
    plt.plot(df.index, df['close'], label='Close Price')
    plt.title(f'{table_name} Close Price Over Time')
    plt.xlabel('Date')
    plt.ylabel('Close Price')
    plt.legend()
    plt.show()

if __name__ == "__main__":

    db_test = 'test.db'
    table_test = 'SPY_weekly_adjusted'
    
    visualize_data(db_test, table_test)
    print(f"Data visualization for {table_test} from {db_test} completed successfully.")