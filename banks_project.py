import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
exchange_rate_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"

# Importing the required libraries
def log_progress(message):
    """Logs the progress of the code to a file with a timestamp."""
    with open("code_log.txt", "a") as log_file:
        time_stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  
        log_file.write(f"{time_stamp} : {message}\n")
        print(message)


def extract():
    """Extracts the table under 'By market capitalization' from the webpage."""
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        tables = soup.find_all('table', class_='wikitable')  
        table = tables[0]

        df = pd.read_html(str(table))[0]
        print("Columns in the DataFrame:", df.columns)
        df.columns = df.columns.str.strip()

        if 'Bank name' in df.columns and 'Market cap (US$ billion)' in df.columns:
            df['Market cap (US$ billion)'] = df['Market cap (US$ billion)'].replace(r'\n', '', regex=True)
            df['Market cap (US$ billion)'] = pd.to_numeric(df['Market cap (US$ billion)'], errors='coerce')

            df = df[['Bank name', 'Market cap (US$ billion)']]
            df.columns = ['Name', 'MC_USD_Billion'] 
        else:
            print("Expected columns not found")

        log_progress("Data extraction complete. Initiating Transformation process.")
        return df
    except Exception as e:
        print("Error occurred during extraction:", e)
        return None

if __name__ == "__main__":
    df = extract()
    if df is not None:
        print(df) 

def transform(df):
    """Transform the extracted data by adding columns for different currencies."""
    try:
        exchange_rate_df = pd.read_csv(exchange_rate_url)

        exchange_rate = exchange_rate_df.set_index('Currency')['Rate'].to_dict()

        df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
        df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
        df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

        log_progress("Data transformation complete. Columns added for GBP, EUR, INR.")
        return df
    except Exception as e:
        print("Error occurred during transformation:", e)
        return None

if __name__ == "__main__":
    df = extract()
    if df is not None:
        df_transformed = transform(df)
        if df_transformed is not None:
            print(df_transformed)  

            print("Market cap of the 5th largest bank in EUR:", df_transformed['MC_EUR_Billion'][4])

def load_to_csv(df, csv_path):
    """Load the transformed DataFrame to a CSV file."""
    try:
        df.to_csv(csv_path, index=False)
        log_progress(f"Data successfully loaded to {csv_path}.")
        print(f"Data successfully loaded to {csv_path}.")
    except Exception as e:
        print("Error occurred during CSV load:", e)
        log_progress(f"Error occurred during CSV load: {e}")

if __name__ == "__main__":
    df = extract()
    if df is not None:
        df_transformed = transform(df)
        if df_transformed is not None:
            load_to_csv(df_transformed, csv_path)
            print(df_transformed) 
            print("Market cap of the 5th largest bank in EUR:", df_transformed['MC_EUR_Billion'][4])

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    cursor = sql_connection.cursor()
    try:
        cursor.execute(query_statement)
        results = cursor.fetchall()
        print(f"Executing Query: {query_statement}")
        if results:
            for row in results:
                print(row)
        else:
            print("No results returned.")
        sql_connection.commit()
    except Exception as e:
        print(f"Error occurred while executing query: {e}")
    finally:
        cursor.close()

def main():
    # Assuming df is your DataFrame that contains the data
    df = pd.DataFrame({
        'Rank': [1, 2, 3],
        'Name': ['Bank A', 'Bank B', 'Bank C'],
        'Market cap (US$ billion)': [100, 90, 80],
        'MC_GBP_Billion': [80, 70, 60],  # Example transformed columns
        'MC_EUR_Billion': [90, 80, 70],
        'MC_INR_Billion': [7000, 6000, 5000],
    })

    # Connect to SQLite database
    sql_connection = sqlite3.connect('Banks.db')

    # Load the data into the database
    load_to_db(df, sql_connection, 'Largest_banks')

    # Run the queries
    query1 = "SELECT * FROM Largest_banks"
    run_query(query1, sql_connection)

    query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
    run_query(query2, sql_connection)

    query3 = "SELECT Name from Largest_banks LIMIT 5"
    run_query(query3, sql_connection)

    # Close the connection
    sql_connection.close()

if __name__ == "__main__":
    main()
