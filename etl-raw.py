from dotenv import load_dotenv
import os
import requests
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
import re
from dotenv import load_dotenv




load_dotenv()
"""
1. Download CSV Files (Extract):
"""

base_url = os.getenv("BASE_URL")
local_dir = os.getenv("LOCAL_DIR")
conn_string = os.getenv("DB_CONNECTION_STRING")


def download_csv_files(base_url, local_dir):
   
    base_url = os.getenv("BASE_URL")
    local_dir = os.getenv("LOCAL_DIR")
    print(os.getenv("BASE_URL"))
    #  create the directory if it does not exist

    Path(local_dir).mkdir(parents=True, exist_ok=True)
    csv_filenames = ['airports.csv', 'countries.csv', 'navaids.csv', 'regions.csv',
                     'runways.csv', 'airport-frequencies.csv', 'airport-comments.csv']
    
    #  loop over all csv files
    for filename in csv_filenames:
        url = base_url + filename
        local_path = os.path.join(local_dir, filename)

        try:
            #  download the file
            response = requests.get(url)
            response.raise_for_status()
            with open(local_path, 'wb') as f:
                f.write(response.content)
            print(f"{filename} downloaded.")
        except requests.exceptions.HTTPError as e:
            print(f"Failed to download {filename}: {e}")


"""
2. Load CSV into DataFrames (Transform):
"""

import os
import pandas as pd
from datetime import datetime

def load_csv_files_in_dataframe(local_dir):
    """
    Args:
        local_dir (str): the local directory where csv folders are stored
    Returns:
        A list of DataFrames.
    """
    local_dir = os.getenv("LOCAL_DIR")
    dataframes = []

    for filename in os.listdir(local_dir):
        if filename.endswith(".csv"):
            file_path = os.path.join(local_dir, filename)

            try:
                df = pd.read_csv(file_path)
                # Set the DataFrame name based on the filename (without extension)
                df.name = os.path.splitext(filename)[0]
                
                # Add a new column 'timestamp' with current timestamp
                df['timestamp'] = datetime.now()
                
                dataframes.append(df)
                print(f"Loaded the file {filename} into a DataFrame successfully")
            except Exception as e:
                print(f"Failed to Load {filename} into a DataFrame: {e}")

    return dataframes



"""
3. Create Tables and Load Data (Load):
"""

def connect_db():
    """
    connect to the postgreSQL database using a connection string
    """
    conn_string = os.getenv("DB_CONNECTION_STRING")

    try:
        engine = create_engine(conn_string)
        print(f"Connected to the PostgreSQL database: {conn_string}")
        return engine
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        raise



def sanitize_table_name(filename):
    """
    Sanitize a filename to make it a valid table name.
    
    Args:
        filename (str): The filename to  be sanitized
    Returns:
        str: The sanitized filename
    """

    table_name = filename.replace('.csv','')
    table_name = re.sub(r'[^\w]', '_', table_name)

    # Ensure the table name starts with a letter or underscore
    if not table_name[0].isalpha() and table_name[0] != '_':
        table_name = '_' + table_name

    return table_name


# "Load": part of the ETL
def create_tables_and_load_data(dataframes, engine):
    """
    Create tables matching the structure of the DataFrames and load data into them.

    Args:
        dataframes (list): List of DataFrames containing the data to be loaded into tables.
        engine (sqlalchemy.engine.Engine): SQLAlchemy Engine instance for connecting to the database.
    """
    # Iterate over each DataFrame
    for df in dataframes:
        filename = df.name.iloc[0] if isinstance(df.name, pd.Series) else df.name  # Assuming DataFrame name matches table name
        table_name = sanitize_table_name(filename)
        # Create table in the database
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        # Load data into the table
        try:
            with engine.connect() as connection:
                df.to_sql(table_name, connection, if_exists='replace', index=False)
            print(f"Data loaded into table '{table_name}' successfully.")
        except Exception as e:
            print(f"Failed to load data into table '{table_name}': {e}")


if __name__=="__main__":

    # download_csv_files(base_url, local_dir)
    dataframes = load_csv_files_in_dataframe(local_dir)
    engine = connect_db()
    create_tables_and_load_data(dataframes, engine)


# python etl-raw.py











