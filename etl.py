from dotenv import load_dotenv
import os
import requests
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
import re
from dotenv import load_dotenv
from dagster import asset
from datetime import datetime
from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)


load_dotenv()
"""
1. Download CSV Files (Extract):
"""

# base_url = os.getenv("BASE_URL")
# local_dir = os.getenv("LOCAL_DIR")
# conn_string = os.getenv("DB_CONNECTION_STRING")


@asset
def download_csv_files():
    load_dotenv()
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



# usage
# base_url = ""
# local_dir = ""
# download_csv_files(base_url, local_dir)

  
"""
2. Load CSV into DataFrames (Transform):
"""

@asset(deps=[download_csv_files])
def load_csv_files_in_dataframe():
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
            file_path = os.path.join(local_dir,filename)

            try:
                df = pd.read_csv(file_path)
                # add the call of clean dataframe here.
                # Set the DataFrame name based on the filename (without extension)
                df.name = os.path.splitext(filename)[0]

                # Add a new column 'timestamp' with current timestamp
                df['timestamp'] = datetime.now()
                
                dataframes.append(df)
                print(f"Loaded the file {filename} into a DataFrame successfully")
                # print(df.head())
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

# usage
# connect_db()


# define a function to sanitize filenames to be valid for database tables names

def sanitize_table_name(filename):
    """
    Sanitize a filename to make it a valid table name.
    
    Args:
        filename (str): The filename to  be sanitized
    Returns:
        str: The sanitized filename
    """
    # Remove the file extension
    table_name = filename.replace('.csv','')
    table_name = re.sub(r'[^\w]', '_', table_name)

    # Ensure the table name starts with a letter or underscore
    if not table_name[0].isalpha() and table_name[0] != '_':
        table_name = '_' + table_name

    return table_name


# "Load": part of the ETL
@asset(deps=[load_csv_files_in_dataframe])
def create_tables_and_load_data():
    """
    Create tables matching the structure of the DataFrames and load data into them.

    Args:
        dataframes (list): List of DataFrames containing the data to be loaded into tables.
        engine (sqlalchemy.engine.Engine): SQLAlchemy Engine instance for connecting to the database.
    """
    dataframes = load_csv_files_in_dataframe()
    engine = connect_db()
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

# Usage
# local_dir = "csv_files"
# dataframes = load_csv_files_in_dataframe(local_dir)
# engine = connect_db()
# create_tables_and_load_data(dataframes, engine)

defs = Definitions(
    assets=[download_csv_files, load_csv_files_in_dataframe, create_tables_and_load_data],
    jobs=[
        define_asset_job(
            name= "etl_job",
            selection=[download_csv_files, load_csv_files_in_dataframe, create_tables_and_load_data]
        )
    ],
    schedules=[
        ScheduleDefinition(
            name="etl_schedule",
            job_name="etl_job",
            cron_schedule="@daily",
        )
    ]
)


if __name__=="__main__":

    download_csv_files(base_url, local_dir)
    dataframes = load_csv_files_in_dataframe(local_dir)
    engine = connect_db()
    create_tables_and_load_data(dataframes)













