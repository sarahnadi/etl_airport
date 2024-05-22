import os
import sqlalchemy
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv


load_dotenv()

conn_string = os.getenv("DB_CONNECTION_STRING")

def drop_all_tables(engine):

    with engine.connect() as connection:
       inspector = inspect(connection)
       table_names = inspector.get_table_names()

        # Drop tables in reverse order
       for table_name in reversed(table_names):
           try:
             if '-' in table_name:
                quoted_name = f'"{table_name}"'
             else:
                quoted_name = table_name

             query = (f"DROP TABLE {quoted_name}")
             connection.execute(text(query))  # Execute the SQL statement directly
             print(f"Table '{table_name}' dropped successfully.")
           except Exception as e:
             print(f"Failed to drop table '{table_name}': {e}")
       





    #    try:
    #     query = "DROP TABLE IF EXISTS public.*;"
    #     connection.execute(text(query))
    #    except Exception as e:
    #       print (f"Failed to drop tablesL: {e}")    


engine = create_engine(conn_string)
drop_all_tables(engine)
