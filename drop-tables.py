import os
import sqlalchemy
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv


load_dotenv()

conn_string = os.getenv("DB_CONNECTION_STRING")

def drop_all_tables(engine):

    with engine.connect() as connection:
       inspector = inspect(connection)
       table_names = inspector.get_table_names(schema='public')

        # Drop tables in reverse order
       for table_name in table_names:
           try:
             if '-' in table_name:
                quoted_name = f'"{table_name}"'
             else:
                quoted_name = table_name

             query = (f"DROP TABLE public.{quoted_name}")
             connection.execute(text(query))
             connection.commit()  # Execute the SQL statement directly
             print(f"Table '{table_name}' dropped successfully.")
           except Exception as e:
             print(f"Failed to drop table '{table_name}': {e}")
   
       remaining_tables = inspector.get_table_names(schema='public')
       if not remaining_tables:
        print("All tables dropped successfully.")
       else:
        print(f"Remaining tables: {remaining_tables}")
       
def list_schemas(engine):
   with engine.connect() as connection:
      query = text("SELECT nspname FROM pg_namespace")
      result = connection.execute(query)
      schemas = [row[0] for row in result]
      print("Schemas:")
      for schema in schemas:
         print(schema)

def list_tables_by_schema(engine):
    with engine.connect() as connection:
        # Get all schema names
        schema_query = text("SELECT schema_name FROM information_schema.schemata")
        schema_result = connection.execute(schema_query)
        schemas = [row[0] for row in schema_result]

        for schema in schemas:
            print(f"\nSchema: {schema}")
            # Get tables for each schema
            table_query = text(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'")
            table_result = connection.execute(table_query)
            tables = [row[0] for row in table_result]

            if not tables:
                print("  (No tables found)")  # Indicate no tables in the schema
            else:
                print("  Tables:")
                for table in tables:
                    print(f"    - {table}")


engine = create_engine(conn_string)
# list_tables_by_schema(engine)
# list_schemas(engine)
drop_all_tables(engine)
