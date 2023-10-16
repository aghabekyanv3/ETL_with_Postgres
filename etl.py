import pandas as pd
import json
import requests
from sqlalchemy import create_engine
import configparser
import logging
from logging_utils import CustomFormatter  

# logger instance
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# console handler and setting the level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# Set the custom formatter
ch.setFormatter(CustomFormatter())

# Add the handler to the logger
logger.addHandler(ch)

class ETL_module():
    def __init__(self, table_name, table_details, table_cols):
        self.table_name = table_name
        self.table_details = table_details
        self.table_cols = table_cols

    def get_data(self, url):
        """Function to extract data from an API, convert to a Pandas DataFrame"""
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for non-2xx responses
            data = json.loads(response.text)
            self.df = pd.json_normalize(data=data)
            return self.df
        except requests.RequestException as e:
            logger.error(f"API request error: {str(e)}")
            return None

    def commit_postgres(self, username, password, host, port, db):
        """Saves the DataFrame to a PostgreSQL database"""
        try:
            engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}')
            logger.info("Logged credentials into PostgreSQL")
            return engine
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            return None

    def create_table(self, engine):
        """Creates table in PostgreSQL"""
        ct_query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({self.table_details})"
        try:
            cnxn = engine.raw_connection()
            cursor = cnxn.cursor()
            cursor.execute(ct_query)
            cnxn.commit()
            cursor.close()
            cnxn.close()
            logger.info(f"Table {self.table_name} created")
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}")

    def insert_row(self, engine, values):
        """Inserts row of values into table"""
        ir_query = f"INSERT INTO {self.table_name} ({self.table_cols}) VALUES {values}"
        try:
            cnxn = engine.raw_connection()
            cursor = cnxn.cursor()
            cursor.execute(ir_query)
            cnxn.commit()
            cursor.close()
            cnxn.close()
            logger.info(f"Values {values} inserted into table {self.table_name}")
        except Exception as e:
            logger.error(f"Error inserting values into table: {str(e)}")

    def truncate_table(self):
        """Truncates table, removes all rows"""
        tr_query = f"TRUNCATE TABLE {self.table_name};"
        logger.info(f"{self.table_name} truncated.")

    def delete_rows_with_condition(self, condition):
        """removes specific wors from table based on specific condition"""
        dft_query = f"DELETE FROM {self.table_name} WHERE {condition}"
        logger.info(f"deleted rows from {self.table_name} where {condition}")
    

if __name__ == "__main__":
    # Configuration and parameters
    api_url = "YOUR_API_URL_HERE"
    db_config = configparser.ConfigParser()
    db_config.read('postgres_db_credentials.txt')
    table_name = "your_table_name"
    table_details = "your_table_details"
    table_cols = "your_table_cols"

    # Initialize ETL module
    etl = ETL_module(table_name, table_details, table_cols)

    # Extract data
    df = etl.get_data(api_url)

    if df is not None:
        # Commit to PostgreSQL
        username = db_config.get('Credentials', 'username')
        password = db_config.get('Credentials', 'password')
        host = db_config.get('Credentials', 'host')
        port = db_config.get('Credentials', 'port')
        db = db_config.get('Credentials', 'db')
        engine = etl.commit_postgres(username, password, host, port, db)

        if engine is not None:
            # Create the table
            etl.create_table(engine)

            # Insert rows
            values = "your_values_here"
            etl.insert_row(engine, values)
