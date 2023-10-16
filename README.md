# ETL Module for Extracting and Storing Data

This Python script provides an ETL (Extract, Transform, Load) module for extracting data from an API, transforming it into a Pandas DataFrame, and storing it in a PostgreSQL database. It also supports creating tables and inserting rows into the database.

**Note: I am also going to add functions which extract data from Excel and CSV files, this is the initial stage of the repo.**

## Prerequisites

Before using this script, make sure you have the following prerequisites:

- Python installed on your system
- Required Python packages, which can be installed using `pip`

*The requirements are in the requirements.txt file.*

I used this [article](https://medium.com/@davidaryee360/building-an-etl-pipeline-with-python-and-postgresql-7fc92056f9a3) as reference, to learn how to use PostgreSQL through Python.


- A PostgreSQL database with the necessary credentials and access permissions
- Configuration file 'postgres_db_credentials.txt' containing database connection details (username, password, host, port, and database name)

## Usage

1. Open the script 'etl_module.py' in your code editor.

2. Update the script with the following information:
 - API URL: Replace `"YOUR_API_URL_HERE"` with the URL of the API you want to extract data from.
 - Table Name: Set the desired table name for your PostgreSQL database.
 - Table Details: Define the table structure in the format `"column_name datatype(length) constraint"`.
 - Table Columns: Specify the column names for insertion into the table.
 - Values: Provide the values to be inserted into the table.

3. Run the script:


The script will perform the following steps:
- Extract data from the API.
- Create a Pandas DataFrame.
- Commit the data to the PostgreSQL database.

4. The script will log detailed information, including any errors, using a custom logging formatter.

5. Ensure the database configuration file 'postgres_db_credentials.txt' is correctly set up with your PostgreSQL credentials.

## Custom Logging

This script uses custom logging with different log levels, including INFO and DEBUG, to provide informative log messages. Log output is formatted with different colors based on log levels for better readability.

## License

This script is provided under the [MIT License](LICENSE).

