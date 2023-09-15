# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 21:57:21 2023

@author: AndreMota
"""


import pandas as pd

from unidecode import unidecode

import pyodbc

from sqlalchemy import create_engine

import urllib.parse  # Import the urllib.parse module


# Define your database and server details
server_name = 'localhost\SQLEXPRESS'
database_name = 'master'  # Replace with your actual database name
trusted_connection = 'yes'  # Use 'yes' for trusted connection (Windows authentication)

# Construct the connection string
connection_string = f'Driver=SQL Server;Server={server_name};Database={database_name};Trusted_Connection={trusted_connection};'

# Establish a connection to SQL Server using SQLAlchemy
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}')

# Load CSV data into a DataFrame
csv_file = 'cleaned_file.csv'
df = pd.read_csv(csv_file)

# Define your table name
table_name = 'finance_details'

# Use pandas to insert the data into the SQL Server table
df.to_sql(table_name, engine, if_exists='replace', index=False)



# Execute a SELECT query to retrieve some sample records
query = f'SELECT TOP 10 * FROM {table_name}'  # Adjust the number of records as needed

# Execute the query and fetch the results into a DataFrame
sample_data = pd.read_sql(query, engine)

# Display the sample data
print(sample_data)


# Close the SQLAlchemy engine (optional)
engine.dispose()
