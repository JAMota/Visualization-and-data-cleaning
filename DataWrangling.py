# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 18:23:53 2023

@author: Joao Mota
"""

#### 

import pandas as pd

from unidecode import unidecode

from sqlalchemy import create_engine

import urllib.parse  # Import the urllib.parse module








financeDetailsData = pd.read_csv('hgyj-gyin.csv')
print(financeDetailsData)

## I will first start by utilizing the unicode library to replace all special 
##character in each language with its unicode counterpart



# Define a function to apply unidecode only on str objects
def apply_unidecode(val):
    return unidecode(val) if isinstance(val, str) else val

# Apply the function to the entire DataFrame
financeDetailsData = financeDetailsData.applymap(apply_unidecode)




# Save the cleaned data
financeDetailsData.to_csv('cleaned_file.csv', index=False)




# Define your database and server details
server_name = 'localhost\SQLEXPRESS'
database_name = 'master'  # Replace with your actual database name
trusted_connection = 'yes'  # Use 'yes' for trusted connection (Windows authentication)

# Construct the connection string
connection_string = f'Driver=SQL Server;Server={server_name};Database={database_name};Trusted_Connection={trusted_connection};'

# Establish a connection to SQL Server using SQLAlchemy
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}')



# Define your table name
table_name = 'finance_details'

# Use pandas to insert the data into the SQL Server table
financeDetailsData.to_sql(table_name, engine, if_exists='replace', index=False)



# Close the SQLAlchemy engine (optional)
engine.dispose()
