# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 18:23:53 2023

@author: Joao Mota
"""

#### 

import pandas as pd

from unidecode import unidecode

import pyodbc

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



## SQL INSTANCE NAME
## Server=localhost\SQLEXPRESS;Database=master;Trusted_Connection=True;

# Establish a connection to SQL Server
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost\SQLEXPRESS;'
                      'Database=master;'
                      'Trusted_Connection=yes;')


cursor = conn.cursor()

# Check if the table exists using the INFORMATION_SCHEMA
table_name = 'finance_details'
schema_name = 'dbo'  

query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
cursor.execute(query)
exists = cursor.fetchone()[0]

if not exists:
    # Create the table if it doesn't exist using the SQL Server connection
    cursor.execute(f"CREATE TABLE {schema_name}.{table_name} ("
                   "row_number INT PRIMARY KEY,"
                   "ms VARCHAR(2),"
                   "ms_name_en VARCHAR(255),"
                   "cci VARCHAR(20),"
                   "programme_title_short VARCHAR(255),"
                   "status VARCHAR(20),"
                   "version DECIMAL(3,1),"
                   "last_adoption_date_ecms DATE,"
                   "priority_code INT,"
                   "priority_name VARCHAR(255),"
                   "policy_objective_short_name VARCHAR(255),"
                   "policy_objective_code INT,"
                   "policy_objective_name VARCHAR(255),"
                   "specific_objective_short_name VARCHAR(255),"
                   "specific_objective_code VARCHAR(10),"
                   "specific_objective_name VARCHAR(255),"
                   "dimension_type VARCHAR(255),"
                   "category_title_short VARCHAR(255),"
                   "category_code INT,"
                   "category_name VARCHAR(255),"
                   "fund VARCHAR(20),"
                   "category_of_region VARCHAR(20),"
                   "cofinancing_rate DECIMAL(18,9),"
                   "eu_amount DECIMAL(18,2),"
                   "total_amount DECIMAL(18,2),"
                   "climate_weighting DECIMAL(18,9),"
                   "eu_climate_amount DECIMAL(18,2),"
                   "total_climate_amount DECIMAL(18,2),"
                   "environmental_weighting DECIMAL(18,9),"
                   "eu_environmental_amount DECIMAL(18,2),"
                   "total_environmental_amount DECIMAL(18,2),"
                   "biodiversity_weighting DECIMAL(18,9),"
                   "eu_biodiversity_amount DECIMAL(18,2),"
                   "total_biodiversity_amount DECIMAL(18,2),"
                   "gender_weighting DECIMAL(18,9),"
                   "eu_gender_amount DECIMAL(18,2),"
                   "total_gender_amount DECIMAL(18,2),"
                   "clean_air_weighting DECIMAL(18,9),"
                   "eu_clean_air_amount DECIMAL(18,2),"
                   "total_clean_air_amount DECIMAL(18,2),"
                   "digital_weighting DECIMAL(18,9),"
                   "eu_amount_digital DECIMAL(18,2),"
                   "total_amount_digital DECIMAL(18,2),"
                   "sustainable_urban_development VARCHAR(20),"
                   "territorial_tool VARCHAR(255),"
                   "territory_type VARCHAR(255),"
                   "un_sdg_tracking INT,"
                   "jtf_themes VARCHAR(255),"
                   "priority_type_code INT,"
                   "priority_type_description VARCHAR(255)"
                   ")")

# Commit the changes
conn.commit()

# Close the connection temporarily
conn.close()



## now that the table already exists or was created I will check if the data is already there and needs to be updated
##or if the data is not present and needs to be inserted, minding that this data is updated regularly



# Re-establish the connection to SQL Server using SQLAlchemy engine
from sqlalchemy import create_engine
engine = create_engine('mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus('Driver={SQL Server};'
                                                                                   'Server=localhost\SQLEXPRESS;'
                                                                                   'Database=master;'
                                                                                   'Trusted_Connection=yes;'))

# Load the existing data from the SQL Server table into a DataFrame using SQLAlchemy engine
existing_data = pd.read_sql(f"SELECT * FROM {schema_name}.{table_name}", engine)

# Re-establish the connection to SQL Server for data insertion
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost\SQLEXPRESS;'
                      'Database=master;'
                      'Trusted_Connection=yes;')

# Create a column in your DataFrame to store row numbers
financeDetailsData['row_number'] = range(1, len(financeDetailsData) + 1)


# Iterate through the rows in your DataFrame
for _, row in financeDetailsData.iterrows():
    # Check if a row with the same row number exists in the SQL Server table
    matching_row = existing_data[existing_data['row_number'] == row['row_number']]
    
    if matching_row.empty:
        # Insert the row into the table using the SQL Server connection
        cursor = conn.cursor()
        
        # Specify the columns to insert and their corresponding values
        columns_to_insert = [
            'row_number', 'ms', 'ms_name_en', 'cci', 'programme_title_short',
            'status', 'version', 'last_adoption_date_ecms', 'priority_code',
            'priority_name', 'policy_objective_short_name', 'policy_objective_code',
            'policy_objective_name', 'specific_objective_short_name', 'specific_objective_code',
            'specific_objective_name', 'dimension_type', 'category_title_short', 'category_code',
            'category_name', 'fund', 'category_of_region', 'cofinancing_rate', 'eu_amount',
            'total_amount', 'climate_weighting', 'eu_climate_amount', 'total_climate_amount',
            'environmental_weighting', 'eu_environmental_amount', 'total_environmental_amount',
            'biodiversity_weighting', 'eu_biodiversity_amount', 'total_biodiversity_amount',
            'gender_weighting', 'eu_gender_amount', 'total_gender_amount', 'clean_air_weighting',
            'eu_clean_air_amount', 'total_clean_air_amount', 'digital_weighting',
            'eu_amount_digital', 'total_amount_digital', 'sustainable_urban_development',
            'territorial_tool', 'territory_type', 'un_sdg_tracking', 'jtf_themes',
            'priority_type_code', 'priority_type_description'
        ]
        
        # Create a comma-separated list of columns and placeholders for values
        columns_str = ', '.join(columns_to_insert)
        placeholders_str = ', '.join(['?'] * len(columns_to_insert))
        
        # Construct the SQL INSERT statement
        sql_insert = f"INSERT INTO {schema_name}.{table_name} ({columns_str}) VALUES ({placeholders_str})"
        
        # Create a tuple of values to be inserted
        values = tuple(row[col] for col in columns_to_insert)
        
        # Debugging: Print the problematic row and column
        print(f"Inserting row: {row['row_number']}")
        print(f"Problematic row data: {row}")
        
        try:
            # Execute the INSERT statement with the values
            cursor.execute(sql_insert, values)
            
            # Commit the transaction
            conn.commit()
        except Exception as e:
            print(f"Error inserting row: {e}")
    else:
        # Update the existing row
        matching_index = matching_row.index[0]
        existing_data.loc[matching_index] = row

# Commit the changes
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()



# Establish a connection to SQL Server using SQLAlchemy
engine = create_engine('mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus('Driver={SQL Server};'
                                                                                   'Server=localhost\SQLEXPRESS;'
                                                                                   'Database=YourDatabaseName;'
                                                                                   'Trusted_Connection=yes;')) 

# Use pandas to insert the data into the SQL Server table
financeDetailsData.to_sql(table_name, engine, if_exists='replace', index=False)

# Commit the changes
conn.commit()

# Close the connection
conn.close()


# Save the cleaned data
financeDetailsData.to_csv('cleaned_file.csv', index=False)
