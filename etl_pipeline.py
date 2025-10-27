import sqlite3
import pandas as pd
import requests
import json
from datetime import datetime

# --- CONFIGURATION ---
MART_DB_NAME = "restaurant_mart.db"     # ### MODIFIED ### Renamed for clarity
SOURCE_HR_DB_NAME = "source_hr.db"     # ### NEW ### Our new SQL source
TABLES_CSV = "tables.csv"
RESERVATIONS_JSON = "reservations_log.json"
CUSTOMER_API_URL = "https://randomuser.me/api/?results=5&seed=midterm"

def extract():
    """
    Extracts data from the 4 sources: CSV, JSON, API, and Source SQL DB.
    """
    print("Extracting data...")
    try:
        # 1. Extract Restaurant Tables from CSV
        tables_df = pd.read_csv(TABLES_CSV)
        
        # 2. Extract Reservations Log from JSON
        with open(RESERVATIONS_JSON, 'r') as f:
            reservations_data = json.load(f)
        reservations_df = pd.DataFrame(reservations_data)
        
        # 3. Extract Customer data from API
        response = requests.get(CUSTOMER_API_URL)
        response.raise_for_status()
        customer_api_data = response.json()['results']
        
        # ### NEW ###
        # 4. Extract Employee data from Source SQL Database
        conn_hr = sqlite3.connect(SOURCE_HR_DB_NAME)
        employees_df = pd.read_sql_query("SELECT * FROM employees", conn_hr)
        conn_hr.close()
        
        print("Extraction complete.")
        # ### MODIFIED ### Return the new dataframe
        return tables_df, reservations_df, customer_api_data, employees_df

    except Exception as e:
        print(f"Error during extraction: {e}")
        # ### MODIFIED ###
        return None, None, None, None

def transform(tables_df, reservations_df, customer_api_data, employees_df): # ### MODIFIED ###
    """
    Transforms the raw data into our Star Schema DataFrames.
    """
    # ### MODIFIED ### Check for all 4 sources
    if any(df is None for df in [tables_df, reservations_df, employees_df]) or not customer_api_data:
        print("Missing raw data. Transformation aborted.")
        return None, None, None, None, None # ### MODIFIED ###
        
    print("Transforming data...")

    # --- 1. Transform DimTable ---
    dim_table = tables_df.rename(columns={
        'table_id': 'TableKey',
        'table_number': 'TableNumber',
        'section': 'Section',
        'max_capacity': 'Capacity'
    })
    
    # --- 2. Transform DimCustomer ---
    customer_list = []
    for user in customer_api_data:
        customer_list.append({
            'Phone': user['phone'],
            'CustomerName': f"{user['name']['first']} {user['name']['last']}"
        })
    
    dim_customer = pd.DataFrame(customer_list)
    dim_customer.reset_index(inplace=True)
    dim_customer['CustomerKey'] = 'C' + (dim_customer['index'] + 1).astype(str)
    dim_customer = dim_customer[['CustomerKey', 'CustomerName', 'Phone']]
    
    unknown_customer = pd.DataFrame({
        'CustomerKey': ['C-1'], 
        'CustomerName': ['Unknown Diner'], 
        'Phone': ['Unknown']
    })
    dim_customer = pd.concat([dim_customer, unknown_customer], ignore_index=True)

    # --- 3. Transform DimDate ---
    unique_dates_str = reservations_df['reservation_date'].unique()
    date_list = []
    for date_str in unique_dates_str:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        date_list.append({
            'DateKey': int(dt.strftime('%Y%m%d')),
            'FullDate': dt.date(),
            'MonthName': dt.strftime('%B'),
            'Year': dt.year,
            'DayOfWeek': dt.strftime('%A')
        })
    dim_date = pd.DataFrame(date_list)

    # --- ### NEW ### 4. Transform DimEmployee ---
    dim_employee = employees_df.rename(columns={
        'employee_id': 'EmployeeKey',
        'first_name': 'FirstName',
        'last_name': 'LastName',
        'position': 'Position'
    })
    dim_employee['FullName'] = dim_employee['FirstName'] + ' ' + dim_employee['LastName']
    dim_employee = dim_employee[['EmployeeKey', 'FullName', 'Position']]
    
    unknown_employee = pd.DataFrame({
        'EmployeeKey': [-1],
        'FullName': ['Unknown Staff'],
        'Position': ['Unknown']
    })
    dim_employee = pd.concat([dim_employee, unknown_employee], ignore_index=True)


    # --- ### MODIFIED ### 5. Transform FactReservations ---
    fact_reservations = reservations_df.copy()
    
    # a. Look up the DateKey
    fact_reservations = pd.merge(
        fact_reservations, dim_date,
        left_on='reservation_date', right_on='FullDate', how='left'
    )
    
    # b. Look up the TableKey
    fact_reservations = pd.merge(
        fact_reservations, dim_table,
        left_on='table_id', right_on='TableKey', how='left'
    )
    
    # c. Look up the CustomerKey
    fact_reservations = pd.merge(
        fact_reservations, dim_customer,
        left_on='customer_phone', right_on='Phone', how='left'
    )
    fact_reservations['CustomerKey'] = fact_reservations['CustomerKey'].fillna('C-1')

    # ### NEW ###
    # d. Look up the EmployeeKey
    fact_reservations = pd.merge(
        fact_reservations, dim_employee,
        left_on='employee_id', right_on='EmployeeKey', how='left'
    )
    fact_reservations['EmployeeKey'] = fact_reservations['EmployeeKey'].fillna(-1)

    # e. Final clean-up
    fact_reservations = fact_reservations.rename(columns={
        'reservation_id': 'ReservationKey',
        'party_size': 'PartySize',
        'reservation_time': 'ReservationTime'
    })
    
    # ### MODIFIED ### Select final columns
    fact_reservations = fact_reservations[[
        'ReservationKey', 'DateKey', 'CustomerKey', 'TableKey', 'EmployeeKey', # Added EmployeeKey
        'PartySize', 'ReservationTime'
    ]]

    print("Transformation complete.")
    # ### MODIFIED ### Return all 5 DataFrames
    return dim_table, dim_customer, dim_date, dim_employee, fact_reservations

def load(dim_table, dim_customer, dim_date, dim_employee, fact_reservations): # ### MODIFIED ###
    """
    Loads the transformed DataFrames into the Data Mart SQLite database.
    """
    # ### MODIFIED ### Check for all 5 DataFrames
    if any(df is None for df in [dim_table, dim_customer, dim_date, dim_employee, fact_reservations]):
        print("Missing dataframes. Load aborted.")
        return

    print(f"Loading data into {MART_DB_NAME}...")
    try:
        conn_mart = sqlite3.connect(MART_DB_NAME)
        
        # Load each DataFrame into an SQL table
        dim_table.to_sql('DimTable', conn_mart, if_exists='replace', index=False)
        dim_customer.to_sql('DimCustomer', conn_mart, if_exists='replace', index=False)
        dim_date.to_sql('DimDate', conn_mart, if_exists='replace', index=False)
        dim_employee.to_sql('DimEmployee', conn_mart, if_exists='replace', index=False) # ### NEW ###
        fact_reservations.to_sql('FactReservations', conn_mart, if_exists='replace', index=False)
        
        conn_mart.commit()
        print("Data load complete.")
        
    except Exception as e:
        print(f"Error during data load: {e}")
    finally:
        if conn_mart:
            conn_mart.close()
            print(f"Database connection to {MART_DB_NAME} closed.")

# --- Main execution ---
if __name__ == "__main__":
    print("Starting ETL pipeline...")
    
    # 1. Extract
    # ### MODIFIED ###
    tables_df, reservations_df, customer_data, employees_df = extract()
    
    # 2. Transform
    # ### MODIFIED ###
    dim_table, dim_customer, dim_date, dim_employee, fact_res = transform(
        tables_df, reservations_df, customer_data, employees_df
    )
    
    # 3. Load
    # ### MODIFIED ###
    load(dim_table, dim_customer, dim_date, dim_employee, fact_res)
    
    print("ETL pipeline finished successfully.")

