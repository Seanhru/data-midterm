import sqlite3

# This script creates our "source" relational database (OLTP)
# We will run this script ONE TIME to simulate a real HR database.

DB_NAME = "source_hr.db"
TABLE_NAME = "employees"

# Data to be inserted
employees_data = [
    (101, 'Sarah', 'Chen', 'Host'),
    (102, 'Michael', 'Smith', 'Server'),
    (103, 'David', 'Lee', 'Server'),
    (104, 'Emily', 'Brown', 'Manager')
]

# Create the database and table
try:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Drop table if it exists (to make this script runnable again)
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    
    # Create the table
    cursor.execute(f"""
    CREATE TABLE {TABLE_NAME} (
        employee_id INTEGER PRIMARY KEY,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        position TEXT NOT NULL
    )
    """)
    
    print(f"Table '{TABLE_NAME}' created successfully.")
    
    # Insert the data
    cursor.executemany(f"""
    INSERT INTO {TABLE_NAME} (employee_id, first_name, last_name, position)
    VALUES (?, ?, ?, ?)
    """, employees_data)
    
    print(f"{len(employees_data)} records inserted into '{TABLE_NAME}'.")
    
    conn.commit()
    print("Database 'source_hr.db' created and populated.")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if conn:
        conn.close()

    
