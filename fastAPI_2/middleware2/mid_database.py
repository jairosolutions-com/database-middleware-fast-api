from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Define database URLs for db1 and db2
SQLALCHEMY_DATABASE_URL1 = "sqlite:///./fastAPI_1/fastapi_1-practice.db"
SQLALCHEMY_DATABASE_URL2 = "sqlite:///./fastAPI_2/fastapi_2-practice.db"

# Create SQLAlchemy engine objects for db1 and db2
engine1 = create_engine(SQLALCHEMY_DATABASE_URL1)
engine2 = create_engine(SQLALCHEMY_DATABASE_URL2)

# Create sessionmaker objects for db1 and db2
SessionLocal1 = sessionmaker(autocommit=False, autoflush=False, bind=engine1)
SessionLocal2 = sessionmaker(autocommit=False, autoflush=False, bind=engine2)

# Function to check database connection for db1
def check_db1_connection():
    try:
        with engine1.connect():
            return True
    except Exception as e:
        print(f"Failed to connect to db1: {e}")
        return False

# Function to check database connection for db2
def check_db2_connection():
    try:
        with engine2.connect():
            return True
    except Exception as e:
        print(f"Failed to connect to db2: {e}")
        return False

# Function to print data from db1
def print_data_db1():
    if not check_db1_connection():
        print("Failed to connect to db1.")
        return

    print("Successfully connected to db1.")
    inspector = inspect(engine1)
    table_names = inspector.get_table_names()
    if not table_names:
        print("No tables found in db1.")
        return

    most_recent_date_db1 = None  # Variable to store the most recent 'modifiedAt' date in db1
    most_recent_table_db1 = None  # Variable to store the table with the most recent 'modifiedAt' date in db1

    for table_name in table_names:
        print(f"Table Name: {table_name}")
        columns = inspector.get_columns(table_name)
        print("Columns:")
        for column in columns:
            print(column["name"])
        print("Data:")
        Session = SessionLocal1()
        try:
            query = text(f"SELECT * FROM {table_name}")
            result = Session.execute(query)
            for row in result:
                print(row)
                modified_at = row.modifiedAt  # Assuming 'modifiedAt' is a column in the table
                if modified_at and (most_recent_date_db1 is None or modified_at > most_recent_date_db1):
                    most_recent_date_db1 = modified_at
                    most_recent_table_db1 = table_name
        finally:
            Session.close()

    return most_recent_date_db1, most_recent_table_db1

# Function to print data from db2
def print_data_db2():
    if not check_db2_connection():
        print("Failed to connect to db2.")
        return

    print("Successfully connected to db2.")
    inspector = inspect(engine2)
    table_names = inspector.get_table_names()
    if not table_names:
        print("No tables found in db2.")
        return

    most_recent_date_db2 = None  # Variable to store the most recent 'modifiedAt' date in db2
    most_recent_table_db2 = None  # Variable to store the table with the most recent 'modifiedAt' date in db2

    for table_name in table_names:
        print(f"Table Name: {table_name}")
        columns = inspector.get_columns(table_name)
        print("Columns:")
        for column in columns:
            print(column["name"])
        print("Data:")
        Session = SessionLocal2()
        try:
            query = text(f"SELECT * FROM {table_name}")
            result = Session.execute(query)
            for row in result:
                print(row)
                modified_at = row.modifiedAt  # Assuming 'modifiedAt' is a column in the table
                if modified_at and (most_recent_date_db2 is None or modified_at > most_recent_date_db2):
                    most_recent_date_db2 = modified_at
                    most_recent_table_db2 = table_name
        finally:
            Session.close()

    return most_recent_date_db2, most_recent_table_db2

# Compare the most recent 'modifiedAt' dates between db1 and db2 and copy data accordingly
def compare_modified_dates(check_db1_func, check_db2_func):
    most_recent_date_db1, most_recent_table_db1 = print_data_db1()
    most_recent_date_db2, most_recent_table_db2 = print_data_db2()

    if most_recent_date_db1 and most_recent_date_db2:
        if most_recent_date_db1 > most_recent_date_db2:
            print("Copying data from db1 to db2...")
            copy_data_db1_to_db2()
        elif most_recent_date_db1 < most_recent_date_db2:
            print("Copying data from db2 to db1...")
            copy_data_db2_to_db1()
        else:
            print("Both databases have the same 'modifiedAt' date.")
    else:
        print("Unable to compare 'modifiedAt' dates as one or both databases have no data.")

# Function to copy data from db1 to db2
def copy_data_db1_to_db2():
    # Add your code here to copy data from db1 to db2
    pass  # Placeholder, replace with actual copying logic

# Function to copy data from db2 to db1
def copy_data_db2_to_db1():
    # Add your code here to copy data from db2 to db1
    pass  # Placeholder, replace with actual copying logic

if __name__ == "__main__":
    compare_modified_dates(check_db1_connection, check_db2_connection)
