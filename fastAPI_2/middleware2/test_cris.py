from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.dml import Insert

# Define database URLs for db1 and db2
SQLALCHEMY_DATABASE_URL1 = "sqlite:///../fastAPI_1/fastapi_1-practice.db"
SQLALCHEMY_DATABASE_URL2 = "sqlite:///../fastAPI_2/fastapi_2-practice.db"

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

def get_data_db1(getId):
    data_list = {}

    if not check_db1_connection():
        print("Failed to connect to db1.")
        return data_list

    print("Successfully connected to db1.")
    inspector = inspect(engine1)
    table_names = inspector.get_table_names()
    if not table_names:
        print("No tables found in db1.")
        return data_list

    for table_name in table_names:
        print(f"Table Name: {table_name}")
        columns = inspector.get_columns(table_name)
        print("Columns:")
        for column in columns:
            print(column["name"])
        print("Data:")
        Session = SessionLocal1()
        try:
            query = text(f"SELECT * FROM {table_name} WHERE id = {getId}")
            result = Session.execute(query)
            # for column in columns:
            #     for row in result:
            #         for i in range(0, len(row)):
            #             data_list = {
            #                 column['name'] : row[i]
            #             }
            for row in result:
                data_list.update({columns[i]['name']: row[i] for i in range(len(row))})
        finally:
            Session.close()
        
        print(data_list)
        print(data_list['username'])

    return data_list

def get_data_db2(getId):
    data_list2 = {}

    if not check_db2_connection():
        print("Failed to connect to db2.")
        return data_list2

    print("Successfully connected to db2.")
    inspector = inspect(engine2)
    table_names = inspector.get_table_names()
    if not table_names:
        print("No tables found in db2.")
        return data_list2

    for table_name in table_names:
        print(f"Table Name: {table_name}")
        columns = inspector.get_columns(table_name)
        print("Columns:")
        for column in columns:
            print(column["name"])
        print("Data:")
        Session = SessionLocal2()
        try:
            query = text(f"SELECT * FROM {table_name} WHERE id = {getId}")
            result = Session.execute(query)
            # for column in columns:
            #     for row in result:
            #         for i in range(0, len(row)):
            #             data_list = {
            #                 column['name'] : row[i]
            #             }
            for row in result:
                data_list2.update({columns[i]['name']: row[i] for i in range(len(row))})
        finally:
            Session.close()
        
        print(data_list2)
        # print(data_list2['id'])

    return data_list2

def compare_databases():
    getId = input('Select from Id: ')

    data_from_db1 = get_data_db1(getId)
    data_from_db2 = get_data_db2(getId)

    print('Comparing 2 databases: ')

    if data_from_db1 == data_from_db2:
        print("The data in both databases is identical.")
    else:
        print("The data in the databases differs.")

        # Find and print differences
        for key in data_from_db1.keys():
            if key in data_from_db2:
                if data_from_db1[key]!= data_from_db2[key]:
                    if data_from_db1[key]['modifiedAt'] > data_from_db2[key]['modifiedAt']:
                        print(f"Different data found for {key}: db1={data_from_db1[key]}, db2={data_from_db2[key]}. Updating db2 with db1 data.")
                        data_from_db2[key] = data_from_db1[key]
                    else:
                        print(f"Different data found for {key}: db1={data_from_db1[key]}, db2={data_from_db2[key]}. Updating db1 with db2 data.")
                        data_from_db1[key] = data_from_db2[key]
            else:
                print(f"{key} is not present in db2.")

        for key in data_from_db2.keys():
            if key in data_from_db1:
                if data_from_db2[key]!= data_from_db1[key]:
                    if data_from_db2[key]['modifiedAt'] > data_from_db1[key]['modifiedAt']:
                        print(f"Different data found for {key}: db1={data_from_db1[key]}, db2={data_from_db2[key]}. Updating db1 with db2 data.")
                        data_from_db1[key] = data_from_db2[key]
                    else:
                        print(f"Different data found for {key}: db1={data_from_db1[key]}, db2={data_from_db2[key]}. Updating db2 with db1 data.")
                        data_from_db2[key] = data_from_db1[key]
            else:
                print(f"{key} is not present in db1.")

        if data_from_db1!= data_from_db2:
            print("Data from db1 is not present in db2. Inserting data...")
            data_from_db2 = data_from_db1
            # Session = SessionLocal2()
            # try:
            #     inspector = inspect(engine2)
            #     for table_name in inspector.get_table_names():
            #         stmt = Insert(table_name).values(**data_from_db1)
            #         Session.execute(stmt)
            #         Session.commit()
            #         print(f"Data inserted into {table_name} in db2.")
            # finally:
            #     Session.close()
            return data_from_db2
        elif data_from_db2!=data_from_db1:
            data_from_db1 = data_from_db2
            return data_from_db1
                
        print(data_from_db1)
        print(data_from_db2)


compare_databases()