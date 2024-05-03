import datetime
from sqlalchemy import create_engine, inspect, text, update
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Define database URLs for db1 and db2
SQLALCHEMY_DATABASE_URL1 = "sqlite:///../fastAPI_1/fastapi_1-practice.db"
SQLALCHEMY_DATABASE_URL2 = "sqlite:///../fastAPI_2/fastapi_2-practice.db"

# Create SQLAlchemy engine objects for db1 and db2
engine1 = create_engine(SQLALCHEMY_DATABASE_URL1)
engine2 = create_engine(SQLALCHEMY_DATABASE_URL2)

# Create sessionmaker objects for db1 and db2
SessionLocal1 = sessionmaker(autocommit=False, autoflush=False, bind=engine1)
SessionLocal2 = sessionmaker(autocommit=False, autoflush=False, bind=engine2)

# global vavriables
modiAtList1 = []
modiAtList2 = []
idList1 = []
idList2 = []
modifiedCol = "modifiedAt"
idCol = "id"
tableName = ""
colList = []
rowVals2 = []
missingDb1 = []
missingDb2 = []


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


def get_data_db1():
    global tableName
    data_list = []

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
        tableName = table_name
        print(f"Table Name: {table_name}")
        columns = inspector.get_columns(table_name)
        print("Columns:")
        for column in columns:
            print(column["name"])
        print("Data1:")
        Session = SessionLocal1()
        try:
            query = text(f"SELECT * FROM {table_name}")
            result = Session.execute(query)
            for row in result:
                data_dict = {row}
                data_list.append(data_dict)
                k = getattr(row, modifiedCol)
                k = datetime.strptime(k.split(".")[0], "%Y-%m-%d %H:%M:%S")
                k = k.strftime("%Y-%m-%d %H:%M:%S")
                modiAtList1.append(k)
                idList1.append(getattr(row, idCol))
        finally:
            Session.close()

        # print(data_list)
        print(modiAtList1)

    return data_list


def get_data_db2():
    global tableName
    data_list2 = []
    global colList

    if not check_db1_connection():
        print("Failed to connect to db1.")
        return data_list2

    print("Successfully connected to db1.")
    inspector = inspect(engine2)
    table_names = inspector.get_table_names()
    if not table_names:
        print("No tables found in db1.")
        return data_list2

    for table_name in table_names:
        tableName = table_name
        print(f"Table Name: {table_name}")
        columns = inspector.get_columns(table_name)
        print("Columns:")
        for column in columns:
            colList.append(column["name"])
            print(column["name"])
        print("Data2:")
        Session = SessionLocal2()
        try:
            query = text(f"SELECT * FROM {table_name}")
            result = Session.execute(query)
            for row in result:
                data_dict2 = {row}
                data_list2.append(data_dict2)
                k = getattr(row, modifiedCol)
                k = datetime.strptime(k.split(".")[0], "%Y-%m-%d %H:%M:%S")
                k = k.strftime("%Y-%m-%d %H:%M:%S")
                modiAtList2.append(k)
                idList2.append(getattr(row, idCol))

        finally:
            Session.close()

        # print(modiAtList2)

    return data_list2


def compareDBs():
    idtoChangeTodb1 = []
    idtoChangeTodb2 = []
    missingDb1 = []
    missingDb2 = []

    # will update
    for i, j, k, o in zip(idList1, idList2, modiAtList1, modiAtList2):
        if i == j:
            if k > o:
                # db1 is more recent
                print(f"db1 id{i} ({k}) is more reccent than id{j} ({o})")
                idtoChangeTodb1.append(i)
            elif k == o:
                print("no data to update")
            else:
                # db2 is more recent
                print(f"db2 id{j} ({o}) is more reccent than id{i} ({k})")
                idtoChangeTodb2.append(j)
    # here is the method for updating values
    Updates(tableName, idtoChangeTodb1, idtoChangeTodb2)

    # insert missing
    # make array the same length to stop error
    longer = idList1 if len(idList1) > len(idList2) else idList2
    shorter = idList2 if len(idList1) > len(idList2) else idList1

    # Pad the shorter array with dummy values
    shorter.extend([None] * (len(longer) - len(shorter)))

    for n1, id1 in enumerate(idList1):
        if id1 != idList2[n1]:
            if idList2[n1] is not None:
                missingDb1.append(idList2[n1])

    for n2, id2 in enumerate(idList2):
        if id2 != idList1[n2]:
            if idList1[n2] is not None:
                missingDb2.append(idList1[n2])

    # here is the method for inserting missing rows
    insertMissing(missingDb1, missingDb2)


def Updates(tbName, db1ids, db2ids):
    Session1 = SessionLocal1()
    for id1 in db1ids:
        try:
            query = text(f"SELECT * FROM {tbName} WHERE id = {id1}")
            result = Session1.execute(query)
            row = result.fetchone()
            if row:
                for n, _row in enumerate(row):
                    update_row_intoDb2(tbName, id1, colList[n], row[n])
                print(row)
            else:
                print(f"No row found with ID {id1}")
        finally:
            Session1.close()

    Session2 = SessionLocal2()
    for id2 in db2ids:
        try:
            query = text(f"SELECT * FROM {tbName} WHERE id = {id2}")
            result = Session2.execute(query)
            row = result.fetchone()
            if row:
                for n, _row in enumerate(row):
                    update_row_intoDb1(tbName, id2, colList[n], row[n])
                print(row)
            else:
                print(f"No row found with ID {id2}")
        finally:
            Session2.close()


def update_row_intoDb1(tbName, idValue, column_name, new_value):
    Session = SessionLocal1()
    try:
        print(tbName)
        print(column_name)
        print(new_value)
        print(idValue)

        # Construct an update query using SQLAlchemy's update function
        query = text(
            f"UPDATE {tbName} SET {column_name} = '{new_value}' WHERE ID = {idValue};"
        )

        # Execute the update query
        Session.execute(query)
        Session.commit()
        print(f"Row with ID {idValue} updated successfully.")
    except Exception as e:
        Session.rollback()
        print(f"Failed to update row with ID {idValue}: {e}")
    finally:
        Session.close()


def update_row_intoDb2(tbName, idValue, column_name, new_value):
    Session = SessionLocal2()
    try:
        print(f"table: {tbName}")
        print(f"column: {column_name}")
        print(f"newval: {new_value}")
        print(f"id: {idValue}")

        # Construct an update query using SQLAlchemy's update function
        query = text(
            f"UPDATE {tbName} SET {column_name} = '{new_value}' WHERE ID = {idValue};"
        )

        # Execute the update query
        Session.execute(query)
        Session.commit()
        print(f"Row with ID {idValue} updated successfully.")
    except Exception as e:
        Session.rollback()
        print(f"Failed to update row with ID {idValue}: {e}")
    finally:
        Session.close()


def insertMissing(db1ids, db2ids):
    Session1 = SessionLocal1()
    for id1 in db2ids:
        try:
            query = text(f"SELECT * FROM {tableName} WHERE id = :id_value")
            result = Session1.execute(query, {"id_value": id1})
            row = result.fetchone()
            if row:
                missing2(row)
                print(f"ahhhhh {row}")
            else:
                print(f"wrong")
        finally:
            Session1.close()

    Session2 = SessionLocal2()
    for id2 in db1ids:
        try:
            query = text(f"SELECT * FROM {tableName} WHERE id = :id_value")
            result = Session2.execute(query, {"id_value": id2})
            row = result.fetchone()
            if row:
                missing1(row)
                print(f"ahhhhh {row}")
            else:
                print(f"wrong")
        finally:
            Session2.close()


def missing1(row):
    Session = SessionLocal1()
    try:
        # Construct an update query using SQLAlchemy's update function
        query = text(f"INSERT INTO {tableName} VALUES {row};")
        # Execute the update query
        Session.execute(query)
        Session.commit()
        print(f"Row with ID nn updated successfully.")
    except Exception as e:
        Session.rollback()
        print(f"Failed to update row with ID nn: {e}")
    finally:
        Session.close()


def missing2(row):
    Session = SessionLocal2()
    try:
        # Construct an update query using SQLAlchemy's update function
        query = text(f"INSERT INTO {tableName} VALUES {row};")
        # Execute the update query
        Session.execute(query)
        Session.commit()
        print(f"Row with ID nn updated successfully.")
    except Exception as e:
        Session.rollback()
        print(f"Failed to update row with ID nn: {e}")
    finally:
        Session.close()


def delete_missing(db1ids, db2ids, db_name):
    Session = SessionLocal1() if db_name == "db1" else SessionLocal2()
    for id_value in db1ids if db_name == "db1" else db2ids:
        try:
            query = text(f"DELETE FROM {tableName} WHERE id = :id_value")
            Session.execute(query, {"id_value": id_value})
            Session.commit()
            print(f"Row with ID {id_value} deleted from {db_name}.")
        except Exception as e:
            Session.rollback()
            print(f"Failed to delete row with ID {id_value} from {db_name}: {e}")
        finally:
            Session.close()


def delete_method():
    pass


get_data_db1()
get_data_db2()
compareDBs()
