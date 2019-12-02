# This class will house the api call to retreive both fictures and general data about a given team

import wrds

def getConnection():
    return wrds.Connection(wrds_username='kleiwc15')
    print('Connection Successful')
def closeConnection():
    # putConn(conn):
    return

def getAllLibraries():
    conn = getConnection()

    libraries = conn.list_libraries()

    closeConnection()

    return print(libraries)

def getTables(library):
    conn = getConnection()

    data = conn.list_tables(library=library)

    closeConnection()

    return print(data)

def getTableData(library, table):
    conn = getConnection()

    data = conn.get_table(library=library, table=table)

    closeConnection()

    return print(data)

def executeQuery(query):
    conn = getConnection()

    data = conn.raw_sql(query)

    closeConnection()

    return print(data)
