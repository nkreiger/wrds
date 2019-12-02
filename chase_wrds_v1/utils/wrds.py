# packages
import wrds

# services
from settings import settings


def connect_first():
    """
    Function to connect to WRDS and establish .pgpass file

    :return db: wrds database connection
    """
    db = wrds.Connection(wrds_username=settings.WRDS_USER)

    db.create_pgpass_file()

    return db


def connect():
    """
    Function to connect to WRDS

    :return: db: wrds database connection
    """
    db = wrds.Connection(wrds_username=settings.WRDS_USER)

    return db

def econnection(db):
    """
    Function to end connection to WRDS

    :param db: wrds database connection
    :return:
    """
    db.close()

def execute_query(db, query):
    """
    Function to execute sql query on WRDS DB Connection

    :param db: wrds database connection
    :param query: string
    :return: query result
    """

    return db.raw_sql(query)

def getAllLibraries(db):
    """
    Function to get all libraries from WRDS

    :param db: wrds database connection
    :return: query result
    """
    return db.list_libraries()

def getTables(db, library):
    """
    Function to get sql tables from WRDS

    :param db: wrds database connection
    :param library: string
    :return: query result
    """
    return db.list_tables(library=library)

def getTableData(db, library, table):
    """
       Function to get sql table from WRDS

       :param db: wrds database connection
       :param library: string
       :param table: string
       :return: query result
       """

    return db.get_table(library=library, table=table)
