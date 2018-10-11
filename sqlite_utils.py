import sqlite3 as lite
from contextlib import closing
import random as random
import math

class SelectClause():

    def __init__(self, columns):
        self._columns = columns

    def __str__(self):
        columns_str = ", ".join([c for c in self._columns])
        return "SELECT {0}".format(columns_str)

class LtPredicate():

    def __init__(self, column, value):
        self._column = column
        self._value = value

    def __str__(self):
        return "{0} < {1}".format(self._column, self._value)
        

class Query():

    def __init__(self, table_name):
        self._table_name = table_name
        self._select_clause = None
        self._where_clause = None

    def __str__(self):
        query = str(self._select_clause)

        print(LtPredicate('x', 60))

        """
        query = "SELECT x, y from {0} \
                 WHERE x > 51 AND \
                 y > 0.0 AND \
                 y < 0.85".format(self._table_name)
        """
        return query

    def select(self, columns):
        self._select_clause = SelectClause(columns)

class SQLiteDataAdapterConnection():

    def __init__(self, adapter):
        self._adapter = adapter
        self._connection = lite.connect(self._adapter.database)

    def execute_select_query(self, query):
        with closing(self._connection.cursor()) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
        return rows

    def execute_insert_query(self, table_name, rows):
        # Get column names
        column_names_query = "SELECT * from {0} LIMIT 1".format(table_name)
        with closing(self._connection.cursor()) as cursor:
            cursor.execute(column_names_query)
            column_names = [desc[0] for desc in cursor.description]

        # Create bindings for column names
        bindings = ", ".join([':' + name for name in column_names])
        print("Bindings: " + bindings)

        # Insert row data
        query = "INSERT INTO {0} VALUES({1})".format(table_name, bindings)
        with closing(self._connection.cursor()) as cursor:
            for row in rows:
                cursor.execute(query, row)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        print("Closing connection")
        self._connection.commit()
        self._connection.close()

class SQLiteDataAdapter():

    def __init__(self, database):
        self._database = database

    @property
    def database(self):
        return self._database

    def _get_connection(self):
        return SQLiteDataAdapterConnection(self)

    # Temporary hardcoded implementation
    def create_table(self, table_name):
        print("Creating table: {0}".format(table_name))
        with self._get_connection() as conn:
            conn.execute_select_query("CREATE TABLE {0} (id INTEGER PRIMARY KEY, x REAL, y REAL)".format(table_name))

    def insert(self, table_name, rows_dict):
        with self._get_connection() as conn:
            conn.execute_insert_query(table_name, rows_dict)

    def get_tables(self):
        with self._get_connection() as conn:
            result = conn.execute_select_query("SELECT * FROM sqlite_master WHERE type='table'")
        tables = [table[1] for table in result]
        return tables

    def get_table_schema(self, table_name):
        with self._get_connection() as conn:
            query = "PRAGMA table_info({0})".format(table_name)
            result = conn.execute_select_query(query)
        return result[0]

    def execute_query(self, query):
        with self._get_connection() as conn:
            rows = conn.execute_select_query(query)
        return rows
