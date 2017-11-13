import sqlite3 as lite
from contextlib import closing
import random as random
import math

class SQLiteDataAdapterConnection():

    def __init__(self, adapter):
        self._adapter = adapter
        self._connection = lite.connect(self._adapter.database)

    def execute_query(self, query):
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

        # Insert row data
        query = "INSERT INTO {0} VALUES({1})".format(table_name, bindings)
        with closing(self._connection.cursor()) as cursor:
            for row in rows:
                cursor.execute(query, row)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._connection.commit()
        self._connection.close()

class SQLiteDataAdapter():

    def __init__(self, database):
        self._database = database

    @property
    def database(self):
        return self._database

    def get_connection(self):
        return SQLiteDataAdapterConnection(self)

    # Temporary hardcoded implementation
    def create_table(self, table_name):
        print("Creating table: {0}".format(table_name))
        with self.get_connection() as conn:
            conn.execute_query("CREATE TABLE {0} (id INTEGER PRIMARY KEY, x REAL, y REAL)".format(table_name))

    def create_test_table(self, table_name, number_of_rows):
        self.create_table(table_name)
        self.insert(table_name, self._get_rows(number_of_rows))

    def _get_rows(self, num_rows):
        i = 0
        while i < num_rows:
            x = i
            y = math.sin(x + random.random()/10)
            yield ({"id": None, "x": x, "y": y})
            i += 1

    def insert(self, table_name, column_dict):
        with self.get_connection() as conn:
            conn.execute_insert_query(table_name, column_dict)

    def get_tables(self):
        with self.get_connection() as conn:
            result = conn.execute_query("SELECT * FROM sqlite_master WHERE type='table' LIMIT 1;")
        tables = [table[1] for table in result]
        return tables

    def get_table_schema(self, table_name):
        with self.get_connection() as conn:
            query = "PRAGMA table_info({0})".format(table_name)
            result = conn.execute_query(query)
        return result[0]

    def execute_query(self, query):
        with self.get_connection() as conn:
            rows = conn.execute_query(query)
        return rows
