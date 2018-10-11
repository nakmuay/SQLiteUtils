from sqlite_utils import SQLiteDataAdapter, SQLiteDataAdapterConnection, Query
from matplotlib import pyplot as plt
import time

import math
import random as rnd


def create_test_table(adapter, table_name, num_rows=100):
    tquery = time.time()

    # Check if table already exist
    tables = adapter.get_tables()
    if table_name in tables:
        print("Table '{0}' already exists. The table will not be overwritten.".format(table_name))
        return

    def create_rows(num_rows):
        i = 0
        while i < num_rows:
            x = i
            y = math.sin(x + rnd.random())
            yield ({"id": None, "x": x, "y": y})
            i += 1

    adapter.create_table(table_name)
    adapter.insert(table_name, create_rows(num_rows))
    print("Time for creating table: {0}.".format(time.time() - tquery))

def main():
    adapter = SQLiteDataAdapter("./test.db")
    table_names = ["table_{0}".format(i) for i in range(5)]

    for table_name in table_names:
        tquery = time.time()
        create_test_table(adapter, table_name, 10000)

        query = "SELECT x, y from {0} \
                 WHERE x > 51 AND \
                 y > 0.0 AND \
                 y < 0.85".format(table_name)
        rows = adapter.execute_query(query)
    
        print("Time for querying table: {0}, number of rows returned {1}".format(time.time() - tquery, len(rows)))
        plt.figure()
        plt.plot(*zip(*rows))

    plt.show()

def query_test():
    q = Query("test")
    q.select(['x', 'z'])
    
    print(q)

if __name__ == "__main__":
    #main()
    query_test()
