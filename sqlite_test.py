from sqlite_utils import SQLiteDataAdapter, SQLiteDataAdapterConnection
from matplotlib import pyplot as plt
import time

def main():
    adapter = SQLiteDataAdapter("./test.db")
    table_name = "data"

    #tquery = time.time()
    #adapter.create_test_table(table_name, 10000)
    #print("Time for creating table: {0}.".format(time.time() - tquery))

    tables = adapter.get_tables()
    print(tables)
    
    schema = adapter.get_table_schema(tables[0])
    print(schema)
    
    tquery = time.time()
    """
    query = "SELECT x, y from {0} \
             WHERE x > 51 AND \
             y > 0.0 AND \
             y < 0.85".format(table_name)
    """
    
    query = "SELECT x, y from {0} WHERE x*y < 10".format(table_name)
    rows = adapter.execute_query(query)
    
    print("Time for querying table: {0}, number of rows returned {1}".format(time.time() - tquery, len(rows)))
    print(rows[0])
    plt.plot(*zip(*rows))
    plt.show()

if __name__ == "__main__":
    main()
