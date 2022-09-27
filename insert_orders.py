import dask.dataframe as dd
import pyodbc


def sql_con():
    cnxn_str = ("Driver={SQL Server Native Client 11.0};"
                "Server=SITHARA-WANIGAS;"
                "Database=DBMS;"
                "UID=sa;"
                "PWD=root;")
    return pyodbc.connect(cnxn_str)


# get defined sales
def insert_sales():
    connection = sql_con()
    cursor = connection.cursor()
    i = 0
    sales = dd.read_csv('ddls/12.orders_ddl.txt', sep="$", header=None, engine='python')
    for sale in sales.itertuples():
        for length in range(0, sale.__len__()):
            if isinstance(sale[length], str):
                if ('INSERT' in sale[length]):
                    for query in sale[length].split(";"):
                        if 'INSERT' in query:
                            i += 1
                            cursor.execute(query + ";")
                            print(query)
                            if i % 10000 == 0:
                                connection.commit()
                                print("**************************Committing**************************")


if __name__ == '__main__':
    insert_sales()
