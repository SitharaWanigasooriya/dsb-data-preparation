import pandas as pd

from common import write_data_to_file


def customer_scripts():
    df = pd.read_csv('raw_data/babynames-clean.csv', header=None)
    customer_ddl = []
    for i in range(1, 3001):
        ddl = 'INSERT INTO Customer(customer_id,customer_name,customer_age) VALUES (' \
              + str(i) + ',\'' \
              + df.values[i + 3000][0] + '\'' \
              + ',' + str(10) + ');'
        print(ddl)
        customer_ddl.append(ddl)
    write_data_to_file(customer_ddl, 'ddls/6.customer_ddl.txt')


if __name__ == '__main__':
    customer_scripts()
