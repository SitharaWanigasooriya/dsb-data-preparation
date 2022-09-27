import pandas as pd

from common import write_data_to_file


def employee_scripts():
    df = pd.read_csv('raw_data/babynames-clean.csv', header=None)
    employee_ddl = []

    for i in range(1, 3001):
        ddl = 'INSERT INTO Employee(employee_id,employee_name) VALUES (' + str(i) + ',\'' + df.values[i][0] + '\');'
        print(ddl)
        employee_ddl.append(ddl)

    write_data_to_file(employee_ddl, 'ddls/5.employee_ddl.txt')


if __name__ == '__main__':
    employee_scripts()
