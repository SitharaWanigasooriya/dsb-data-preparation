import math

from common import write_data_to_file, get_quarter_name, get_months


def quarter_scripts():
    quarter_ddl = []
    quarter_id = 1
    current_quarter = 1
    months = get_months().values
    for month in months:
        if math.ceil(int(month[3]) / 3) == current_quarter:
            ddl = 'INSERT INTO Quarter(' \
                  'quarter_id, ' \
                  'year_id, ' \
                  'quarter_name) VALUES (' \
                  + str(quarter_id) + ',' \
                  + str(month[1]) + ',' \
                  + '\'' + get_quarter_name(current_quarter) + '\');'
            current_quarter = get_next_quarter(current_quarter)
            quarter_id = quarter_id + 1
            print(ddl)
            quarter_ddl.append(ddl)
    write_data_to_file(quarter_ddl, 'ddls/10.quarter_ddl.txt')


def get_next_quarter(current_quarter):
    if current_quarter < 4:
        return current_quarter + 1
    else:
        return 1


if __name__ == '__main__':
    quarter_scripts()
