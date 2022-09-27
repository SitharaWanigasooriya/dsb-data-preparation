import math

from common import write_data_to_file, get_outlets, \
    get_quarters


def tax_scripts():
    tax_ddl = []
    outlets = get_outlets().values
    quarters = get_quarters().values

    for outlet in outlets:
        for quarter in quarters:
            outlet_id = outlet[0]
            quarter_id = quarter[0]
            tax = get_tax(int(quarter[1]), int(quarter_id))

            ddl = 'INSERT INTO Taxes(outlet_id, quarter_id, tax) VALUES (' \
                  + str(outlet_id) + ',' \
                  + str(quarter_id) + ',' \
                  + str(tax) + ');'
            print(ddl)
            tax_ddl.append(ddl)
    write_data_to_file(tax_ddl, 'ddls/14.tax_ddl.txt')


def get_tax(year, quarter_id):
    return str(math.floor((year / 2022) * 40000) + quarter_id * 5000)


if __name__ == '__main__':
    tax_scripts()
