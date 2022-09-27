import math
import random

from common import write_data_to_file, get_outlets, get_months


def operations_cost_scripts():
    operations_cost_ddl = []
    outlets = get_outlets().values
    months = get_months().values

    for outlet in outlets:
        for month in months:
            outlet_id = outlet[0]
            month_id = month[0]
            staff_cost = get_staff_cost(int(month[1]), int(outlet[6]), int(outlet[4]))
            operation_cost = get_operation_cost(int(outlet[4]))
            ddl = 'INSERT INTO OperatingCosts(outlet_id,month_id,staff_cost,operation_cost) VALUES (' \
                  + str(outlet_id) + ',' \
                  + str(month_id) + ',' \
                  + str(staff_cost) + ',' \
                  + str(operation_cost) + ');'
            print(ddl)
            operations_cost_ddl.append(ddl)
    write_data_to_file(operations_cost_ddl, 'ddls/13.operations_cost_ddl.txt')


def get_staff_cost(year, employee_count, popularity_index):
    manager_staff_members = math.floor((popularity_index / 100) * 5)
    normal_staff_members = employee_count - manager_staff_members

    manager_staff_member_salary = (year / 2022) * random.randint(50000, 70000)
    normal_staff_member_salary = (year / 2022) * random.randint(25000, 27500)
    return str(manager_staff_member_salary * manager_staff_members + normal_staff_member_salary * normal_staff_members)


def get_operation_cost(popularity_index):
    return math.floor((popularity_index / 100) * random.randint(20000, 50000))


if __name__ == '__main__':
    operations_cost_scripts()
