import math
import random

from common import write_data_to_file, get_towns

assigned_oldest_outlet_age = 0


# popularity index of a town is proportional to age of a outlet
def outlet_scripts():
    outlet_ddl = []
    towns = get_towns().values
    outlet_id = 1
    for town in towns:
        for i in range(0, town[2]):
            ddl = 'INSERT INTO Outlet(outlet_id,town_id,long,lat,popularity_index,age,employee_count) VALUES (' \
                  + str(outlet_id) + ',' \
                  + str(town[0]) + ',' \
                  + '\'\'' + ',' \
                  + '\'\'' + ',' \
                  + str(town[1]) + ',' \
                  + get_outlet_age(town[0], town[1]) + ',' \
                  + get_employee_count(town[1]) + ');'
            print(ddl)
            outlet_ddl.append(ddl)
            outlet_id += 1
            if outlet_id == 300:
                break
        if outlet_id == 300:
            break
    write_data_to_file(outlet_ddl, 'ddls/4.outlet_ddl')


def get_outlet_age(town_id, priority_index):
    global assigned_oldest_outlet_age
    if town_id == 14:
        if assigned_oldest_outlet_age == 0:
            assigned_oldest_outlet_age = 1
            return str(42)
        return str(math.floor((random.randint(36, 42) / 100) * priority_index))
    else:
        return str(math.floor((random.randint(1, 36) / 100) * priority_index))


def get_employee_count(priority_index):
    return str(math.floor((priority_index / 100) * 30))


if __name__ == '__main__':
    outlet_scripts()
