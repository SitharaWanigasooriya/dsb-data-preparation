import random

import pandas as pd

from common import write_data_to_file


def menu_item_scripts():
    df = pd.read_csv('raw_data/Dish.csv', header=None)
    menu_item_ddl = []
    for i in range(1, 51):
        ddl = 'INSERT INTO MenuItems(menu_item_id,menu_item_name,menu_item_category,price) VALUES (' \
              + str(i) + ',\'' \
              + df.values[i][1] + '\',' \
              + '\'\'' + ',' \
              + str(random.randint(350, 4500)) + ');'
        print(ddl)
        menu_item_ddl.append(ddl)
    write_data_to_file(menu_item_ddl, 'ddls/7.menu_item_ddl')


if __name__ == '__main__':
    menu_item_scripts()
