import random

from common import write_data_to_file, get_orders, get_menu_items, get_outlets


def sales_scripts():
    orders = get_orders().values
    menu_items = get_menu_items().values
    outlets = get_outlets()
    for order in orders:
        sales_ddl = []
        for item_no in range(1, get_item_count()):
            menu_item = menu_items[random.randint(0, 49)]
            print(menu_item)
            order_id = order[0]
            menu_item_id = menu_item[0]
            outlet_id = order[1]
            employee_id = random.randint(outlets[outlets.outlet_id == outlet_id].employee_count_min.values[0],
                                         outlets[outlets.outlet_id == outlet_id].employee_count_max.values[0])
            customer_id = random.randint(outlets[outlets.outlet_id == outlet_id].employee_count_min.values[0],
                                         outlets[outlets.outlet_id == outlet_id].employee_count_max.values[0])
            print(employee_id)
            print(customer_id)
            date_id = order[2]
            sales_amount = menu_item[3]
            ddl = 'INSERT INTO Sales(order_id,menu_item_id,outlet_id,employee_id,customer_id,date_id,sales_amount) VALUES (' \
                  + str(order_id) + ',' \
                  + str(menu_item_id) + ',' \
                  + str(outlet_id) + ',' \
                  + str(employee_id) + ',' \
                  + str(customer_id) + ',' \
                  + str(date_id) + ',' \
                  + str(sales_amount) + ');'
            print(ddl)
            sales_ddl.append(ddl)
        write_data_to_file(sales_ddl, 'ddls_light/15.sales_ddl.txt')


def get_item_count():
    max_limits = [1, 2, 3, 4, 5, 6]
    item_count = random.randint(1, max_limits[random.randint(0, 4)])
    print(item_count)
    return item_count


if __name__ == '__main__':
    sales_scripts()
