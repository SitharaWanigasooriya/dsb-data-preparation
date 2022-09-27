import random
from datetime import datetime

from common import get_outlets, get_dates, write_data_to_file, get_week_day_orders, \
    get_seasonal_orders

popular_districts = []


def orders():
    order_ddl = []
    order_id = 1
    outlets = get_outlets().values
    dates = get_dates().values
    for outlet in outlets:
        for day in dates:

            no_of_orders = get_min_orders()
            weekend_orders = get_week_day_orders(datetime.strptime(day[0], '%Y-%m-%d'))
            seasonal_orders = get_seasonal_orders(datetime.strptime(day[0], '%Y-%m-%d'))

            no_of_orders = no_of_orders + weekend_orders + seasonal_orders

            for i in range(0, no_of_orders):
                order_id = order_id + 1
                ddl = 'INSERT INTO ORDERS(order_id,outlet_id,order_type,date_id) VALUES (' \
                      + str(order_id) + ',' \
                      + str(outlet[0]) + ',\'' \
                      + str(get_order_type()) + '\',\'' \
                      + str(day[0]) + '\');'
                print(ddl)
                order_ddl.append(ddl)

    write_data_to_file(order_ddl, 'ddls/12.orders_ddl.txt')


def get_min_orders():
    return 2


def get_order_type():
    order_types = ['take-away', 'dining', 'delivery']
    return order_types[random.randint(0, 2)]


if __name__ == '__main__':
    orders()
