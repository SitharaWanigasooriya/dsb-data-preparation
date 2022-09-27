import math
import random
from datetime import timedelta, date, datetime

import dask.dataframe as dd
import pandas as pd


def write_data_to_file(customer_ddl, file_name):
    with open(file_name, 'a') as f:
        f.write('\n'.join(customer_ddl))


def daterange(date1, date2):
    for n in range(int((date2 - date1).days) + 1):
        yield date1 + timedelta(n)


def get_year(dt):
    if dt.year == 2022:
        return 13
    else:
        return 12


def get_the_season(dt):
    if dt.month == 1:
        return 1
    elif dt.month == 4:
        return 3
    elif dt.month == 12:
        return 2
    else:
        return 'null'


def get_quarter_name(quarter_number):
    if quarter_number == 1:
        return 'Q1'
    elif quarter_number == 2:
        return 'Q2'
    elif quarter_number == 3:
        return 'Q3'
    elif quarter_number == 4:
        return 'Q4'


def get_next_quarter(current_quarter):
    if current_quarter < 4:
        return current_quarter + 1
    else:
        return 1


def get_start_date():
    return date(2018, 1, 1)


def get_end_date():
    return date(2022, 9, 13)


def get_week_day_orders(dt):
    if dt.weekday() > 4:
        return random.randint(1, 45)
    else:
        return random.randint(0, 25)


def get_seasonal_orders(dt):
    if dt.month in (1, 4, 12):
        return random.randint(15, 100)
    else:
        return 0


# get defined towns
def get_towns():
    town_info_list = []
    for town in pd.read_csv('ddls/2.town_ddl', sep=";", header=None).values:
        cur_town = town[0].split("VALUES")[1] \
            .replace("(", "") \
            .replace(")", "") \
            .replace(";", "") \
            .replace(" ", "") \
            .split(",")
        curr_town_id = cur_town[0]
        curr_town_p_i = cur_town[3]
        miss_id = 1
        miss_index = 1
        if cur_town[0] != '':
            miss_id = 0
            miss_index = 0
            curr_town_id = int(cur_town[0])
        if cur_town[3] != '':
            miss_id = 0
            miss_index = 0
            curr_town_p_i = int(cur_town[3])

        if miss_id == 0 & miss_index == 0:
            town_info_list.append({
                'town_id': curr_town_id,
                'popularity_index': curr_town_p_i,
                'outlet_count': math.floor((curr_town_p_i / 100) * 20)
            })
        else:
            print(curr_town_id)
    town_df = pd.DataFrame.from_records(town_info_list)
    town_df.sort_values(by=['outlet_count'], ascending=False)
    return town_df


# get defined dates
def get_dates():
    date_list = []
    for date_ in pd.read_csv('ddls/11.dates_ddl.txt', sep=";", header=None).values:
        cur_date = date_[0].split("VALUES")[1] \
            .replace("(", "") \
            .replace(")", "") \
            .replace(";", "") \
            .replace(" ", "") \
            .split(",")
        date_id = cur_date[0].replace('\'', '')
        month_id = cur_date[1]
        year_id = cur_date[2]
        weekday = cur_date[3]
        day_number = cur_date[4]
        day_name = cur_date[5]

        date_list.append({
            'date_id': date_id,
            'month_id': month_id,
            'year_id': year_id,
            'weekday': weekday,
            'day_number': day_number,
            'day_name': day_name
        })
    date_df = pd.DataFrame.from_records(date_list)
    return date_df


# get defined months
def get_months():
    month_list = []
    for month in pd.read_csv('ddls/9.month_ddl.txt', sep=";", header=None).values:
        cur_month = month[0].split("VALUES")[1] \
            .replace("(", "") \
            .replace(")", "") \
            .replace(";", "") \
            .replace(" ", "") \
            .split(",")

        month_id = cur_month[0]
        year_id = cur_month[1]
        season_id = cur_month[2]
        month_number = cur_month[3]
        month_name = cur_month[4]

        month_list.append({
            'month_id': month_id,
            'year_id': year_id,
            'season_id': season_id,
            'month_number': month_number,
            'month_name': month_name
        })
    month_df = pd.DataFrame.from_records(month_list)
    return month_df


# get defined quarters
def get_quarters():
    quarter_list = []
    for quarter in pd.read_csv('ddls/10.quarter_ddl.txt', sep=";", header=None).values:
        cur_quarter = quarter[0].split("VALUES")[1] \
            .replace("(", "") \
            .replace(")", "") \
            .replace(";", "") \
            .replace(" ", "") \
            .split(",")

        quarter_id = cur_quarter[0]
        year_id = cur_quarter[1]
        quarter_name = cur_quarter[2]

        quarter_list.append({
            'quarter_id': quarter_id,
            'year_id': year_id,
            'quarter_name': quarter_name
        })
    quarter_df = pd.DataFrame.from_records(quarter_list)
    return quarter_df


# get defined outlet
def get_outlets():
    outlets_list = []
    employee_count_min = 1
    for outlet in pd.read_csv('ddls/4.outlet_ddl', sep=";", header=None).values:
        cur_outlet = outlet[0].split("VALUES")[1] \
            .replace("(", "") \
            .replace(")", "") \
            .replace(";", "") \
            .replace(" ", "") \
            .split(",")

        outlet_id = cur_outlet[0]
        town_id = cur_outlet[1]
        long = cur_outlet[2]
        lat = cur_outlet[3]
        popularity_index = cur_outlet[4]
        age = cur_outlet[5]
        employee_count = int(cur_outlet[6])
        employee_count_min_ = employee_count_min
        employee_count_min += employee_count
        employee_count_max_ = employee_count_min

        outlets_list.append({
            'outlet_id': outlet_id,
            'town_id': town_id,
            'long': long,
            'lat': lat,
            'popularity_index': popularity_index,
            'age': age,
            'employee_count': employee_count,
            'employee_count_min': employee_count_min_,
            'employee_count_max': employee_count_max_
        })
    outlets_df = pd.DataFrame.from_records(outlets_list)
    return outlets_df


# get defined menu-item
def get_menu_items():
    menu_item_list = []
    for menu_item in pd.read_csv('ddls/7.menu_item_ddl', sep=";", header=None).values:
        cur_menu_item = menu_item[0].split("VALUES")[1] \
            .replace("(", "") \
            .replace(")", "") \
            .replace(";", "") \
            .replace(" ", "") \
            .split(",")
        menu_item_id = cur_menu_item[0]
        menu_item_name = cur_menu_item[1]
        menu_item_category = cur_menu_item[2]
        price = cur_menu_item[3]

        menu_item_list.append({
            'menu_item_id': menu_item_id,
            'menu_item_name': menu_item_name,
            'menu_item_category': menu_item_category,
            'price': price
        })
    menu_item_list_df = pd.DataFrame.from_records(menu_item_list)
    return menu_item_list_df


# get defined orders
def get_orders():
    orders_list = []
    for order in pd.read_csv('ddls/12.orders_ddl.txt', sep=";", header=None).values:
        cur_order = order[0].split("VALUES")[1] \
            .replace("(", "") \
            .replace(")", "") \
            .replace(";", "") \
            .replace(" ", "") \
            .split(",")

        order_id = cur_order[0]
        outlet_id = cur_order[1]
        date_id = cur_order[2]

        orders_list.append({
            'order_id': order_id,
            'outlet_id': outlet_id,
            'date_id': date_id
        })
    order_list_df = pd.DataFrame.from_records(orders_list)
    return order_list_df


# get defined sales
def get_sales():
    sales_list = []
    sales = dd.read_csv('ddls/15.sales_ddl.txt', sep="$", header=None, engine='python')
    for sale in sales.itertuples():
        for length in range(0, sale.__len__()):
            if isinstance(sale[length], str):
                if 'INSERT' in sale[length]:
                    for query in sale[length].split(";"):
                        if 'INSERT' in query:
                            cur_sale = query.split("VALUES")[1] \
                                .replace("(", "") \
                                .replace(")", "") \
                                .replace(";", "") \
                                .replace(" ", "") \
                                .split(",")
                            date = datetime.strptime(cur_sale[5].strip('\''), '%Y-%m-%d')
                            order_id = cur_sale[0]
                            menu_item_id = cur_sale[1]
                            outlet_id = cur_sale[2]
                            employee_id = cur_sale[3]
                            customer_id = cur_sale[4]
                            date_id = date
                            month_id = str(date.year) + "-" + str("%02d" % (date.month,))
                            sales_amount = 0

                            if str(cur_sale[6].strip('\'')) != '':
                                sales_amount = float(cur_sale[6])

                            sales_list.append({
                                'order_id': order_id,
                                'menu_item_id': menu_item_id,
                                'outlet_id': outlet_id,
                                'employee_id': employee_id,
                                'customer_id': customer_id,
                                'date_id': date_id,
                                'month_id': month_id,
                                'sales_amount': sales_amount
                            })
    sales_list_df = pd.DataFrame.from_records(sales_list)
    return sales_list_df


# 0-target not meet;1-target meet;
def get_sales_target(sales_amount, default_start, default_end):
    expectation_met = random.randint(0, 2)
    if math.isnan(sales_amount):
        sales_target = random.randint(default_start, default_end)
    elif expectation_met == 0:
        sales_target = random.randint(0, int(sales_amount))
    else:
        sales_target = random.randint(int(sales_amount), int(sales_amount) + random.randint(0, 100))
    return sales_target
