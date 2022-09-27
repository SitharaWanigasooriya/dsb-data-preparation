from common import write_data_to_file, get_sales, get_sales_target


def sales_target_scripts():
    sales_target_ddl = []
    sales = get_sales()
    updated_sales = sales.join(
        sales.groupby(['outlet_id', 'menu_item_id', 'month_id'], as_index=False)['sales_amount'].sum(),
        rsuffix='_sum')
    for sale in updated_sales.values:
        order_id = sale[0]
        menu_item_id = sale[1]
        outlet_id = sale[2]
        employee_id = sale[3]
        customer_id = sale[4]
        date_id = sale[5]
        month_id = sale[6]
        sales_amount = sale[7]
        sales_target = get_sales_target(sale[11])
        ddl = 'INSERT INTO SalesTargets(outlet_id,menu_item_id,month_id,sales_target) VALUES (' \
              + str(outlet_id) + ',' \
              + str(menu_item_id) + ',\'' \
              + str(month_id) + '\',' \
              + str(sales_target) + ');'
        print(ddl)
        sales_target_ddl.append(ddl)
    write_data_to_file(sales_target_ddl, 'ddls/17.sales_target_ddl.txt')


if __name__ == '__main__':
    sales_target_scripts()
