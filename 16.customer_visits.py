from common import write_data_to_file, get_sales, get_sales_target


def customer_visits_scripts():
    customer_visit_ddl = []
    sales = get_sales()
    updated_sales = sales.join(
        sales.groupby(['outlet_id', 'customer_id', 'month_id'], as_index=False)['sales_amount'].sum(),
        rsuffix='_sum')
    for sale in updated_sales.values:
        outlet_id = sale[0]
        customer_id = sale[1]
        month_id = sale[2]
        customer_visit = sale[3]
        customer_visit_target = get_sales_target(sale[7], 1, 35)
        ddl = 'INSERT INTO CustomerVisits(outlet_id,customer_id,month_id,customer_visit,customer_visit_target) VALUES (' \
              + str(outlet_id) + ',' \
              + str(customer_id) + ',' \
              + str(month_id) + ',' \
              + str(customer_visit) + ',' \
              + str(customer_visit_target) + ');'
        print(ddl)
        customer_visit_ddl.append(ddl)
    write_data_to_file(customer_visit_ddl, 'ddls/16.customer_visit_ddl.txt')


if __name__ == '__main__':
    customer_visits_scripts()
