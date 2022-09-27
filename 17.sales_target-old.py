from common import write_data_to_file, get_sales


def sales_target_scripts():
    sales_target_ddl = []
    sales = get_sales()
    visit_df = sales[['outlet_id', 'menu_item_id', 'month_id']]
    visit_df['sales_count'] = visit_df.groupby(['outlet_id', 'menu_item_id', 'month_id']).transform('count')
    visit_df.drop_duplicates(keep=False)
    for visit in visit_df.values:
        outlet_id = visit[0]
        menu_item_id = visit[1]
        month_id = visit[2]
        sale = visit[3]
        sales_target = 10
        ddl = 'INSERT INTO SalesTargets(outlet_id,menu_item_id,month_id,sale,sales_target) VALUES (' \
              + str(outlet_id) + ',' \
              + str(menu_item_id) + ',\'' \
              + str(month_id) + '\',' \
              + str(sale) + ',' \
              + str(sales_target) + ');'
        print(ddl)
        sales_target_ddl.append(ddl)
    write_data_to_file(sales_target_ddl, 'ddls/17.sales_target_ddl.txt')


def get_sales_target():
    return 100.0


if __name__ == '__main__':
    sales_target_scripts()
