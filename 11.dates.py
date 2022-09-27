from common import write_data_to_file, daterange, get_end_date, get_start_date


def dates_scripts():
    dates_ddl = []
    start_dt = get_start_date()
    end_dt = get_end_date()
    date_id = 1
    for dt in daterange(start_dt, end_dt):
        isWeekday = 1
        if dt.weekday() > 4:
            isWeekday = 0
        ddl = 'INSERT INTO Date(' \
              'date_id,' \
              'month_id,' \
              'year_id,' \
              'weekday,' \
              'day_number,' \
              'day_name) VALUES (\'' \
              + str(dt.strftime("%Y-%m-%d")) + '\', \'' \
              + str(dt.strftime("%Y-%m")) + '\' ,' \
              + str(dt.strftime("%Y")) + ',' \
              + str(isWeekday) + ',' \
              + str(dt.day) + ', \'' \
              + str(dt.strftime("%A")) + '\');'
        date_id = date_id + 1
        print(ddl)
        dates_ddl.append(ddl)
    write_data_to_file(dates_ddl, 'ddls/11.dates_ddl.txt')


if __name__ == '__main__':
    dates_scripts()
