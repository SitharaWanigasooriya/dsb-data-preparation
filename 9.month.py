from dateutil import relativedelta

from common import write_data_to_file, get_year, daterange, get_the_season, get_start_date, get_end_date


def dates_scripts():
    month_ddl = []
    start_dt = get_start_date()
    end_dt = get_end_date()
    current_month = 1
    for dt in daterange(start_dt, end_dt):
        year = get_year(dt)
        season = get_the_season(dt)
        if dt.month == current_month:
            next_month = dt + relativedelta.relativedelta(months=1)
            current_month = next_month.month
            ddl = 'INSERT INTO Month(month_id,year_id, season_id,month_number,month_name) VALUES ( \'' \
                  + str(dt.strftime("%Y-%m")) + '\' ,' \
                  + str(dt.strftime("%Y")) + ' , \'' \
                  + str(season) + '\', ' \
                  + str(dt.month) + ',\'' \
                  + dt.strftime("%B") + '\');'
            print(ddl)
            month_ddl.append(ddl)

    write_data_to_file(month_ddl, 'ddls/9.month_ddl.txt')


if __name__ == '__main__':
    dates_scripts()
