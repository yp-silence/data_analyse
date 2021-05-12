"""
时间操作工具类
"""

import time
import datetime
from datetime import date


from utils.logger_utils import get_logger

logg = get_logger()


def get_begin_of_month(data_str=None, ft_str='%Y-%m-%d'):
    if not data_str:
        data_str = get_current_date_str(ft_str)[0:8]
    else:
        data_str = data_str[0:8]
    return data_str + '01'


def get_current_date_str(ft_str='%Y-%m-%d'):
    """
    获取当前日期字符串
    :return:
    """
    date_str = time.strftime(ft_str, time.localtime())
    logg.info(f'date_str:{date_str}')
    return date_str


def get_current_time_str(ft_str='%H:%M:%S'):
    """
    获取当前时间字符串
    :return:
    """
    time_str = time.strftime(ft_str, time.localtime())
    logg.info(f'time_str:{time_str}')
    return time_str


def date_add(start_day, days, ft='%Y-%m-%d', to_str=True):
    p_time = time.strptime(start_day, ft)
    time_stamp = time.mktime(p_time)
    shift_date = date.fromtimestamp(time_stamp) + datetime.timedelta(days=days)
    return str(shift_date) if to_str else shift_date


def get_week_day(date_str=None, ft='%Y-%m-%d', end_current=False):
    """
    获取指定日期 所在周 周一 和 周日的日期 如果不指定 日期 默认是今天所在的日期
    start_date 周一所在的日期
    end_date   周日所在的日期
    """
    if date_str:
        # 获取日期所属的星期 0 - 6
        week = time.strptime(date_str, ft).tm_wday
    else:
        week = time.localtime().tm_wday
        date_str = get_current_date_str()
    start_date = date_add(date_str, 0 - week, to_str=True)
    if end_current:
        end_date = date_str
    else:
        end_date = date_add(date_str, 6 - week, to_str=True)
    return start_date, end_date


def run():
    get_current_time_str()
    get_current_date_str()


if __name__ == '__main__':
    print(get_week_day())
    print(get_current_time_str())
