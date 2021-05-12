import pymysql
import yaml
import pandas as pd
from utils.logger_utils import get_logger

logger = get_logger()


def _get_db_config(path='../config.yaml', is_md=False, key=None):
    if not key:
        key = 'md_config' if is_md else "db_config"
    data_dict = yaml.load(open(path), Loader=yaml.SafeLoader).get(key)
    logger.info(f'data_dict:{data_dict}')
    return data_dict


def _get_connect(is_md, key=None):
    connect_params = _get_db_config(is_md=is_md, key=key)
    connect = pymysql.Connection(**connect_params)
    cursor = connect.cursor(cursor=pymysql.cursors.DictCursor)
    return connect, cursor


def fetch_data(sql_str: str, use_df: bool = True, is_md: bool = False, key=None):
    logger.info(f'query sql: {sql_str}')
    connect, cursor = _get_connect(is_md=is_md, key=key)
    af_rows = cursor.execute(sql_str)
    logger.info(f'total fetch {af_rows} 条记录~')
    if use_df:
        df = pd.DataFrame(cursor.fetchall())
        df.fillna(0, inplace=True)
        logger.info(f'df:{df.head()}')
        cursor.close()
        connect.close()
        return df
    else:
        data_dict = cursor.fetchall()
        logger.info(f'data_dict:{data_dict}')
        cursor.close()
        connect.close()
        return data_dict


if __name__ == '__main__':
    sql = """
    select * from usr_account limit 10;
    """
    fetch_data(sql)
