"""
定时任务工具类
"""
import sys

sys.path.append('../')
from types import FunctionType
import re
import importlib
import click

from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from utils.config_utils import parse_config
from utils.logger_utils import get_logger

logger = get_logger()
scheduler = BlockingScheduler()
url = 'sqlite:///jobs.sqlite'
scheduler.add_jobstore('sqlalchemy', url=url)


def parse_task_and_run(package='daily_report'):
    """"""
    task_list = parse_config('scheduler_config', path='../task_config.yaml')
    for key, value in task_list.items():
        # 动态导入包
        base = importlib.import_module(key, package=package)
        trigger = value[0]
        # 方法名
        method = value[2]
        # 运行的时间参数
        params = value[1]
        func = getattr(base, method)
        _add_task(func, trigger, params)


def _add_task(func, trigger, params, use_self=False):
    is_valid = __check_params(func, trigger, params, use_self=use_self)
    if is_valid:
        if trigger == 'cron':
            # 使用自定义的方法解析cron 参数
            if use_self:
                trigger_args = __parse_cron_to_dict(params)
                scheduler.add_job(func, trigger='cron', **trigger_args)
            else:
                # 使用内置的cron 表达式来实现
                scheduler.add_job(func, CronTrigger.from_crontab(params))
        elif trigger == 'interval':
            scheduler.add_job(func, IntervalTrigger(**params))
        else:
            # date 类型
            scheduler.add_job(func, DateTrigger(params))
    else:
        logger.info(f'{func.__name__} skipp ...')


def __parse_cron_to_dict(cron_exp):
    """
    A B C D E F" --> "秒 分 时 日 月 星期"
    support fields
    year  month  day week day_of_week hour minute day_of
    ps: 周的表达式  范围0-6 或者 mon,tue,wed,thu,fri,sat,sun
    :param cron_exp:
    :return:
    """
    support_keys = ['second', 'minute', 'hour', 'day', 'month', 'week']
    cron_dict = {}
    pms = cron_exp.split()
    for k, v in zip(support_keys, pms):
        if v != '*':
            cron_dict[k] = v
    return cron_dict


def __check_params(func, trigger, params, use_self):
    """
    校验参数使用有效
    :param func:
    :param trigger:
    :param params:
    :param use_self:
    :return:
    """
    is_valid = True
    # func 为函数类型
    if not isinstance(func, FunctionType):
        logger.error(f'{func.__name__} is not function type')
        is_valid = False
    elif trigger not in ('interval', 'cron', 'date'):
        logger.error(f'{trigger} type is invalid only support')
        is_valid = False
    else:
        if trigger == 'cron' and isinstance(params, str):
            # 简单校验 cron 字符串表达式格式是否正确
            pattern = r'^(\*|\d{1,2}| |,|-|/)+$'
            format_flag = bool(re.match(pattern, params))
            base_len = 6 if use_self else 5
            length_flag = len(params.split()) == base_len
            if not (format_flag and length_flag):
                logger.error(f'invalid cron expr:{params}')
                is_valid = False
        elif trigger == 'interval':
            # weeks=0, days=0, hours=0, minutes=0, seconds=0, start_date=None, end_date=None
            support_keys = ('weeks', 'days', 'hours', 'minutes', 'seconds', 'start_date', 'end_date')
            logger.info(f'params:{params}')
            # type check
            type_flag = isinstance(params, dict)
            key_flag = True
            for key in params:
                if key not in support_keys:
                    logger.info(f'{key} not in support keys')
                    key_flag = False
                    break
            if not (type_flag and key_flag):
                is_valid = False
        else:
            # date 类型不做参数校验 默认为 True
            pass
    return is_valid


def run_task():
    parse_task_and_run()
    scheduler.start()


def stop_task():
    # scheduler state == 0 表示任务停止状态
    logger.info(f'current scheduler state:{scheduler.state}')
    if scheduler.state != 0:
        scheduler.shutdown()
    else:
        logger.info(f'not running task skip...')


@click.command()
# '--count' default 默认值 help 提示信息
@click.option('--op_type', default='start', help='操作类型: start or stop ', type=click.Choice(['start', 'stop']))
def run(op_type):
    if op_type == 'start':
        run_task()
    elif op_type == 'stop':
        stop_task()


if __name__ == '__main__':
    run()
