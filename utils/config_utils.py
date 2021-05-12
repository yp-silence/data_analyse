"""
config parser
"""

import yaml
from utils.logger_utils import get_logger

_logg = get_logger()


def parse_config(key, path='../config.yaml'):
    node = yaml.load(open(path), Loader=yaml.SafeLoader)
    _logg.info(f'key:{key}')
    param_config = node.get(key)
    _logg.info(f'param_config:{param_config}')
    return param_config
