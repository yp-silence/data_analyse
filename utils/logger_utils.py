import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s_%(levelname)s_[%(filename)s:%(lineno)d]: %(message)s")
logger = logging.getLogger(__name__)


def get_logger():
    return logger


def hello():
    print('hello')


if __name__ == '__main__':
    from utils import time_utils
    print(getattr(time_utils, 'get_current_date_str')())
