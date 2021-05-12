class BaseConfig(object):
    DEBUG = True
    SECRET_KEY = '$$&YP@**^^'


class ProductionConfig(BaseConfig):
    """线上环境"""
    DEBUG = False


class DevelopmentConfig(BaseConfig):
    """开发环境"""
    pass


class TestConfig(BaseConfig):
    """测试环境"""
    pass
