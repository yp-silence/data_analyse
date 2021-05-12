from flask import Blueprint, session, redirect

# url_prefix url 前缀
ad = Blueprint('ad', __name__, url_prefix='/admin')


@ad.before_request
def before_request():
    """验证用户是否登陆了 登陆后才可以访问"""
    user = session.get('user_info')
    if not user:
        redirect('/account/login')
    else:
        return None


@ad.route('/home')
def home():
    return 'home page'


@ad.route('/detail')
def detail():
    return 'detail page'
