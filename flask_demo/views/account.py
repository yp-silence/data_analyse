from flask import Blueprint, render_template

ac = Blueprint('ac', __name__, url_prefix='/account')


@ac.route('/login')
def login_in():
    return '用户登陆'


@ac.route('/loginout')
def login_out():
    render_template('login.html')
