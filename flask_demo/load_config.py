from flask import Flask,redirect, request, session

app = Flask(__name__)
app.secret_key = 'rixsyw-sopxo2-bedniG'
app.config.from_object('settings.TestConfig')


# 做用户登陆校验
@app.before_request
def login_check():
    if request.path == '/login':
        return None
    # 获取用户登陆信息 从 session中
    user = session.get('user_info')
    if not user:
        return redirect('/login')


@app.route('/login', methods=['POST','GET'])
def index():
    return '登陆视图'


@app.after_request
def after_request(response):
    print('view 执行后 执行')
    return response


if __name__ == '__main__':
    app.run()
