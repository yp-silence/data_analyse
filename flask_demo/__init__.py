from flask import Flask
from .views import admin, account

app = Flask(__name__)

app.register_blueprint(admin.ad)
app.register_blueprint(account.ac)
