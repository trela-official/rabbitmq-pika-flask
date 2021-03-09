from flask import Flask
from .services.rabbit import rabbit
from .events import *

app = Flask(__name__)

rabbit.init_app(app, True)


@app.route('/ping', methods=['GET'])
def ping():
    rabbit.send(body='ping', routing_key='ping.message')
    return 'pong'
