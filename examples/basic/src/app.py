from flask import Flask

from src.events import *
from src.services.rabbit import rabbit


def create_app():
    app = Flask(__name__)

    rabbit.init_app(app, True)

    @app.route('/ping', methods=['GET'])
    def _():
        rabbit.send(body='ping', routing_key='ping.message')
        return 'pong'

    return app
