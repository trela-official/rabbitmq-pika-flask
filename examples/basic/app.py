from flask import Flask
from rabbitmq_pika_flask import RabbitMQ

app = Flask(__name__)

rabbit = RabbitMQ(app, use_ssl=True)


@app.route('ping', methods=['GET'])
def ping():
    rabbit.send(body='ping', routing_key='ping.message')
    return 'pong'


@rabbit.queue(routing_key='ping.message')
def ping_event(routing_key, body):
    app.logger.info('Message received:')
    app.logger.info('\tKey: {}'.format(routing_key))
    app.logger.info('\tBody: {}'.format(body))
