import ssl
from threading import Thread
from flask.app import Flask
from flask.config import Config
from pika import BlockingConnection, ConnectionParameters
from pika.adapters.blocking_connection import BlockingChannel
from pika.connection import SSLOptions
from pika.credentials import PlainCredentials
from functools import update_wrapper
from typing import Callable


def setup_method(f):
    def wrapper_func(self, *args, **kwargs):
        return f(self, *args, **kwargs)

    return update_wrapper(wrapper_func, f)


class ExchangeType(object):
    DEFAULT = 'topic'
    DIRECT = 'direct'
    FANOUT = 'fanout'
    TOPIC = 'topic'
    HEADER = 'header'


class RabbitMQ():

    app: Flask
    config: Config
    getConnection: Callable[[], BlockingConnection]
    exchange_name: str
    consumers: set

    def __init__(self, app: Flask = None) -> None:
        self.consumers = set()

        if app is not None:
            self.init_app(app)

    # Inits class from flask app
    def init_app(self, app: Flask):
        self.app = app
        self.config = app.config
        self.exchange_name = app.config.get('MQ_EXCHANGE')
        self.getConnection = lambda: BlockingConnection(ConnectionParameters(
            host=app.config.get('MQ_HOST'),
            port=app.config.get('MQ_PORT'),
            credentials=PlainCredentials(
                username=app.config.get('MQ_USER'),
                password=app.config.get('MQ_PASS')
            ),
            ssl_options=SSLOptions(ssl.SSLContext(ssl.PROTOCOL_TLSv1_2))
        ))

        # Run every consumer queue
        for consumer in self.consumers:
            consumer()

    # Adds queue functionality to a method
    def queue(self, routing_key: str, queue_name: str = None, exchange_type: ExchangeType = ExchangeType.DEFAULT):

        def decorator(f):
            def new_consumer(): return self.add_exchange_queue(f, queue_name=queue_name, exchange_type=exchange_type,
                                                               routing_key=routing_key)
            self.consumers.add(new_consumer)

            return f

        return decorator

    # Add exchange queue to method
    @setup_method
    def add_exchange_queue(self, func: Callable,  routing_key: str, queue_name: str, exchange_type: ExchangeType):

        # Create connection channel
        channel = self.getConnection().channel()

        # Declare exchange
        channel.exchange_declare(
            exchange=self.exchange_name, exchange_type=exchange_type)

        # Create new queue
        queue_name = func.__name__
        channel.queue_declare(queue_name)

        # Bind queue to exchange
        channel.queue_bind(exchange=self.exchange_name,
                           queue=queue_name, routing_key=routing_key)

        def callback(_ch, method, _routing, body):
            with self.app.app_context():
                func(routing_key=method.routing_key, body=body.decode())

        channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=True)

        thread = Thread(target=channel.start_consuming)
        thread.setDaemon(True)
        thread.start()

    # Send message to exchange

    def send(self, body: str, routing_key: str, exchange_type: ExchangeType = ExchangeType.DEFAULT):
        channel = self.getConnection().channel()

        channel.exchange_declare(
            exchange=self.exchange_name, exchange_type=exchange_type)

        channel.basic_publish(exchange=self.exchange_name,
                              routing_key=routing_key, body=body)
        channel.close()

    def ack_message(channel: BlockingChannel, delivery_tag):
        if channel.is_open:
            channel.basic_ack(delivery_tag)
        else:
            pass
