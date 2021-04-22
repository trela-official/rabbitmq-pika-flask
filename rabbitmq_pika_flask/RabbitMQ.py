import os
import ssl
from functools import update_wrapper
from threading import Thread
from typing import Callable
from uuid import uuid4

from flask.app import Flask
from flask.config import Config
from pika import BlockingConnection, ConnectionParameters
from pika.connection import SSLOptions
from pika.credentials import PlainCredentials


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
    get_connection: Callable[[], BlockingConnection]
    exchange_name: str
    consumers: set
    body_parser: Callable or None
    msg_parser: Callable or None

    def __init__(self, app: Flask = None, use_ssl: bool = False, body_parser: Callable = None, msg_parser: Callable = None) -> None:
        self.app = None
        self.consumers = set()

        if app is not None:
            self.init_app(app, use_ssl, body_parser, msg_parser)

    # Inits class from flask app
    def init_app(self, app: Flask, use_ssl: bool = False, body_parser: Callable = None, msg_parser: Callable = None):
        """This callback can be used to initialize an application for the use with this RabbitMQ setup.

        Args:
            app (Flask): Flask app
            use_ssl (bool, optional): If the connection should use SSL. Defaults to False.
            body_parser (Callable, optional): A parser function to parse received messages. Defaults to None.
            msg_parser (Callable, optional): A parser function to parse messages to be sent. Defaults to None.
        """

        self.app = app
        self.config = app.config
        self.exchange_name = os.getenv('MQ_EXCHANGE')
        self.body_parser = body_parser
        self.msg_parser = msg_parser
        self.get_connection = lambda: BlockingConnection(ConnectionParameters(
            host=os.getenv('MQ_HOST'),
            port=os.getenv('MQ_PORT'),
            credentials=PlainCredentials(
                username=os.getenv('MQ_USER'),
                password=os.getenv('MQ_PASS')
            ),
            ssl_options=SSLOptions(ssl.SSLContext(
                ssl.PROTOCOL_TLSv1_2)) if use_ssl else None,
            heartbeat=300,
            blocked_connection_timeout=150
        ))

        # Run every consumer queue
        for consumer in self.consumers:
            consumer()

    # Adds queue functionality to a method
    def queue(self, routing_key: str, queue_name: str = None, exchange_type: ExchangeType = ExchangeType.DEFAULT):
        """Creates new RabbitMQ queue

        Args:
            routing_key (str): The routing key for this queue
            queue_name (str, optional): The queue name, if none is sent, the function name will be used. Defaults to None.
            exchange_type (ExchangeType, optional): The exchange type to be used. Defaults to ExchangeType.DEFAULT.

        """

        def decorator(f):
            # ignore flask default reload when on debug mode
            if self.app and (not self.app.debug or os.getenv('WERKZEUG_RUN_MAIN') == 'true'):
                def new_consumer():
                    return self.add_exchange_queue(
                        f, queue_name=queue_name,
                        exchange_type=exchange_type,
                        routing_key=routing_key
                    )
                self.consumers.add(new_consumer)

            return f

        return decorator

    # Add exchange queue to method
    @setup_method
    def add_exchange_queue(self, func: Callable,  routing_key: str, queue_name: str, exchange_type: ExchangeType):

        # Create connection channel
        channel = self.get_connection().channel()

        # Declare exchange
        channel.exchange_declare(
            exchange=self.exchange_name, exchange_type=exchange_type)

        # Create new queue
        queue_name = self.exchange_name.lower() + '_' + (queue_name or func.__name__) + \
            '_' + str(uuid4())
        channel.queue_declare(queue_name, durable=True)

        # Bind queue to exchange
        channel.queue_bind(exchange=self.exchange_name,
                           queue=queue_name, routing_key=routing_key)

        def callback(_ch, method, _routing, body):
            with self.app.app_context():
                if self.body_parser is not None:
                    func(routing_key=method.routing_key,
                         body=self.body_parser(body.decode()))
                else:
                    func(routing_key=method.routing_key,
                         body=body.decode())

        channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=True)

        thread = Thread(target=channel.start_consuming)
        thread.setDaemon(True)
        thread.start()

    # Send message to exchange

    def send(self, body, routing_key: str, exchange_type: ExchangeType = ExchangeType.DEFAULT):
        """Sends a message to a given routing key 

        Args:
            body (str): The body to be sent
            routing_key (str): The routing key for the message
            exchange_type (ExchangeType, optional): The exchange type to be used. Defaults to ExchangeType.DEFAULT.
        """

        channel = self.get_connection().channel()

        channel.exchange_declare(
            exchange=self.exchange_name, exchange_type=exchange_type)

        if self.msg_parser:
            body = self.msg_parser(body)

        channel.basic_publish(exchange=self.exchange_name,
                              routing_key=routing_key, body=body)
        channel.close()
