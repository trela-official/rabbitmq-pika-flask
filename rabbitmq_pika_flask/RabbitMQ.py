import json
import os
from enum import Enum
from functools import wraps
from hashlib import sha256
from threading import Thread
from typing import Callable
from uuid import uuid4

from flask.app import Flask
from flask.config import Config
from pika import BlockingConnection, URLParameters, spec
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError
from retry import retry
from retry.api import retry_call


class QueueParams:
    """ Default parameters for queues
    """

    durable: bool
    auto_delete: bool
    exclusive: bool

    def __init__(self, durable=True, auto_delete=False,  exclusive=False) -> None:
        self.durable = durable
        self.auto_delete = auto_delete
        self.exclusive = exclusive


class ExchangeType(Enum):
    """The type of exchange to be used
    """

    DEFAULT = 'topic'
    DIRECT = 'direct'
    FANOUT = 'fanout'
    TOPIC = 'topic'
    HEADER = 'header'


class RabbitMQ():
    """ Main class containing queue and message sending methods
    """

    app: Flask
    config: Config

    get_connection: Callable[[], BlockingConnection]
    exchange_name: str
    consumers: set

    body_parser: Callable or None
    msg_parser: Callable or None

    queue_prefix: str
    queue_params: QueueParams

    def __init__(
        self,
        app: Flask = None,
        queue_prefix: str = '',
        body_parser: Callable = None,
        msg_parser: Callable = None,
        queue_params: QueueParams = QueueParams(),
        development: bool = False
    ) -> None:
        self.app = None
        self.consumers = set()
        self.queue_params = queue_params

        if app is not None:
            self.init_app(app, queue_prefix, body_parser,
                          msg_parser, development)

    # Inits class from flask app
    def init_app(
        self,
        app: Flask,
        queue_prefix: str,
        body_parser: Callable = None,
        msg_parser: Callable = None,
        development: bool = False
    ):
        """This callback can be used to initialize an application for the use with this RabbitMQ setup.

        Args:
            app (Flask): Flask app
            body_parser (Callable, optional): A parser function to
                parse received messages. Defaults to None.
            msg_parser (Callable, optional): A parser function to
                parse messages to be sent. Defaults to None.
        """

        self._check_env()

        self.app = app
        self.queue_prefix = queue_prefix
        self.config = app.config
        self.body_parser = body_parser
        self.msg_parser = msg_parser
        self.development = development
        self.exchange_name = self.config['MQ_EXCHANGE']
        params = URLParameters(self.config['MQ_URL'])
        self.get_connection = lambda: BlockingConnection(params)

        if os.getenv('FLASK_ENV') == 'production' or os.getenv('WERKZEUG_RUN_MAIN') == 'true':
            self._validate_connection()

        if development:
            self.queue_prefix = 'dev.' + str(uuid4()) + queue_prefix
            self.queue_params = QueueParams(False, True, True)

        # Run every consumer queue
        for consumer in self.consumers:
            consumer()

    def _validate_connection(self):
        try:
            connection = self.get_connection()
            if connection.is_open:
                self.app.logger.info('Connected to RabbitMQ')
                connection.close()
        except Exception as error:  # pylint: disable=broad-except
            self.app.logger.error('Invalid RabbitMQ connection')
            self.app.logger.error(error.__class__.__name__)

    def _check_env(self):
        """assert env variables are set
        """

        assert os.getenv(
            'FLASK_ENV') is not None, 'No FLASK_ENV variable found. Add one such as "production" or "development"'

        assert os.getenv(
            'MQ_URL') is not None, 'No MQ_URL variable found. Please add one following this' + \
            ' format https://pika.readthedocs.io/en/stable/examples/using_urlparameters.html'

        assert os.getenv(
            'MQ_EXCHANGE') is not None, 'No MQ_EXCHANGE variable found. Please add a default exchange'

    def queue(
        self,
        routing_key: str,
        exchange_type: ExchangeType = ExchangeType.DEFAULT,
        auto_ack: bool = False,
        dead_letter_exchange: bool = False
    ):
        """Creates new RabbitMQ queue

        Args:
            routing_key (str): The routing key for this queue
            exchange_type (ExchangeType, optional): The exchange type to be used. Defaults to TOPIC.
            auto_ack (bool, optional): If messages should be auto acknowledged. Defaults to False
            dead_letter_exchange (bool): If a dead letter exchange should be created for this queue
        """

        def decorator(f):
            # ignore flask default reload when on debug mode
            if os.getenv('FLASK_ENV') == 'production' or os.getenv('WERKZEUG_RUN_MAIN') == 'true':
                @wraps(f)
                def new_consumer():

                    return self._setup_connection(f, routing_key, exchange_type, auto_ack, dead_letter_exchange)

                # adds consumer to consumers list if not initiated, or runs new consumer if already initiated
                if self.app is not None:
                    new_consumer()
                else:
                    self.consumers.add(new_consumer)
            return f
        return decorator

    def _setup_connection(
        self,
        func: Callable,
        routing_key: str,
        exchange_type: ExchangeType,
        auto_ack: bool,
        dead_letter_exchange: bool
    ):
        """Setup new queue connection in a new thread

        Args:
            func (Callable): function to run as callback for a new message
            routing_key (str): routing key for the new queue bind
            exchange_type (ExchangeType): Exchange type to be used with new queue
            auto_ack (bool): If messages should be auto acknowledged.
            dead_letter_exchange (bool): If a dead letter exchange should be created for this queue
        """

        def create_queue():
            return self._add_exchange_queue(func, routing_key, exchange_type, auto_ack, dead_letter_exchange)

        thread = Thread(target=create_queue)
        thread.setDaemon(True)
        thread.start()

    @retry((AMQPConnectionError, AssertionError), delay=5, jitter=(5, 15))
    def _add_exchange_queue(
        self,
        func: Callable,
        routing_key: str,
        exchange_type: ExchangeType,
        auto_ack: bool,
        dead_letter_exchange: bool
    ):
        """ Creates or connects to new queue, retries connection on failure

        Args:
            func (Callable): function to run as callback for a new message
            routing_key (str): routing key for the new queue bind
            exchange_type (ExchangeType): Exchange type to be used with new queue
            auto_ack (bool): If messages should be auto acknowledged.
            dead_letter_exchange (bool): If a dead letter exchange should be created for this queue
        """

        # Create connection channel
        connection = self.get_connection()
        channel = connection.channel()

        # declare dead letter exchange if needed
        if dead_letter_exchange:
            dead_letter_exchange_name = f'dead.letter.{self.exchange_name}'
            channel.exchange_declare(dead_letter_exchange_name, 'direct')

        # Declare exchange
        channel.exchange_declare(
            exchange=self.exchange_name, exchange_type=exchange_type)

        # Creates new queue or connects to existing one
        queue_name = self.queue_prefix + '.' + func.__name__.replace('_', '.')
        exchange_args = {}
        if dead_letter_exchange and not self.development:
            dead_letter_queue_name = f'dead.letter.{queue_name}'
            channel.queue_declare(
                dead_letter_queue_name,
                durable=self.queue_params.durable,
            )

            # Bind queue to exchange
            channel.queue_bind(
                exchange=dead_letter_exchange_name, queue=dead_letter_queue_name, routing_key=routing_key)

            exchange_args = {
                'x-dead-letter-exchange': dead_letter_exchange_name,
                'x-dead-letter-routing-key': routing_key
            }

        channel.queue_declare(
            queue_name,
            durable=self.queue_params.durable,
            auto_delete=self.queue_params.auto_delete,
            exclusive=self.queue_params.exclusive,
            arguments=exchange_args
        )
        self.app.logger.info(f'Declaring Queue: {queue_name}')

        # Bind queue to exchange
        channel.queue_bind(exchange=self.exchange_name,
                           queue=queue_name, routing_key=routing_key)

        def callback(_: BlockingChannel, method: spec.Basic.Deliver, props: spec.BasicProperties, body: bytes):
            with self.app.app_context():
                decoded_body = body.decode()

                try:
                    func(
                        routing_key=method.routing_key,
                        body=self.body_parser(
                            decoded_body) if self.body_parser is not None else decoded_body,
                        message_id=props.message_id
                    )

                    if not auto_ack:
                        # ack message after fn was ran
                        channel.basic_ack(method.delivery_tag)
                except Exception as err:
                    if not auto_ack:
                        channel.basic_reject(
                            method.delivery_tag, requeue=(not method.redelivered))

                    raise err from err

        channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=auto_ack)

        try:
            channel.start_consuming()
        except Exception as err:
            self.app.logger.error(err)
            channel.stop_consuming()
            connection.close()

            raise AMQPConnectionError from err

    def _send_msg(self, body, routing_key, exchange_type):
        try:
            channel = self.get_connection().channel()

            channel.exchange_declare(
                exchange=self.exchange_name, exchange_type=exchange_type)

            if self.msg_parser:
                body = self.msg_parser(body)

            channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=body,
                properties=spec.BasicProperties(
                    message_id=sha256(json.dumps(body).encode('utf-8')).hexdigest())
            )

            channel.close()
        except Exception as err:
            self.app.logger.error('Error while sending message')
            self.app.logger.error(err)

            raise AMQPConnectionError from err

    def send(self, body, routing_key: str, exchange_type: ExchangeType = ExchangeType.DEFAULT, retries: int = 5):
        """Sends a message to a given routing key

        Args:
            body (str): The body to be sent
            routing_key (str): The routing key for the message
            exchange_type (ExchangeType, optional): The exchange type to be used. Defaults to ExchangeType.DEFAULT.
        """

        thread = Thread(target=lambda: retry_call(
            self._send_msg,
            (body, routing_key, exchange_type),
            exceptions=(
                AMQPConnectionError, AssertionError),
            tries=retries,
            delay=5,
            jitter=(5, 15)
        ))
        thread.setDaemon(True)
        thread.start()
