import inspect
import itertools
import json
import os
from datetime import datetime
from enum import Enum, auto
from functools import wraps
from hashlib import sha256
from threading import Thread
from typing import Any, Callable, List, Union
from uuid import uuid4

from flask.app import Flask
from flask.config import Config
from pika import BlockingConnection, URLParameters, spec
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError
from retry import retry
from retry.api import retry_call

from rabbitmq_pika_flask.ExchangeType import ExchangeType
from rabbitmq_pika_flask.ExchangeParams import ExchangeParams
from rabbitmq_pika_flask.QueueParams import QueueParams
from rabbitmq_pika_flask.RabbitConsumerMiddleware import (
    RabbitConsumerMessage,
    RabbitConsumerMiddleware,
    call_middlewares,
)

# (queue_name, dlq_name, method, props, body, exception)
MessageErrorCallback = Callable[
    [str, Union[str, None], spec.Basic.Deliver, spec.BasicProperties, str, Exception], Any
]


class OptionalProps(Enum):
    """Props that can be optionally passed to functions decorated by @queue."""

    message_id = auto()
    sent_at = auto()
    message_version = auto()


class RabbitMQ:
    """Main class containing queue and message sending methods"""

    app: Flask
    config: Config

    get_connection: Callable[[], BlockingConnection]
    consumers: set
    development: bool or None

    body_parser: Callable or None
    msg_parser: Callable or None

    exchange_name: str
    exchange_params: ExchangeParams

    queue_prefix: str
    queue_params: QueueParams

    on_message_error_callback: Union[MessageErrorCallback, None]
    middlewares: List[RabbitConsumerMiddleware]

    def __init__(
        self,
        app: Flask = None,
        queue_prefix: str = "",
        body_parser: Callable = None,
        msg_parser: Callable = None,
        queue_params: QueueParams = QueueParams(),
        development: bool = None,
        on_message_error_callback: Union[MessageErrorCallback, None] = None,
        middlewares: Union[List[RabbitConsumerMiddleware], None] = None,
        exchange_params: ExchangeParams = ExchangeParams(),
    ) -> None:
        self.app = None
        self.consumers = set()
        self.exchange_params = exchange_params
        self.queue_params = queue_params
        self.middlewares = middlewares or []

        if app is not None:
            self.init_app(
                app,
                queue_prefix,
                body_parser,
                msg_parser,
                development,
                on_message_error_callback,
            )

    # Inits class from flask app
    def init_app(
        self,
        app: Flask,
        queue_prefix: str,
        body_parser: Callable = lambda body: body,
        msg_parser: Callable = lambda msg: msg,
        development: bool = None,
        on_message_error_callback: Union[MessageErrorCallback, None] = None,
        middlewares: Union[List[RabbitConsumerMiddleware], None] = None,
    ):
        """This callback can be used to initialize an application for the use with this RabbitMQ setup.

        Args:
            app (Flask): Flask app
            queue_prefix (str): Prefix for queue names
            body_parser (Callable, optional): A parser function to
                parse received messages. Defaults to None.
            msg_parser (Callable, optional): A parser function to
                parse messages to be sent. Defaults to None.
            development (bool, optional): Overrides development mode checks. Defaults to None, which causes
                development status to be checked using Flask builtin variables.
            on_message_error_callback (Callable, optional): Function that's called when the processing of
                a message fails due to an exception.
            middlewares: List of callables that are called, in order, to process a rabbitmq
                message received from the queue, before finally calling the user consumer func.
        """

        self.app = app
        self.config = app.config

        self.queue_prefix = queue_prefix
        self.body_parser = body_parser
        self.msg_parser = msg_parser
        self.on_message_error_callback = on_message_error_callback
        self.middlewares.extend(middlewares or [])

        self.exchange_name = self.config.get("MQ_EXCHANGE") or os.getenv("MQ_EXCHANGE")
        assert (
            self.exchange_name,
            "MQ_EXCHANGE not set. Please define a default exchange name.",
        )
        mq_url = self.config.get("MQ_URL") or os.getenv("MQ_URL")
        assert (
            mq_url,
            "MQ_URL not set. Please define the RabbitMQ url using this format: https://pika.readthedocs.io/en/stable/examples/using_urlparameters.html",
        )
        self.development = (
            development
            if development is not None
            else (
                self.config.get("ENV") == "development"
                or os.getenv("FLASK_ENV") == "development"
                or self.config.get("DEBUG") == "1"
                or os.getenv("FLASK_DEBUG") == "1"
            )
        )

        params = URLParameters(mq_url)
        self.get_connection = lambda: BlockingConnection(params)

        if self.development:
            self.queue_prefix = "dev." + str(uuid4()) + queue_prefix
            self.queue_params = QueueParams(False, True, True)
            self.exchange_params = ExchangeParams(False, True, False)
        else:
            # Avoiding running twice when flask in debug mode
            self._validate_connection()

        # Run every consumer queue
        for consumer in self.consumers:
            consumer()

    def _validate_connection(self):
        try:
            connection = self.get_connection()
            if connection.is_open:
                self.app.logger.info("Connected to RabbitMQ")
                connection.close()
        except Exception as error:  # pylint: disable=broad-except
            self.app.logger.error("Invalid RabbitMQ connection")
            self.app.logger.error(error.__class__.__name__)

    def _build_queue_name(self, func: Callable):
        """Builds queue name from function name"""
        spacer = self.config["MQ_DELIMITER"] if "MQ_DELIMITER" in self.config else "."
        return self.queue_prefix + spacer + func.__name__.replace("_", spacer)

    def queue(
        self,
        routing_key: Union[str, List[str]],
        exchange_type: ExchangeType = ExchangeType.DEFAULT,
        auto_ack: bool = False,
        dead_letter_exchange: bool = False,
        props_needed: List[str] = None,
    ):
        """Creates new RabbitMQ queue

        Args:
            routing_key (str | list[str]): The routing key(s) for this queue
            exchange_type (ExchangeType, optional): The exchange type to be used. Defaults to TOPIC.
            auto_ack (bool, optional): If messages should be auto acknowledged. Defaults to False
            dead_letter_exchange (bool): If a dead letter exchange should be created for this queue
            props_needed (list[str], optional): List of properties to be passed along with body, such as `sent_at` or `message_id`. Defaults to None.
        """

        def decorator(f):
            # ignore flask default reload when on debug mode
            if not self.development:
                nonlocal props_needed
                if props_needed is None:
                    f_signature = inspect.signature(f).parameters
                    props_needed = [
                        prop.name for prop in OptionalProps if prop.name in f_signature
                    ]

                @wraps(f)
                def new_consumer():

                    return self._setup_connection(
                        f,
                        routing_key,
                        exchange_type,
                        auto_ack,
                        dead_letter_exchange,
                        props_needed or [],
                    )

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
        routing_key: Union[str, List[str]],
        exchange_type: ExchangeType,
        auto_ack: bool,
        dead_letter_exchange: bool,
        props_needed: List[str],
    ):
        """Setup new queue connection in a new thread

        Args:
            func (Callable): function to run as callback for a new message
            routing_key (str | list[str]): routing key(s) for the new queue bind
            exchange_type (ExchangeType): Exchange type to be used with new queue
            auto_ack (bool): If messages should be auto acknowledged.
            dead_letter_exchange (bool): If a dead letter exchange should be created for this queue
            props_needed (list[str]): List of properties to be passed along with body
        """

        def create_queue():
            return self._add_exchange_queue(
                func,
                routing_key,
                exchange_type,
                auto_ack,
                dead_letter_exchange,
                props_needed,
            )

        thread = Thread(target=create_queue, name=self._build_queue_name(func))
        thread.daemon = True
        thread.start()

    @staticmethod
    def __get_needed_props(props_needed: List[str], props: spec.BasicProperties):
        """Sets needed properties for a message"""

        payload = {}

        if "message_id" in props_needed:
            payload["message_id"] = props.message_id

        if "sent_at" in props_needed:
            payload["sent_at"] = datetime.fromtimestamp(props.timestamp)

        if "message_version" in props_needed:
            payload["message_version"] = props.headers.get("x-message-version")

        return payload

    @retry((AMQPConnectionError, AssertionError), delay=5, jitter=(5, 15))
    def _add_exchange_queue(
        self,
        func: Callable,
        routing_key: Union[str, List[str]],
        exchange_type: ExchangeType,
        auto_ack: bool,
        dead_letter_exchange: bool,
        props_needed: List[str],
    ):
        """Creates or connects to new queue, retries connection on failure

        Args:
            func (Callable): function to run as callback for a new message
            routing_key (str | list[str]): routing key(s) for the new queue bind
            exchange_type (ExchangeType): Exchange type to be used with new queue
            auto_ack (bool): If messages should be auto acknowledged.
            dead_letter_exchange (bool): If a dead letter exchange should be created for this queue
            props_needed (list[str]): List of properties to be passed along with body
        """

        # Create connection channel
        connection = self.get_connection()
        channel = connection.channel()

        # declare dead letter exchange if needed
        if dead_letter_exchange:
            dead_letter_exchange_name = f"dead.letter.{self.exchange_name}"
            channel.exchange_declare(dead_letter_exchange_name, ExchangeType.DIRECT)

        # Declare exchange
        channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type=exchange_type,
            passive=self.exchange_params.passive,
            durable=self.exchange_params.durable,
            auto_delete=self.exchange_params.auto_delete,
            internal=self.exchange_params.internal,
        )

        # Creates new queue or connects to existing one
        queue_name = self._build_queue_name(func)
        exchange_args = {}
        dead_letter_queue_name = None
        if dead_letter_exchange and not self.development:
            dead_letter_queue_name = f"dead.letter.{queue_name}"
            channel.queue_declare(
                dead_letter_queue_name,
                durable=self.queue_params.durable,
            )

            # Bind queue to exchange
            channel.queue_bind(
                exchange=dead_letter_exchange_name,
                queue=dead_letter_queue_name,
                routing_key=dead_letter_queue_name,
            )

            exchange_args = {
                "x-dead-letter-exchange": dead_letter_exchange_name,
                "x-dead-letter-routing-key": dead_letter_queue_name,
            }

        channel.queue_declare(
            queue_name,
            durable=self.queue_params.durable,
            auto_delete=self.queue_params.auto_delete,
            exclusive=self.queue_params.exclusive,
            arguments=exchange_args,
        )
        self.app.logger.info(f"Declaring Queue: {queue_name}")

        # Bind queue to exchange
        routing_keys = routing_key if isinstance(routing_key, list) else [routing_key]
        for routing_key in routing_keys:
            channel.queue_bind(
                exchange=self.exchange_name, queue=queue_name, routing_key=routing_key
            )

        def user_consumer(message: RabbitConsumerMessage, call_next) -> None:
            """User consumer as a middleware. Calls the consumer `func`."""
            func(
                routing_key=message.routing_key,
                body=message.parsed_body,
                **RabbitMQ.__get_needed_props(props_needed, message.props),
            )
            call_next(message)

        def callback(
            _: BlockingChannel,
            method: spec.Basic.Deliver,
            props: spec.BasicProperties,
            body: bytes,
        ):
            with self.app.app_context():
                decoded_body = body.decode()

                try:
                    # Fetches original message routing_key from headers if it has been dead-lettered
                    routing_key = method.routing_key

                    if getattr(props, "headers", None) and props.headers.get("x-death"):
                        x_death_props = props.headers.get("x-death")[0]
                        routing_key = x_death_props.get("routing-keys")[0]

                    message = RabbitConsumerMessage(
                        routing_key, body, self.body_parser(decoded_body), method, props
                    )
                    call_middlewares(
                        message, itertools.chain(list(self.middlewares), [user_consumer])
                    )

                    if not auto_ack:
                        # ack message after fn was ran
                        channel.basic_ack(method.delivery_tag)
                except Exception as err:  # pylint: disable=broad-except
                    self.app.logger.error(f"ERROR IN {queue_name}: {err}")
                    self.app.logger.exception(err)

                    try:
                        if not auto_ack:
                            channel.basic_reject(
                                method.delivery_tag, requeue=(not method.redelivered)
                            )
                    finally:
                        if self.on_message_error_callback is not None:
                            self.on_message_error_callback(
                                queue_name, dead_letter_queue_name, method, props, decoded_body, err
                            )

        channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=auto_ack
        )

        try:
            channel.start_consuming()
        except Exception as err:
            self.app.logger.error(err)
            channel.stop_consuming()
            connection.close()

            raise AMQPConnectionError from err

    def _send_msg(
        self, body, routing_key, exchange_type, message_version: str = "v1.0.0", **properties
    ):
        try:
            channel = self.get_connection().channel()

            channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type=exchange_type,
                passive=self.exchange_params.passive,
                durable=self.exchange_params.durable,
                auto_delete=self.exchange_params.auto_delete,
                internal=self.exchange_params.internal,
            )

            if self.msg_parser:
                body = self.msg_parser(body)

            if "message_id" not in properties:
                properties["message_id"] = sha256(json.dumps(body).encode("utf-8")).hexdigest()
            if "timestamp" not in properties:
                properties["timestamp"] = int(datetime.now().timestamp())

            if "headers" not in properties:
                properties["headers"] = {}
            properties["headers"]["x-message-version"] = message_version

            channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=body,
                properties=spec.BasicProperties(**properties),
            )

            channel.close()
        except Exception as err:
            self.app.logger.error("Error while sending message")
            self.app.logger.error(err)

            raise AMQPConnectionError from err

    def send(
        self,
        body,
        routing_key: str,
        exchange_type: ExchangeType = ExchangeType.DEFAULT,
        retries: int = 5,
        message_version: str = "v1.0.0",
        **properties
    ):
        """Sends a message to a given routing key

        Args:
            body (str): The body to be sent
            routing_key (str): The routing key for the message
            exchange_type (ExchangeType, optional): The exchange type to be used. Defaults to ExchangeType.DEFAULT.
            retries (int, optional): Number of retries to send the message. Defaults to 5.
            message_version (str): Message version number.
            properties (dict[str, Any]): Additional properties to pass to spec.BasicProperties
        """

        thread = Thread(
            target=lambda: self.sync_send(
                body, routing_key, exchange_type, retries, message_version, **properties
            ),
        )
        thread.daemon = True
        thread.start()

    def sync_send(
        self,
        body,
        routing_key: str,
        exchange_type: ExchangeType = ExchangeType.DEFAULT,
        retries: int = 5,
        message_version: str = "v1.0.0",
        **properties
    ):
        """Sends a message to a given routing key synchronously

        Args:
            body (str): The body to be sent
            routing_key (str): The routing key for the message
            exchange_type (ExchangeType, optional): The exchange type to be used. Defaults to ExchangeType.DEFAULT.
            retries (int, optional): Number of retries to send the message. Defaults to 5.
            message_version (str): Message version number.
            properties (dict[str, Any]): Additional properties to pass to spec.BasicProperties
        """

        retry_call(
            self._send_msg,
            (body, routing_key, exchange_type, message_version),
            properties,
            exceptions=(AMQPConnectionError, AssertionError),
            tries=retries,
            delay=5,
            jitter=(5, 15)
        )
