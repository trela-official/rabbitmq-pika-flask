import itertools
from typing import Any, Callable, Iterator
from pika import spec


RabbitConsumerMiddlewareCallNext = Callable[["RabbitConsumerMessage"], Any]
RabbitConsumerMiddleware = Callable[
    ["RabbitConsumerMessage", RabbitConsumerMiddlewareCallNext], Any
]


class RabbitConsumerMiddlewareError(RuntimeError):
    """Raised when a middleware results in an unexpected error. This is raised by the
    code that handles middlewares; not raised by the middlewares themselves."""


class RabbitConsumerMessage:
    def __init__(
        self,
        routing_key: str,
        raw_body: bytes,
        parsed_body: Any,
        method: spec.Basic.Deliver,
        props: spec.BasicProperties,
    ) -> None:
        self.routing_key = routing_key
        self.raw_body = raw_body
        self.parsed_body = parsed_body
        self.method = method
        self.props = props

    def __str__(self) -> str:
        return str(self.__dict__)


def call_middlewares(
    message: RabbitConsumerMessage, middlewares: Iterator[RabbitConsumerMiddleware]
) -> None:
    """Calls middlewares with `message`. Each middleware is *expected* to call
    `call_next` once, and exactly once; this is not enforced."""

    middlewares_iter = itertools.chain(middlewares, [lambda message, call_next: None])

    def call_next(message: RabbitConsumerMessage) -> None:
        try:
            middleware = next(middlewares_iter)
        except StopIteration as e:
            # We can't be 100% sure which middleware did this
            raise RabbitConsumerMiddlewareError("Middleware called `call_next` twice.")
        middleware(message, call_next)

    call_next(message)
