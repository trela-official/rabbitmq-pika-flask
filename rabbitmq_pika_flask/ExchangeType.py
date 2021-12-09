from enum import Enum


class ExchangeType(Enum):
    """The type of exchange to be used
    """

    DEFAULT = 'topic'
    DIRECT = 'direct'
    FANOUT = 'fanout'
    TOPIC = 'topic'
    HEADER = 'header'
