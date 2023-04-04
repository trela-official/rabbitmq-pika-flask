class ExchangeParams:
    """Default parameters for exchanges"""

    durable: bool
    auto_delete: bool
    internal: bool

    def __init__(self, durable=False, auto_delete=False, internal=False) -> None:
        self.durable = durable
        self.auto_delete = auto_delete
        self.internal = internal
