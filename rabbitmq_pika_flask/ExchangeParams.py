class ExchangeParams:
    """Default parameters for exchanges"""

    passive: bool
    durable: bool
    auto_delete: bool
    internal: bool

    def __init__(self, passive=False, durable=False, auto_delete=False, internal=False) -> None:
        self.passive = passive
        self.durable = durable
        self.auto_delete = auto_delete
        self.internal = internal
