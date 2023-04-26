class QueueParams:
    """ Default parameters for queues
    """

    durable: bool
    auto_delete: bool
    exclusive: bool
    passive: bool

    def __init__(self, durable=True, auto_delete=False, exclusive=False, passive=False) -> None:
        self.durable = durable
        self.auto_delete = auto_delete
        self.exclusive = exclusive
        self.passive = passive
