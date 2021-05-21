from ..services.rabbit import rabbit


@rabbit.queue(routing_key='ping.message')
def ping_event(routing_key, body):
    print('Message received:')
    print('\tKey: {}'.format(routing_key))
    print('\tBody: {}'.format(body))
