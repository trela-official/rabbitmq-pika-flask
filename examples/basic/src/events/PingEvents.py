from examples.basic.src.services.rabbit import rabbit


@rabbit.queue(routing_key='ping.*', dead_letter_exchange=True, props_needed=["message_id"])
def ping_event(routing_key, body, message_id):
    print('Message received:')
    print('\tKey: {}'.format(routing_key))
    print('\tBody: {}'.format(body))
    print('\tMessage: {}'.format(message_id))

    if routing_key == 'ping.error':
        raise Exception('Generic Error')
