# RabbitMQ Pika Flask

RabbitMQs pika library adapter for Flask. It's developed with the goal to make it easier to integrate the recommended RabbitMQ library with Flask.

For now our attention is focused at the topics exchange type.

## Installing

Install and update using [pip](https://pip.pypa.io/en/stable/quickstart/):

```bash
    pip install rabbitmq-pika-flask
```

Add the following variables to your environment or the Flask settings:

- FLASK*ENV=Flask environment, such as \_production* or _development_
- MQ_EXCHANGE=Your exchange name
- MQ_URL=Your MQ URL following [this format](https://pika.readthedocs.io/en/stable/examples/using_urlparameters.html)

## Basic Example

```python

  from flask import Flask
  from rabbitmq_pika_flask import RabbitMQ

  # init app
  app = Flask(__name__)

  # init rabbit mq
  rabbit = RabbitMQ(app, 'example')

  # send message
  @app.route('ping', methods=['GET'])
  def ping():
    rabbit.send(body='ping', routing_key='ping.message')
    return 'pong'

  # listen to messages
  @rabbit.queue(routing_key='ping.message')
  def ping_event(routing_key, body):
    app.logger.info('Message received:')
    app.logger.info('\tKey: {}'.format(routing_key))
    app.logger.info('\tBody: {}'.format(body))

```

Check examples in the `examples` folder

## Contributing

Still learning how to make a good open source project. Any ideas would be great.
But if feel free to open an `issue` or `pull request` if you have any problems or ideas.

## Author

Aylton Almeida
