# RabbitMQ Pika Flask

RabbitMQs pika library adapter for Flask. It's developed with the goal to make it easier to integrate the recommended RabbitMQ library with Flask.

For now only the topics exchange is available.

## Installing

Install and update using [pip](https://pip.pypa.io/en/stable/quickstart/):

```bash
    pip install rabbitmq-pika-flask
```

Add the following variables to your environment

- MQ_EXCHANGE=Your exchange name
- MQ_HOST=Your MQ Host
- MQ_PORT=Your MQ port
- MQ_USER=Your MQ user
- MQ_PASS=Your MQ password

## Basic Example

```python

  from flask import Flask
  from rabbitmq_pika_flask import RabbitMQ

  # init app
  app = Flask(__name__)

  # init rabbit mq
  rabbit = RabbitMQ(app)

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

## Author

Aylton Almeida
