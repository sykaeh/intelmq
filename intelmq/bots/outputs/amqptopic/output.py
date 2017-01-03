# -*- coding: utf-8 -*-

from intelmq.lib.bot import Bot, Param, ParameterDefinitions
from intelmq.lib.harmonization import Integer, String, Boolean

try:
    import pika
except ImportError:
    pika = None


class AMQPTopicBot(Bot):
    NAME = 'AMQP Topic'
    DESCRIPTION = """AMQP Topic is the bot responsible to send events to a
    AMQP topic exchange. """
    PARAMETERS = ParameterDefinitions('', [
        Param('connection_attempts',
              'The number of connection attempts to defined server', True,
              Integer, default=3),
        Param('connection_heartbeat', 'Heartbeat to server, in seconds', True,
              Integer, default=3600),
        Param('connection_host', 'Name/IP for the AMQP server', True, String,
              default='127.0.0.1'),
        Param('connection_port', 'Port for the AMQP server', True, Integer,
              default=5672),
        Param('connection_vhost',
              'Virtual host to connect, on a http(s) connection would be http:/IP/<your virtual host>',
              True, String),
        Param('content_type',
              'Content type to deliver to AMQP server, currently only supports "application/json"',
              True, String, default='application/json'),
        Param('delivery_mode',
              '1 - Non-persistent, 2 - Persistent. On persistent mode, messages are delivered to \'durable\' queues and will be saved to disk',
              True, Integer, default=2),
        Param('exchange_durable',
              'If set to True, the exchange will survive broker restart, otherwise will be a transient exchange.',
              True, Boolean, default=True),
        Param('exchange_name', 'The name of the exchange to use', True, String),
        Param('exchange_type',
              'Type of the exchange, presently only "topic" is supported', True,
              String, default='topic'),
        Param('keep_raw_field',
              'If set to True, the message \'raw\' field will be sent', True,
              Boolean, default=False),
        Param('password', 'Password for authentication on your AMQP server',
              True, String),
        Param('require_confirmation',
              'If set to True, an exception will be raised if a confirmation error is received',
              True, Boolean, default=True),
        Param('routing_key', 'The routing key for your amqptopic', True,
              String),
        Param('username', 'Username for authentication on your AMQP server',
              True, String),
    ])

    def init(self):
        self.connection = None
        self.channel = None
        self.keep_raw_field = self.parameters.keep_raw_field
        self.delivery_mode = self.parameters.delivery_mode
        self.content_type = self.parameters.content_type
        self.exchange = self.parameters.exchange_name
        self.require_confirmation = self.parameters.require_confirmation
        self.durable = self.parameters.exchange_durable
        self.exchange_type = self.parameters.exchange_type
        self.connection_host = self.parameters.connection_host
        self.connection_port = self.parameters.connection_port
        self.connection_vhost = self.parameters.connection_vhost
        self.credentials = pika.PlainCredentials(self.parameters.username, self.parameters.password)
        self.connection_parameters = pika.ConnectionParameters(
            host=self.connection_host,
            port=self.connection_port,
            virtual_host=self.connection_vhost,
            connection_attempts=self.parameters.connection_attempts,
            heartbeat_interval=self.parameters.connection_heartbeat,
            credentials=self.credentials)
        self.routing_key = self.parameters.routing_key
        self.properties = pika.BasicProperties(
            content_type=self.content_type, delivery_mode=self.delivery_mode)
        self.connect_server()

    def connect_server(self):
        self.logger.info('AMQP Connecting to {}:{}{}.'.format(self.connection_host,
                                                              self.connection_port,
                                                              self.connection_vhost))
        try:
            self.connection = pika.BlockingConnection(self.connection_parameters)
        except pika.exceptions.ProbableAuthenticationError:
            self.logger.error('AMQP authentication failed!')
            raise
        except pika.exceptions.ProbableAccessDeniedError:
            self.logger.error('AMQP authentication for virtual host failed!')
            raise
        except pika.exceptions.AMQPConnectionError:
            self.logger.error('AMQP connection failed!')
            raise
        else:
            self.logger.info('AMQP connection succesfull.')
            self.channel = self.connection.channel()
            try:
                self.channel.exchange_declare(exchange=self.exchange, type=self.exchange_type,
                                              durable=self.durable)
            except pika.exceptions.ChannelClosed:
                self.logger.error('Access to exchange refused.')
                raise
            self.channel.confirm_delivery()

    def process(self):
        """ Stop the Bot if cannot connect to AMQP Server after the defined
        connection attempts. """

        # self.connection and self.channel can be None
        if getattr(self.connection, 'is_closed', None) or getattr(self.channel, 'is_closed', None):
                self.connect_server()

        event = self.receive_message()

        if not self.keep_raw_field:
            del event['raw']

        try:
            if not self.channel.basic_publish(exchange=self.exchange,
                                              routing_key=self.routing_key,
                                              body=event.to_json(),
                                              properties=self.properties,
                                              mandatory=True):
                if self.require_confirmation:
                    raise ValueError('Message sent but not confirmed.')
                else:
                    self.logger.info('Message sent but not confirmed.')
        except (pika.exceptions.ChannelError, pika.exceptions.AMQPChannelError,
                pika.exceptions.NackError):
            self.logger.exception('Error publishing the message.')
        else:
            self.acknowledge_message()


BOT = AMQPTopicBot
