# -*- coding: utf-8 -*-
import socket

import intelmq.lib.utils as utils
from intelmq.lib.bot import Bot, Param, ParameterDefinitions
from intelmq.lib.harmonization import String, Integer, Boolean


class TCPOutputBot(Bot):

    NAME = 'TCP'
    DESCRIPTION = """TCP is the bot responsible to send events to a tcp port
    (Splunk, ElasticSearch, etc..). """
    PARAMETERS = ParameterDefinitions('', [
        Param('ip', '', True, String),
        Param('port', '', True, Integer),
        Param('separator', '', True, String, default='\n'),
        Param('hierarchical_output',
              'Whether to convert events to hierarchical format', True, Boolean,
              default=False)
    ])

    def init(self):
        self.address = (self.parameters.ip, int(self.parameters.port))
        self.separator = utils.encode(self.parameters.separator)
        self.connect()

    def process(self):
        event = self.receive_message()

        data = event.to_json(hierarchical=self.parameters.hierarchical_output)
        try:
            self.con.sendall(utils.encode(data) + self.separator)
        except socket.error as exc:
            self.logger.exception(exc.args[1] + ". Reconnecting..")
            self.con.close()
            self.connect()
        except AttributeError:
            self.logger.info('Reconnecting.')
            self.connect()
        else:
            self.acknowledge_message()

    def connect(self):
        self.con = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.con.connect(self.address)
        except:
            raise
        else:
            self.logger.info("Connected successfully to {!s}: {}"
                             "".format(self.address[0], self.address[1]))


BOT = TCPOutputBot
