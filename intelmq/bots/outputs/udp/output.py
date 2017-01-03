# -*- coding: utf-8 -*-
import socket
import unicodedata

import intelmq.lib.utils as utils
from intelmq.lib.bot import Bot, ParameterDefinitions, Param
from intelmq.lib.harmonization import String, Boolean, Integer


class UDPBot(Bot):

    NAME = 'UDP'
    DESCRIPTION = """TCP is the bot responsible to send events to a tcp port
    (Splunk, ElasticSearch, etc..). """
    PARAMETERS = ParameterDefinitions('', [
        Param('udp_host', 'IP address of the UDP server', True, String),
        Param('udp_port', 'PORT to connect to', True, Integer),
        Param('field_delimiter', 'If the format is \'delimited\' this will be added between fields.', True, String, default='|'),
        Param('format', 'Can be \'json\' or \'delimited\'. The json format outputs the event \'as-is\'. Delimited will deconstruct the event and print each field:value separated by the field delimit.', True, String),
        Param('header', 'Header text to be sent in the udp datagram', True, String),
        Param('keep_raw_field', 'False will not send the raw field in the message.', True, Boolean, default=False)
    ])

    def init(self):
        self.delimiter = self.parameters.field_delimiter
        self.header = self.parameters.header
        self.udp_host = socket.gethostbyname(self.parameters.udp_host)
        self.upd_address = (self.udp_host, self.parameters.udp_port)
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.keep_raw_field = bool(self.parameters.keep_raw_field)
        self.format = self.parameters.format.lower()
        if self.format not in ['json', 'delimited']:
            self.logger.error('Unknown format %r given. Check your configuration.' % self.format)
            self.stop()

    def process(self):
        event = self.receive_message()

        if not self.keep_raw_field:
            del event['raw']

        if self.format == 'json':
            self.send(self.header + event.to_json())
        elif self.format == 'delimited':
            self.send(self.delimited(event))

    def remove_control_char(self, s):
        return "".join(ch for ch in s if unicodedata.category(ch)[0] != "C")

    def delimited(self, event):
        log_line = self.header
        for key, value in event.items():
            log_line += self.delimiter + key + ':' + str(value)

        return log_line

    def send(self, rawdata):
        data = utils.encode(self.remove_control_char(rawdata) + '\n')
        try:
            self.udp.sendto(data, self.upd_address)
        except:
            self.logger.exception('Failed to sent message to {}:{} !'
                                  .format(self.udp_host, self.udp_port))
        else:
            self.acknowledge_message()


BOT = UDPBot
