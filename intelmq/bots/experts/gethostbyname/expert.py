# -*- coding: utf-8 -*-
import socket

import intelmq.lib.harmonization
from intelmq.lib.bot import Bot, ParameterDefinitions


class GethostbynameExpertBot(Bot):

    NAME = 'Gethostbyname'
    DESCRIPTION = """Gethostbyname is the bot responsible to parsing the ip
    from the fqdn. """
    PARAMETERS = ParameterDefinitions('', [])

    def process(self):
        event = self.receive_message()

        for key in ["source.", "destination."]:
            key_fqdn = key + "fqdn"
            key_ip = key + "ip"
            if not event.contains(key_fqdn):
                continue
            if event.contains(key_ip):
                continue
            ip = socket.gethostbyname(event.get(key_fqdn))
            if intelmq.lib.harmonization.IPAddress.is_valid(ip, sanitize=True):
                event.add(key_ip, ip, sanitize=True)

        self.send_message(event)
        self.acknowledge_message()


BOT = GethostbynameExpertBot
