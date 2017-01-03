# -*- coding: utf-8 -*-
"""
See README for database download.
"""

from intelmq.lib.bot import Bot, ParameterDefinitions, Param
from intelmq.lib.harmonization import String


class TorExpertBot(Bot):

    NAME = 'Tor Nodes'
    DESCRIPTION = """Tor Nodes is the bot responsible to check if an IP is an
    Tor Exit Node. """
    PARAMETERS = ParameterDefinitions('', [
        Param('database', '', True, String,
              default='/opt/intelmq/var/lib/bots/tor_nodes/tor_nodes.dat')
    ])

    database = set()

    def init(self):
        self.logger.info("Loading TOR exit node IPs.")

        try:
            with open(self.parameters.database) as fp:
                for line in fp:
                    line = line.strip()

                    if len(line) == 0 or line[0] == "#":
                        continue

                    self.database.add(line)

        except IOError:
            self.logger.critical("TOR rule not defined or failed on open.")
            self.stop()

    def process(self):
        event = self.receive_message()

        for key in ["source.", "destination."]:
            if event.contains(key + 'ip'):
                if event.get(key + 'ip') in self.database:
                    event.add(key + 'tor_node', True)

        self.send_message(event)
        self.acknowledge_message()


BOT = TorExpertBot
