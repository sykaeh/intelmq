# -*- coding: utf-8 -*-

import requests

from intelmq.lib.bot import CollectorBot, Param, ParameterDefinitions
from intelmq.lib.utils import decode
from intelmq.lib.harmonization import String


class HTTPStreamCollectorBot(CollectorBot):

    NAME = 'Generic URL Stream Fetcher'
    DESCRIPTION = """Generic URL Stream Fetcher is the bot responsible to
    get the report from an URL."""
    PARAMETERS = ParameterDefinitions('http feed collector', [
        Param('url', 'The URL of the feed', True, String)
    ])

    def process(self):
        try:
            req = requests.get(self.parameters.url, stream=True)
        except requests.exceptions.ConnectionError:
            raise ValueError('Connection Failed.')
        else:
            for line in req.iter_lines():
                if self.parameters.strip_lines:
                    line = line.strip()

                if not line:
                    # filter out keep-alive new lines and empty lines
                    continue

                report = self.new_report()
                report.add("raw", decode(line))
                self.send_message(report)
            self.logger.info('Stream stopped.')


BOT = HTTPStreamCollectorBot
