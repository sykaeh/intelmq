# -*- coding: utf-8 -*-

from intelmq.lib.bot import CollectorBot, Param, ParameterDefinitions
from intelmq.lib.harmonization import String

try:
    import pycurl
except:
    pycurl = None


class BitsightCollectorBot(CollectorBot):

    NAME = 'BitSight Ciberfeed Stream'
    DESCRIPTION = """Bitsight Collector is the bot responsible to get
    Bitsight Ciberfeed Alert Stream."""
    PARAMETERS = ParameterDefinitions('http feed collector', [
        Param('http_url',
              'URL of the feed (e.g. http://alerts.bitsighttech.com:8080/stream?key=<api>)',
              True, String)
    ])

    def init(self):
        if hasattr(self.parameters, 'http_ssl_proxy'):
            self.logger.warning(
                "Parameter 'http_ssl_proxy' is deprecated and will be removed in "
                "version 1.0!")
            if not self.parameters.https_proxy:
                self.parameters.https_proxy = self.parameters.http_ssl_proxy

        self.logger.info("Connecting to BitSightTech stream server")
        self.conn = pycurl.Curl()
        if self.parameters.http_proxy:
            self.conn.setopt(pycurl.PROXY, self.parameters.http_proxy)
        if self.parameters.https_proxy:
            self.conn.setopt(pycurl.PROXY, self.parameters.https_proxy)
        self.conn.setopt(pycurl.URL, self.parameters.http_url)
        self.conn.setopt(pycurl.WRITEFUNCTION, self.on_receive)

    def process(self):
        self.conn.perform()

    def shutdown(self):
        self.conn.close()

    def on_receive(self, data):
        for line in data.decode().splitlines():
            line = line.strip()
            if line == "":
                continue

            report = self.new_report()
            report.add("raw", line)
            report.add("feed.url", self.parameters.http_url)

            self.send_message(report)


BOT = BitsightCollectorBot
