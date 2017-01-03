# -*- coding: utf-8 -*-
import json

from intelmq.lib.bot import CollectorBot, Param, ParameterDefinitions
from intelmq.lib.harmonization import String

from .OTXv2 import OTXv2


class AlienVaultOTXCollectorBot(CollectorBot):

    NAME = 'AlienVault OTX'
    DESCRIPTION = """AlienVault OTX Collector is the bot responsible to get
    the report through the API. Report could vary according to subscriptions."""
    PARAMETERS = ParameterDefinitions('feed collector', [
        Param('api_key', 'Your API key', True, String),
        Param('https_proxy', 'HTTPS proxy', True, String)
    ])

    def init(self):
        if hasattr(self.parameters, 'http_ssl_proxy'):
            self.logger.warning("Parameter 'http_ssl_proxy' is deprecated and will be removed in "
                                "version 1.0!")
            if not self.parameters.https_proxy:
                self.parameters.https_proxy = self.parameters.http_ssl_proxy

    def process(self):
        self.logger.info("Downloading report through API")
        otx = OTXv2(self.parameters.api_key, proxy=self.parameters.https_proxy)
        pulses = otx.getall()
        self.logger.info("Report downloaded.")

        report = self.new_report()
        report.add("raw", json.dumps(pulses))
        self.send_message(report)


BOT = AlienVaultOTXCollectorBot
