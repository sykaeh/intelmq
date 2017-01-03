# -*- coding: utf-8 -*-
"""
HTTP collector bot
"""

import io
import zipfile

import requests

from intelmq.lib.bot import CollectorBot, Param, ParameterDefinitions
from intelmq.lib.harmonization import String


class HTTPCollectorBot(CollectorBot):

    NAME = 'Generic URL Fetcher'
    DESCRIPTION = """Generic URL Fetcher is the bot responsible to get the
    report from an URL."""
    PARAMETERS = ParameterDefinitions('http feed collector', [
        Param('http_url', 'The URL of the feed', True, String)
    ])

    def init(self):
        self.set_request_parameters()

    def process(self):
        self.logger.info("Downloading report from %s" %
                         self.parameters.http_url)

        resp = requests.get(url=self.parameters.http_url, auth=self.auth,
                            proxies=self.proxy, headers=self.http_header,
                            verify=self.http_verify_cert,
                            cert=self.ssl_client_cert)

        if resp.status_code // 100 != 2:
            raise ValueError('HTTP response status code was {}.'
                             ''.format(resp.status_code))

        self.logger.info("Report downloaded.")

        raw_reports = []
        try:
            zfp = zipfile.ZipFile(io.BytesIO(resp.content), "r")
        except zipfile.BadZipfile:
            raw_reports.append(resp.text)
        else:
            self.logger.info('Downloaded zip file, extracting following files:'
                             ' ' + ', '.join(zfp.namelist()))
            for filename in zfp.namelist():
                raw_reports.append(zfp.read(filename))

        for raw_report in raw_reports:
            report = self.new_report()
            report.add("raw", raw_report)
            report.add("feed.url", self.parameters.http_url)
            self.send_message(report)


BOT = HTTPCollectorBot
