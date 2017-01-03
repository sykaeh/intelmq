# -*- coding: utf-8 -*-
"""
CERT.at geolocate the national CERT abuse service
https://contacts.cert.at/cgi-bin/abuse-nationalcert.pl

HTTP GET: https://contacts.cert.at/cgi-bin/abuse-nationalcert.pl?ip=1.2.3.4
HTTP POST: https://contacts.cert.at/cgi-bin/abuse-nationalcert.pl

Options:
&bShowNationalCERT=on	Show national CERT contact info
&bShowHeader=on	Display a CSV header
&bVerbose=on	Display the source of the data, and other infos
&bFilter=off	Act as a filter: only show lines which geolocate to "AT"
&bKeepLoglines=off	Keep original log lines (separated by "#")
&sep={TAB, comma, semicolon, pipe}  	Separator for the (output) CSV format
"""

import requests

from intelmq.lib.bot import Bot, ParameterDefinitions, Param
from intelmq.lib.harmonization import Boolean

URL = 'https://contacts.cert.at/cgi-bin/abuse-nationalcert.pl'


class CERTatContactExpertBot(Bot):

    NAME = 'CERT.at Contact Database'
    DESCRIPTION = """Cert.at Contact Database is the bot responsible to get
    CERT. Set filter to true if you want to filter out events for cert.at.
    Set add_cc to true if you want to overwrite an existing CC value. """
    PARAMETERS = ParameterDefinitions('', [
        Param('filter', 'Act as a a filter for AT', True, Boolean, default=False),
        Param('http_verify_cert', '', True, Boolean, default=True),
        Param('overwrite_cc', 'Set to true if you want to overwrite any potentially existing cc fields in the event', True, Boolean, default=False)
    ])

    def process(self):
        event = self.receive_message()

        for section, key, abuse in \
            [('source', 'source.ip', 'source.abuse_contact'),
             ('destination', 'destination.ip', 'destination.abuse_contact'),
             ]:
            if key in event:
                parameters = {
                    'ip': event[key],
                    'bFilter': 'on' if self.parameters.filter else 'off',
                    'bShowNationalCERT': 'on',
                    'sep': 'semicolon',
                }
                req = requests.get(URL, params=parameters,
                                   verify=self.parameters.http_verify_cert,
                                   )
                response = req.text.strip().split(';')

                ccfield = '{}.geolocation.cc'.format(section)
                if self.parameters.overwrite_cc or ccfield not in event:
                    event.add(ccfield, response[1])

                if abuse in event:
                    old_abuse = event[abuse]
                    event.change(abuse, old_abuse + ',' + response[3])
                else:
                    event.add(abuse, response[3])

        self.send_message(event)
        self.acknowledge_message()


BOT = CERTatContactExpertBot
