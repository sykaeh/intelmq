# -*- coding: utf-8 -*-
"""
A collector for grabbing appropriately tagged events from MISP.
"""

import json
from urllib.parse import urljoin

from intelmq.lib.bot import CollectorBot, Param, ParameterDefinitions
from intelmq.lib.harmonization import String, Boolean

try:
    from pymisp import PyMISP
except ImportError:
    PyMISP = None


class MISPCollectorBot(CollectorBot):

    NAME = 'MISP Generic'
    DESCRIPTION = 'Collect events from a MISP server.'
    PARAMETERS = ParameterDefinitions('feed collector', [
        Param('misp_url', 'URL of the MISP server (with trailing \'/\')', True, String),
        Param('misp_key', 'API key for accessing MISP', True, String),
        Param('misp_verify', 'Check the validity of the certificate', True, Boolean, default=True),
        Param('misp_tag_to_process', 'MISP tag identifying events to be processed', True, String),
        Param('misp_tag_processed', 'MISP tag identifying events that have been processed', True, String)
    ])

    def init(self):
        if PyMISP is None:
            self.logger.error('Could not import pymisp. Please install it.')
            self.stop()

        # Initialise MISP connection
        self.misp = PyMISP(self.parameters.misp_url,
                           self.parameters.misp_key,
                           self.parameters.misp_verify)

        # URLs used for deleting and adding MISP event tags
        self.misp_add_tag_url = urljoin(self.parameters.misp_url,
                                        'events/addTag')
        self.misp_del_tag_url = urljoin(self.parameters.misp_url,
                                        'events/removeTag')

    def process(self):
        # Grab the events from MISP
        misp_result = self.misp.search(
            tags=self.parameters.misp_tag_to_process
        )

        # Process the response and events
        if 'response' in misp_result:

            # Extract the MISP event details
            for e in misp_result['response']:
                misp_event = e['Event']

                # Send the results to the parser
                report = self.new_report()
                report.add('raw', json.dumps(misp_event, sort_keys=True))
                report.add('feed.url', self.parameters.misp_url)
                self.send_message(report)

            # Finally, update the tags on the MISP events.
            # Note PyMISP does not currently support this so we use
            # the API URLs directly with the requests module.

            for misp_event in misp_result['response']:
                # Remove the 'to be processed' tag
                self.misp.remove_tag(misp_event,
                                     self.parameters.misp_tag_to_process)

                # Add a 'processed' tag to the event
                self.misp.add_tag(misp_event,
                                  self.parameters.misp_tag_processed)


BOT = MISPCollectorBot
