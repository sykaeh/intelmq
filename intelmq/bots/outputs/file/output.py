# -*- coding: utf-8 -*-
import io

from intelmq.lib.bot import Bot, Param, ParameterDefinitions
from intelmq.lib.harmonization import String, Boolean


class FileOutputBot(Bot):

    NAME = 'File'
    DESCRIPTION = """File is the bot responsible to send events to a file. """
    PARAMETERS = ParameterDefinitions('', [
        Param('file', 'Path to output file', True, String),
        Param('hierarchical_output', 'Whether to convert events to hierarchical format', True, Boolean, default=False),
        Param('expand_extra', 'Whether the extra field should be expanded', False, Boolean, default=False)
    ])

    def init(self):
        self.logger.debug("Opening %r file." % self.parameters.file)
        self.file = io.open(self.parameters.file, mode='at', encoding="utf-8")
        self.logger.info("File %r is open." % self.parameters.file)

    def process(self):
        event = self.receive_message()

        event_data = event.to_json(hierarchical=self.parameters.hierarchical_output)
        self.file.write(event_data)
        self.file.write("\n")
        self.file.flush()
        self.acknowledge_message()


BOT = FileOutputBot
