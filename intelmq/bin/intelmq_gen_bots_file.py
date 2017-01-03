#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
import re

import pkgutil
import importlib
import intelmq.bots.collectors
import intelmq.bots.experts
import intelmq.bots.outputs
import intelmq.bots.parsers


def retrieve_bot_group(group):

    bots = []
    packages = pkgutil.walk_packages(group.__path__, group.__name__ + '.')
    for p in packages:
        if p[2] is False:
            print(p[1])
            module = importlib.import_module(p[1])
            try:
                bot = getattr(module, 'BOT')
                bot.MODULE = p[1]
                bots.append(bot)
            except AttributeError:
                continue

    return bots


if __name__ == '__main__':  # pragma: no cover
    DESCRIPTION = """
    Generate the legacy BOTS file

    Corrections:
        Indentation, sorting, separators
    """
    parser = argparse.ArgumentParser(prog='intelmq_gen_bots_file.py',
                                     description="Generate the legacy BOTS file")
    parser.add_argument('-o', '--output-file',
                        help='Path to output file',
                        default='./BOTS')
    args = parser.parse_args()

    BOTS = {'Collector': {}, 'Parser': {}, 'Output': {}, 'Expert': {}}
    bot_types = [
        ('Collector', intelmq.bots.collectors),
        ('Expert', intelmq.bots.experts),
        ('Output', intelmq.bots.outputs)
    ]

    for bt in bot_types:
        for bot in retrieve_bot_group(bt[1]):
            BOTS[bt[0]][bot.NAME] = {
                "description": re.sub(r"\s+", " ", bot.DESCRIPTION),
                "module": bot.MODULE,
                "enabled": True,
                "parameters": bot.PARAMETERS.to_legacy_format()
            }

    with open(args.output_file, 'w') as f:
        f.write(json.dumps(BOTS, indent=4, sort_keys=True, separators=(',', ': ')) + '\n')
