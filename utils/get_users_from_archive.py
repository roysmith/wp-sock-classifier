#!/usr/bin/env python

"""Find information about a suspected sock:

* Username
* Master?
* Blocked?
* Date of initial report
* Date account registration
* Date of first edit
"""

import argparse
import datetime
import re
import urllib.parse
import itertools
import json

from pathlib import Path
from collections import Counter

import mwclient
import mwparserfromhell


SITE = 'en.wikipedia.org'

MONTHS = {
    'january': 1,
    'february': 2,
    'march': 3,
    'april': 4,
    'may': 5,
    'june': 6,
    'july': 7,
    'august': 8,
    'september': 9,
    'october': 10,
    'november': 11,
    'december': 12
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--archive',
                        help='SPI archive file to read',
                        type=open,
                        required=True)
    args = parser.parse_args()

    site = mwclient.Site(SITE)
    namespaces = {v: k for k, v in site.namespaces.items()}

    wikicode = mwparserfromhell.parse(args.archive.read())
    for username, spi_date, master_flag in parse_suspects(wikicode):
        suspect = {
            'username': username,
            'master': master_flag,
            'spi_date': spi_date_to_iso(spi_date),
            }
        print(json.dumps(suspect))



def parse_suspects(wikicode):
    """Iterate over (username, spi_date, master_flag) tuples.

    * username is the account name of the suspect (no User: prefix)

    * spi_date is the date string in the format it appears in the SPI
    case, i.e., '22 November 2019'

    * master_flag is True if this is the suspected sock master
    """

    templates = wikicode.filter_templates(
        matches = lambda template: template.name.matches('SPIarchive notice'))
    if len(templates) != 1:
        raise ValueError('expected exactly 1 SPIarchive notice')
    master_username = templates[0].get(1).value.strip_code()
    master_pending = True

    for section in wikicode.get_sections(levels=[3]):
        spi_date = section.filter_headings()[0].title
        if master_pending:
            yield (master_username, spi_date, True)
            master_pending = False

        templates = section.filter_templates(
            matches = lambda template: template.name.matches('checkuser'))
        for template in templates:
            puppet_username = template.get(1).value.strip_code()
            yield (puppet_username, spi_date, False)

        
def spi_date_to_iso(spi_date):

    """Convert a string like "02 January 2018" into
    an ISO-8601 date string.

    Raises ValueError if spi_date can't be converted.
    """
    try:
        day, month_name, year = spi_date.split()
        month = MONTHS[month_name.lower()]
        return datetime.date(int(year), month, int(day)).isoformat()
    except:
        raise ValueError("can't convert '%s'" % spi_date)
    
    
if __name__ == '__main__':
    main()
