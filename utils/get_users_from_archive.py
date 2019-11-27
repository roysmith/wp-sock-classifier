#!/usr/bin/env python

"""Extract information about suspected socks from a SPI case archive, including:

* Username
* Master?
* Blocked?
* Date of initial report
* Date account registration
* Date of first edit

"""

import argparse
import datetime
import sys
import json
from pathlib import Path

import mwclient
import mwparserfromhell

class ArchiveError(ValueError):
    pass

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
    parser = argparse.ArgumentParser(epilog='''If neither --archive nor --dir
    are given, reads from stdin.''')
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument('--archive',
                             help='SPI archive file to read',
                             default=sys.stdin,
                             type=open)
    input_group.add_argument('--dir',
                             help='''Directory where SPI archives files
                             can be found.  Each file in that directory
                             will be processed in turn.''',
                             type=directory_path)
    args = parser.parse_args()

    site = mwclient.Site(SITE)
    namespaces = {v: k for k, v in site.namespaces.items()}

    if args.dir:
        for path in args.dir.iterdir():
            process_archive(path.open())
    else:
        process_archive(args.archive)


def directory_path(arg):
    "Type filter for argparse.add_argument()."
    path = Path(arg)
    if not path.is_dir():
        raise argparse.ArgumentTypeError('not a directory')
    return path


def process_archive(stream):
    for suspect in get_suspects(stream):
        print(json.dumps(suspect))

def get_suspects(stream):
    """Get the suspected socks from a stream containing an SPI archive.

    Returns a list of suspects.

    Returns an empty list and prints a diagnostic message on any kind
    of parsing error.

    """
    suspects = []
    wikicode = mwparserfromhell.parse(stream.read())
    try:
        for username, spi_date, master_flag in parse_suspects(wikicode):
            suspect = {
                'username': username,
                'master': master_flag,
                'spi_date': spi_date_to_iso(spi_date),
            }
            suspects.append(suspect)
    except ArchiveError as ex:
        print("Skipping %s: %s" % (stream.name, ex), file=sys.stderr)
        return []
    return suspects


def parse_suspects(wikicode):
    """Iterate over (username, spi_date, master_flag) tuples.

    * username is the account name of the suspect (no User: prefix)

    * spi_date is the date string in the format it appears in the SPI
    case, i.e., '22 November 2019'

    * master_flag is True if this is the suspected sock master
    """

    templates = wikicode.filter_templates(
        matches=lambda template: template.name.matches('SPIarchive notice'))
    count = len(templates)
    if count != 1:
        raise ArchiveError('expected exactly 1 SPIarchive notice, found %d' % count)

    master_username = templates[0].get(1).value.strip_code()
    master_pending = True

    for section in wikicode.get_sections(levels=[3]):
        spi_date = section.filter_headings()[0].title
        if master_pending:
            yield (master_username, spi_date, True)
            master_pending = False

        templates = section.filter_templates(
            matches=lambda template: template.name.matches('checkuser'))
        for template in templates:
            puppet_username = template.get(1).value.strip_code()
            yield (puppet_username, spi_date, False)


def spi_date_to_iso(spi_date):

    """Convert a string like "02 January 2018" into
    an ISO-8601 date string.

    Raises ArchiveError if spi_date can't be converted.
    """
    try:
        day, month_name, year = spi_date.split()
        month = MONTHS[month_name.lower()]
        return datetime.date(int(year), month, int(day)).isoformat()
    except:
        raise ArchiveError("can't convert date '%s'" % spi_date)


if __name__ == '__main__':
    main()
