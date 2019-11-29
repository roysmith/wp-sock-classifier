#!/usr/bin/env python

"""Extract information about suspected socks from a SPI case archive, including:

* Username
* Master?
* Blocked?
* Date of initial report
* Date account registration
* Date of first edit

"""

from pathlib import Path
import argparse
import calendar
import datetime
import json
import logging
import sys

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

logger = None

def main():
    parser = argparse.ArgumentParser(epilog='''If neither --archive nor --spi_dir
    are given, reads from stdin.''')
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument('--archive',
                             help='SPI archive file to read',
                             default=sys.stdin,
                             type=open)
    input_group.add_argument('--spi_dir',
                             help='''Directory where SPI archives files
                             can be found.  Each file in that directory
                             will be processed in turn.''',
                             type=directory_path)
    parser.add_argument('--log',
                        help='File to write log messages to',
                        type=argparse.FileType('a'),
                        default=str(Path.home() / 'logs' / 'get_users_from_archive.log'))
    args = parser.parse_args()
    configure_logging(args.log)

    if args.spi_dir:
        for path in args.spi_dir.iterdir():
            process_archive(path.open())
    else:
        process_archive(args.archive)


def configure_logging(log_stream):
    logging.basicConfig(stream=log_stream, level=logging.INFO,
                        format='%(process)d %(asctime)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    global logger
    logger = logging.getLogger()


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

    Returns an empty list and logs a diagnostic message on any kind
    of parsing error.

    """
    suspects = []
    try:
        for sock, spi_date, master in parse_suspects(stream):
            suspect = {'master': master} if master else {}
            suspect['sock'] = sock
            suspect['spi_date'] = spi_date_to_iso(spi_date)

            reg_date = get_registration_date(sock)
            if reg_date:
                suspect['reg_time'] = reg_date.isoformat()

            suspects.append(suspect)
        return suspects
    except ArchiveError as ex:
        logger.warning("Skipping %s: %s", stream.name, ex)
        return []
    except Exception as ex:
        raise RuntimeError("error in %s" % stream.name) from ex


def get_registration_date(username):
    """Return a (UTC) datetime if a registration time can be found for the user.
    If no entry can be found in the logs, return None.
    """
    site = mwclient.Site(SITE)
    events = list(site.logevents(title='User:%s' % username, type='newusers'))
    if len(events) == 0:
        logger.info("Can't find newuser log entry for %s", username)
        return
    if len(events) > 1:
        logger.warning("Multiple newuser log entries for %s", username)
        return
    timestamp = events[0]['timestamp']
    return datetime.datetime.utcfromtimestamp(calendar.timegm(timestamp))


def parse_suspects(stream):
    """Iterate over (sock, spi_date, master) tuples.

    * sock is the account name of the suspected sock (no User:
      prefix).

    * spi_date is the date string in the format it appears in the SPI
    case, i.e., '22 November 2019'.

    * master is the username of the suspected sockmaster.  For
    sockmasters, this will be None, and sock wil hold the username.

    """
    wikicode = mwparserfromhell.parse(stream.read())
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
            # We havn't printed the data from the {{SPIarchive
            # notice}} yet, so print it now.  We needed to delay
            # printing that until we processed the first level-3
            # heading, because that's when we discover the date of the
            # first report.
            yield (master_username, spi_date, None)
            master_pending = False

        templates = section.filter_templates(
            matches=lambda template: template.name.matches('checkuser'))
        for template in templates:
            if template.has(1):
                puppet_username = template.get(1).value.strip_code()
                yield (puppet_username, spi_date, master_username)
            else:
                logger.warning("Skipping template (%s), missing param: %s", stream.name, template)

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
