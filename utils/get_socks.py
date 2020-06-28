#!/usr/bin/env python3

"""Find known socks, based on SPI case archives.  Duplicates are
suppressed.

The goal here is to only extract information which is specific to the
SPI report, so just the username and the sockmaster.  Other features
about the user will be added later in the pipeline.

"""

from pathlib import Path
import argparse
import datetime
import json
import logging
import sys

import mwparserfromhell
import toolforge

import config

class ArchiveError(ValueError):
    "An error was found when parsing an SPI archive"


def main():
    "Main program"
    parser = argparse.ArgumentParser(epilog='''If neither --archive nor --archive_dir
                                               are given, reads from stdin.''',
                                     parents=[config.logging_cli()])

    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument('--archive',
                             help='SPI archive file to read',
                             default=sys.stdin,
                             type=open)
    input_group.add_argument('--archive-dir',
                             help='''Directory where SPI archives files can be found.  Each file in
                             that directory will be processed in turn.''',
                             type=directory_path)
    parser.add_argument('--out',
                        help='Output file',
                        type=argparse.FileType('w'),
                        default=sys.stdout)


    args = parser.parse_args()
    config.configure_logging(args)

    logger = logging.getLogger('get_socks')
    db = toolforge.connect('enwiki')

    if args.archive_dir:
        paths = args.archive_dir.iterdir()
        input_streams = map(lambda p: p.open(), paths)
    else:
        input_streams = [args.archive]

    logger.info("Starting work, job-name = %s", args.job_name)
    start_time = datetime.datetime.now()

    archive_count = 0
    suspect_count = 0
    non_sock_count = 0
    duplicate_count = 0
    seen_users = set()
    for stream in input_streams:
        archive_count += 1
        logger.info("Starting archive %d: %s", archive_count, stream.name)
        archive = Archive(stream)
        for suspect in archive.get_suspects():
            suspect_count += 1
            user = suspect['user']
            if user in seen_users:
                duplicate_count += 1
                logger.info("Duplicate supressed: %s", user)
                continue
            if is_sock(db, user):
                seen_users.add(user)
                suspect['is_sock'] = True
                print(json.dumps(suspect), file=args.out)
            else:
                non_sock_count += 1
                logger.info("Skipping non-sock: %s", user)

    finish_time = datetime.datetime.now()
    elapsed_time = finish_time - start_time
    logger.info("Done with %d archives, %d suspects, %d socks, %d non-socks, %d duplicates in %s",
                archive_count,
                suspect_count,
                len(seen_users),
                non_sock_count,
                duplicate_count,
                elapsed_time)


def is_sock(db, username):
    '''Returns True if sock has indeed been blocked as a sock.

    It's surprisingly non-trivial to figure out if this is the
    case.  The current implementation uses the simplistic
    assumption that if the user is mentioned in an SPI case as a
    suspected sock, and they've been indef blocked, then they're a
    sock.  The problem with this is that some suspects are found
    to not be socks but are indef blocked for other reasons.

    '''
    with db.cursor() as cur:
        cur.execute("""
        select count(*) from ipblocks
        join actor on ipb_user = actor_user
        where actor_name = %(username)s
        and ipb_expiry = 'infinity'
        """, {'username': username})
        row = cur.fetchone()
    return bool(row[0])


def directory_path(arg):
    "Type filter for argparse.add_argument().  Returns a Path object."
    path = Path(arg)
    if not path.is_dir():
        raise argparse.ArgumentTypeError('not a directory')
    return path


class Archive:
    "An SPI case archive."

    def __init__(self, stream):
        self.stream = stream
        self.logger = logging.getLogger('get_socks.archive')


    def get_suspects(self):
        """Get the suspected socks from a stream containing an SPI archive.

        Returns a list of suspects.

        Returns an empty list and logs a diagnostic message on any kind
        of parsing error.

        We build the internal suspect list instead of just yielding
        the suspect directly, because we want any subsequent parsing
        errors to reject the entire archive as unreliable.

        """
        suspects = []
        try:
            for sock, master in self.parse_suspects():
                suspect = {}
                if master:
                    suspect['master'] = master
                suspect['user'] = sock
                suspects.append(suspect)
            return suspects
        except ArchiveError as ex:
            self.logger.warning("Skipping %s: %s", self.stream.name, ex)
            return []
        except Exception as ex:
            raise RuntimeError("error in %s" % self.stream.name) from ex


    def parse_suspects(self):
        """Iterate over (sock, master) tuples.

        * sock is the account name of the suspected sock (no User:
          prefix).

        * master is the username of the suspected sockmaster.  For
        sockmasters, this will be None, and sock will hold the username.

        """
        wikicode = mwparserfromhell.parse(self.stream.read())
        templates = wikicode.filter_templates(
            matches=lambda template: template.name.matches('SPIarchive notice'))
        count = len(templates)
        if count != 1:
            raise ArchiveError('expected exactly 1 SPIarchive notice, found %d' % count)

        master_username = templates[0].get(1).value.strip_code()
        yield (master_username, None)

        templates = wikicode.filter_templates(
            matches=lambda template: template.name.matches('checkuser'))
        for template in templates:
            if template.has(1):
                puppet_username = template.get(1).value.strip_code()
                self.logger.debug("Found %s", puppet_username)
                yield (puppet_username, master_username)
            else:
                self.logger.warning("Skipping template (%s), missing param: %s",
                                    self.stream.name, template)


if __name__ == '__main__':
    main()
