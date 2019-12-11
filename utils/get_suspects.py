#!/usr/bin/env python3

"""Extract information about suspected socks from a SPI case archive.

The goal here is to only extract information which is specific to the
SPI report, so just the username and the suspected master.  Other
features about the user will be added later in the pipeline.

"""

from pathlib import Path
import argparse
import datetime
import json
import logging
import sys

import mwparserfromhell

class ArchiveError(ValueError):
    "An error was found when parsing an SPI archive"


def main():
    "Main program"
    parser = argparse.ArgumentParser(epilog='''If neither --archive nor --archive_dir
    are given, reads from stdin.''')
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
    parser.add_argument('--log',
                        help='File to write log messages to',
                        type=argparse.FileType('a'),
                        default=str(Path.home() / 'sock-classifier/logs/get_features.log'))
    parser.add_argument('--job-name',
                        help='job name, used for status reporting')
    parser.add_argument('--log-level',
                        help='Logging level',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        default='INFO')
    args = parser.parse_args()
    configure_logging(args.log, args.log_level)
    logger = logging.getLogger('main')

    if args.archive_dir:
        paths = args.archive_dir.iterdir()
        input_streams = map(lambda p: p.open(), paths)
    else:
        input_streams = [args.archive]

    logger.info("Starting work, job-name = %s", args.job_name)
    start_time = datetime.datetime.now()

    count = 0
    for stream in input_streams:
        count += 1
        logger.info("Starting archive %d: %s", count, stream.name)
        archive = Archive(stream)
        for suspect in archive.get_suspects():
            print(json.dumps(suspect), file=args.out)

    finish_time = datetime.datetime.now()
    elapsed_time = finish_time - start_time
    logger.info("Processed %d archives in %s", count, elapsed_time)


def configure_logging(log_stream, log_level):
    "Configure logging"
    logging.basicConfig(stream=log_stream,
                        level=log_level,
                        format='%(process)d %(asctime)s [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')


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
        self.logger = logging.getLogger('archive')


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
                suspect['sock'] = sock
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
