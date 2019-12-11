#!/usr/bin/env python

"""
Annotate users with feature values and label as socks or not socks.

The users are read from stdin, output is to stdout.

"""

from pathlib import Path
import argparse
import datetime
import json
import logging
import sys

import toolforge

class ArchiveError(ValueError):
    pass

SEC_PER_DAY = 60 * 60 * 24
NAMESPACE_USER = 2


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--progress',
                        help='log progress every N suspects',
                        type=int,
                        metavar='N')
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

    logger.info("Starting work, job-name = %s", args.job_name)
    start_time = datetime.datetime.now()

    finder = FeatureFinder(toolforge.connect('enwiki'))

    count = 0
    for line in sys.stdin:
        suspect = json.loads(line)
        finder.find(suspect)
        print(json.dumps(suspect))
        count += 1
        if args.progress and (count % args.progress == 0):
            logger.info("Processed %s suspects", count)
                
    finish_time = datetime.datetime.now()
    elapsed_time = finish_time - start_time
    logger.info("Processed %d suspects in %s", count, elapsed_time)


def configure_logging(log_stream, log_level):
    logging.basicConfig(stream=log_stream,
                        level=log_level,
                        format='%(process)d %(asctime)s [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')


class FeatureFinder():
    """Find suspect features.

    """
    def __init__(self, db):
        self.logger = logging.getLogger('feature-finder')
        self.db = db


    def find(self, suspect):
        """Get the features for a suspected sock.

        Suspect is a dict which is modified in-place.

        Returns None

        """
        # TODO: Only look up user_id once #36

        sock = suspect['sock']
        reg_date = self.get_registration_date(sock)
        if reg_date:
            suspect['reg_time'] = reg_date.isoformat()

        first_contrib_time = self.get_first_contribution_time(sock)
        if first_contrib_time:
            suspect['first_contrib_time'] = first_contrib_time.isoformat()

        if reg_date and first_contrib_time:
            suspect['first_contrib_days'] = (first_contrib_time - reg_date).total_seconds() / SEC_PER_DAY

        count = self.get_live_edit_count(sock)
        if count is not None:
            suspect['live_edit_count'] = count

        count = self.get_deleted_edit_count(sock)
        if count is not None:
            suspect['deleted_edit_count'] = count

        suspect['block_count'] = self.get_block_count(sock)
        suspect['is_sock'] = self.get_is_sock(sock)


    def get_registration_date(self, username):
        """Return a (UTC) datetime if a registration time can be found for the user.
        If no entry can be found in the logs, return None.
        """
        with self.db.cursor() as cur:
            cur.execute("""
            SELECT user_registration
            FROM user
            WHERE user_name = %(username)s
            """, {'username': username})
            rows = cur.fetchall()
            if rows:
                timestamp = rows[0][0]
                if timestamp:
                    return self.wikidb_timestamp_to_datetime(timestamp)


    @staticmethod
    def wikidb_timestamp_to_datetime(ts):
        """Return a (UTC) datetime from a wiki database timestamp string.
        This is a 14-digit string of the form YYYYMMDDHHMMSS,
        i.e. 20191129202516 for 2019 November 29 20:25:16.  See
        https://www.mediawiki.org/wiki/Manual:Timestamp for more
        details.

        """
        parts = (ts[0:4], ts[4:6], ts[6:8], ts[8:10], ts[10:12], ts[12:14])
        yyyymmddhhmmss = list(map(int, parts))
        return datetime.datetime(*yyyymmddhhmmss)


    def get_first_contribution_time(self, sock):
        """Return a (UTC) datetime if the first edit time can be found for the user.
        If no edits can be found, return None.

        Note that if the user's first edit has been deleted, it won't
        be visible here.  In that case, this returns the time of the
        first non-deleted edit.  It's unclear how revdel affects this.

        """
        with self.db.cursor() as cur:
            cur.execute("""
            SELECT rev_timestamp
            FROM revision_userindex
            JOIN actor_revision ON actor_id = rev_actor
            WHERE actor_name = %(sock)s
            ORDER by rev_timestamp ASC
            LIMIT 1
            """, {'sock': sock})
            rows = cur.fetchall()
            if rows:
                timestamp = rows[0][0]
                return self.wikidb_timestamp_to_datetime(timestamp)


    def get_live_edit_count(self, sock):
        with self.db.cursor() as cur:
            # TODO: Use better query for live_edit_count #35
            cur.execute("""
            SELECT user_editcount
            FROM user
            WHERE user_name = %(username)s
            """, {'username': sock})
            row = cur.fetchone()
            if row:
                return row[0]


    def get_deleted_edit_count(self, sock):
        with self.db.cursor() as cur:
            cur.execute("""
            SELECT count(*)
            FROM archive_userindex
            JOIN actor ON ar_actor = actor_id
            WHERE actor_name = %(username)s
            """, {'username': sock})
            row = cur.fetchone()
            return row[0]


    def get_block_count(self, sock):
        '''Returns the number of times the user has been blocked.'''
        with self.db.cursor() as cur:
            cur.execute("""
            SELECT count(*)
            FROM logging_logindex
            WHERE log_namespace = %(namespace)s
              and log_title = %(username)s
              and log_type = 'block'
              and log_action = 'block'
            """, {'namespace': NAMESPACE_USER, 'username': sock})
            row = cur.fetchone()
            return row[0]

    def get_is_sock(self, sock):
        '''Returns True if sock has indeed been blocked as a sock.

        It's surprisingly non-trivial to figure out if this is the
        case.  The current implementation uses the simplistic
        assumption that if the user is mentioned in an SPI case as a
        suspected sock, and they've been indef blocked, then they're a
        sock.  The problem with this is that some suspects are found
        to not be socks but are indef blocked for other reasons.

        '''
        with self.db.cursor() as cur:
            cur.execute("""
            select count(*) from ipblocks
            join actor on ipb_user = actor_user
            where actor_name = %(username)s
              and ipb_expiry = 'infinity'
            """, {'username': sock})
        row = cur.fetchone()
        return bool(row[0])


if __name__ == '__main__':
    main()
