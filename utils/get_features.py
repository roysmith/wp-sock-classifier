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
from types import MappingProxyType

import toolforge

import config

class ArchiveError(ValueError):
    pass

SEC_PER_DAY = 60 * 60 * 24
NAMESPACE_USER = 2


def main():
    parser = argparse.ArgumentParser(parents=[config.logging_cli()])
    parser.add_argument('--progress',
                        help='log progress every N suspects',
                        type=int,
                        metavar='N')
    
    args = parser.parse_args()
    config.configure_logging(args)
    logger = logging.getLogger('get_features')

    logger.info("Starting work")
    logger.info("job-name = %s", args.job_name)
    start_time = datetime.datetime.now()

    db_connection = toolforge.connect('enwiki')

    count = 0
    for line in sys.stdin:
        initial_data = json.loads(line)
        suspect = Suspect(db_connection, initial_data)
        suspect.add_all_features()
        print(json.dumps(suspect.data))
        count += 1
        if args.progress and (count % args.progress == 0):
            logger.info("Processed %s suspects", count)
                
    finish_time = datetime.datetime.now()
    elapsed_time = finish_time - start_time
    logger.info("Processed %d suspects in %s", count, elapsed_time)


class Suspect:
    """A suspected sock.

    """
    def __init__(self, db, initial_data):
        """Db is a database connection.

        Initial_data is a dict containing some initially known data
        about the suspect.  The passed-in dict is not modified.

        """
        self.logger = logging.getLogger('get_features.suspect')
        self.db = db
        self.data = initial_data.copy()


    def add_all_features(self):

        """Update the suspect's data with all the known features, if possible.
        Some features may require information which is unavailable, in
        which case the coresponding keys are left unset.

        Returns None.

        """
        # TODO: Only look up user_id once #36

        reg_time = RegistrationTime(self.db, self.data).eval()
        if reg_time:
            self.data['reg_time'] = reg_time

        first_contrib_time = FirstContributionTime(self.db, self.data).eval()
        if first_contrib_time:
            self.data['first_contrib_time'] = first_contrib_time

        if reg_time and first_contrib_time:
            self.data['first_contrib_interval'] = (first_contrib_time - reg_time)

        count = LiveEditCount(self.db, self.data).eval()
        if count is not None:
            self.data['live_edit_count'] = count

        count = DeletedEditCount(self.db, self.data).eval()
        if count is not None:
            self.data['deleted_edit_count'] = count

        self.data['block_count'] = BlockCount(self.db, self.data).eval()


class Feature:
    """Each subclass of this represents a single feature.

    The eval() method knows how to gather whatever input data it needs
    (including from other previously-evaluated features).  If any of
    the required data is unavailable, it returns None.

    """
    def __init__(self, db, initial_data):
        self.db = db
        self.data = MappingProxyType(initial_data)


    @staticmethod
    def wikidb_timestamp_to_posix(ts):
        """Return a POSIX timestamp from a wiki database timestamp
        string.  The later is a 14-digit string of the form
        YYYYMMDDHHMMSS, i.e. 20191129202516 for 2019 November 29
        20:25:16.  See https://www.mediawiki.org/wiki/Manual:Timestamp
        for more details.

        Note: the returned value is an interger.

        """
        parts = (ts[0:4], ts[4:6], ts[6:8], ts[8:10], ts[10:12], ts[12:14])
        yyyymmddhhmmss = list(map(int, parts))
        dt = datetime.datetime(*yyyymmddhhmmss)
        return int(dt.timestamp())


class RegistrationTime(Feature):
    def eval(self):
        """Return when the user registered, as a POSIX timestamp.

        """
        with self.db.cursor() as cur:
            cur.execute("""
            SELECT user_registration
            FROM user
            WHERE user_name = %(username)s
            """, {'username': self.data['user']})
            rows = cur.fetchall()
            if rows:
                timestamp = rows[0][0]
                if timestamp:
                    # unclear if it's possible for this branch not to be taken.
                    return self.wikidb_timestamp_to_posix(timestamp)


class FirstContributionTime(Feature):
    def eval(self):
        """Return a POSIX timestamp if the first edit time can be found for
        the user.

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
            """, {'sock': self.data['user']})
            rows = cur.fetchall()
            if rows:
                timestamp = rows[0][0]
                return self.wikidb_timestamp_to_posix(timestamp)


class LiveEditCount(Feature):
    def eval(self):
        with self.db.cursor() as cur:
            # TODO: Use better query for live_edit_count #35
            cur.execute("""
            SELECT user_editcount
            FROM user
            WHERE user_name = %(username)s
            """, {'username': self.data['user']})
            row = cur.fetchone()
            if row:
                return row[0]


class DeletedEditCount(Feature):
    def eval(self):
        with self.db.cursor() as cur:
            cur.execute("""
            SELECT count(*)
            FROM archive_userindex
            JOIN actor ON ar_actor = actor_id
            WHERE actor_name = %(username)s
            """, {'username': self.data['user']})
            row = cur.fetchone()
            return row[0]


class BlockCount(Feature):
    def eval(self):
        '''Returns the number of times the user has been blocked.'''
        with self.db.cursor() as cur:
            cur.execute("""
            SELECT count(*)
            FROM logging_logindex
            WHERE log_namespace = %(namespace)s
              and log_title = %(username)s
              and log_type = 'block'
              and log_action = 'block'
            """, {'namespace': NAMESPACE_USER, 'username': self.data['user']})
            row = cur.fetchone()
            return row[0]



if __name__ == '__main__':
    main()
