#!/usr/bin/env python

"""
Annotate users with feature values and label as socks or not socks.

The users are read from stdin, output is to stdout.

"""

from pathlib import Path
import argparse
import datetime
import inspect
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
    parser.add_argument('--list_features',
                        action='store_true',
                        help='print all available features and exit')
    parser.add_argument('--feature',
                        action='append',
                        dest='features',
                        help='''Feature to evaluate.  May be repeated.
                        Features are evaluated in
                        the order given; it is the responsibility of
                        the user to include the required dependencies,
                        and in the right order.  If no features are
                        specified, defaults to all features.  See
                        --list-features to get a list of available
                        features.''')

    args = parser.parse_args()

    if args.list_features:
        print_features()
        return

    features = args.features or ['reg_time',
                                 'first_contrib_time',
                                 'first_contrib_interval',
                                 'live_edit_count',
                                 'deleted_edit_count',
                                 'block_count']
    check_valid_features(features)

    config.configure_logging(args)
    logger = logging.getLogger('get_features')

    logger.info("Starting work")
    logger.info("job-name = %s", args.job_name)
    logger.info("Using features: %s", features)
    start_time = datetime.datetime.now()

    db_connection = toolforge.connect('enwiki')

    count = 0
    for line in sys.stdin:
        initial_data = json.loads(line)

        suspect = Suspect(db_connection, initial_data)
        suspect.add_features(features)
        print(json.dumps(suspect.clean_data()))

        count += 1
        if args.progress and (count % args.progress == 0):
            logger.info("Processed %s suspects", count)
                
    finish_time = datetime.datetime.now()
    elapsed_time = finish_time - start_time
    logger.info("Processed %d suspects in %s", count, elapsed_time)


def print_features():
    map = {cls.tag: cls for cls in Feature.subclasses()}
    for tag in sorted(map.keys()):
        deps = map[tag].dependencies
        if deps:
            print('%s (depends on %s)' % (tag, ', '.join(d.tag for d in deps)))
        else:
            print('%s' % tag)


def check_valid_features(features):
    valid_features = set(cls.tag for cls in Feature.subclasses())
    for feature in features:
        if feature not in valid_features:
            print("Invalid feature: %s" % feature)
            sys.exit(1)


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


    def clean_data(self):
        """Returns a clean copy of the suspect's data.

        Internally, missing feature values are indicated by either the
        key not being in the data dict at all, or the value being
        None.  This returns a copy of the internal data with all the
        None values elided.

        """
        return {k: v for k, v in self.data.items() if v is not None}


    def add_features(self, features):
        """Update the suspect's data with all requested freatures, if possible.
        Some features may require information which is unavailable, in
        which case the coresponding keys are set to None.

        Returns None.

        """
        # TODO: Only look up user_id once #36
        feature_map = {cls.tag: cls for cls in Feature.subclasses()}
        for key in features:
            cls = feature_map[key]
            self.data[key] = cls(self.db, self.data).eval()


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
    def subclasses():
        "Iterates over all the Feature subclasses."
        module = inspect.getmodule(Feature)
        for name, member in inspect.getmembers(module, inspect.isclass):
            if issubclass(member, Feature) and member != Feature:
                yield member


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
    """When the user registered, as a POSIX timestamp.

    """
    dependencies = set()
    tag = 'reg_time'
    def eval(self):
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
    """First edit time, as a POSIX timestamp.

    Note that if the user's first edit has been deleted, it won't be
    visible here.  In that case, this returns the time of the first
    non-deleted edit.  It's unclear how revdel affects this.

    TODO: first_contrib_time should check for deleted edits #50

    """
    dependencies = set()
    tag = 'first_contrib_time'
    def eval(self):
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


class FirstContribInterval(Feature):
    """Time between when a user registered and they made their first edit,
    in seconds.

    """
    dependencies = set([RegistrationTime, FirstContributionTime])
    tag = 'first_contrib_interval'
    def eval(self):
        if self.data.get('first_contrib_time') and self.data.get('reg_time'):
            return self.data['first_contrib_time'] - self.data['reg_time']


class LiveEditCount(Feature):
    """Number of live (i.e. non-deleted) edits.

    """
    dependencies = set()
    tag = 'live_edit_count'
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
    """Number of edits that have been deleted.

    """
    dependencies = set()
    tag = 'deleted_edit_count'
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
    """Number of times this user has been blocked, regardless of whether
    any blocks are currently active.

    """
    dependencies = set()
    tag = 'block_count'
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
