#!/usr/bin/env python3

"""Pick a random collection of control users.

These are users who are not known socks.  For simplicity, we select
users who are not blocked, which may be overly restrictive, since it
eliminates users who have been blocked for reasons other than socking.

"""

import argparse
import datetime
import json
import logging
import random

import toolforge

import config


def main():
    "Main program"
    #pylint: disable=R0914
    parser = argparse.ArgumentParser()
    parser.add_argument('--count',
                        help='Number of users to select',
                        type=int,
                        default=100)
    config.provide_logging_cli(parser)


    args = parser.parse_args()
    config.configure_logging(args)

    logger = logging.getLogger('get_controls')

    logger.info("Starting work, job-name = %s", args.job_name)
    start_time = datetime.datetime.now()

    #pylint: disable=C0103
    db = toolforge.connect('enwiki')

    with db.cursor() as cur:
        cur.execute("select max(user_id) from user")
        row = cur.fetchone()
    max_id = row[0]

    user_count = 0
    duplicate_count = 0
    non_existant_count = 0
    blocked_count = 0
    user_ids = set()
    while user_count < args.count:
        user_id = random.randint(1, max_id)
        if user_id in user_ids:
            duplicate_count += 1
            continue
        user_ids.add(user_id)
        with db.cursor() as cur:
            cur.execute("""
            select user_name
            from user
            where user_id = %(user_id)s
            """, {'user_id': user_id})
            rows = cur.fetchall()
        if not rows:
            non_existant_count += 1
            continue
        user = rows[0][0]
        with db.cursor() as cur:
            cur.execute("select count(*)from ipblocks where ipb_user = %(user_id)s",
                        {'user_id': user_id})
            row = cur.fetchone()
        block_count = row[0]
        if block_count:
            blocked_count += 1
            continue
        record = {'sock': user.decode("utf-8"),
                  'is_sock': False,
                  }
        print(json.dumps(record))
        user_count += 1


    finish_time = datetime.datetime.now()
    elapsed_time = finish_time - start_time
    logger.info("Processed %d users (%d duplicates, %d non-existant, %d blocked) in %s",
                user_count,
                duplicate_count,
                non_existant_count,
                blocked_count,
                elapsed_time)


if __name__ == '__main__':
    main()
