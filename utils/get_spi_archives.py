#!/usr/bin/env python

"""Utility to retrieve SPI case archives.

SPI archives under [[WP:Sockpuppet investigations/<master>/Archive]]
are retrieved.  The wikitext for each case is stored in a file named
for the master, encoded with urllib.parse.quote_plus().

Only cases where the master is a registered account are retrieved.  It
is expensive to check to see if a username is registered, so
heuristics are used to filter out what appear to be anonymous users,
i.e. either IPv4 or IPv6 addresses.

"""

import argparse
import re
import urllib.parse
from pathlib import Path
from pprint import pprint

import mwclient

SITE = 'en.wikipedia.org'
ARCHIVE_PATTERN = re.compile(r'^Sockpuppet investigations/(.*)/Archive$')

IPV4_PATTERN = re.compile(r'''\d{1,3}
                              [.]\d{1,3}
                              [.]\d{1,3}
                              [.]\d{1,3}''', re.VERBOSE)

IPV6_PATTERN = re.compile(r'''^[0-9a-fA-F]{1,4}
                              :[0-9a-fA-F]{1,4}
                              :[0-9a-fA-F]{1,4}
                              :[0-9a-fA-F]{1,4}
                              :[0-9a-fA-F]{1,4}
                              :[0-9a-fA-F]{1,4}
                              :[0-9a-fA-F]{1,4}
                              :[0-9a-fA-F]{1,4}$''', re.VERBOSE)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir',
                        help='Directory in which to write output files',
                        required=True)
    parser.add_argument('--limit',
                        help='maximum number of cases to download',
                        type=int)
    parser.add_argument('--progress',
                        help='print progress indicator every N pages',
                        type=int,
                        metavar='N',
                        default=100)
    parser.add_argument('--verbose',
                        help='print verbose status information',
                        action='store_true')

    args = parser.parse_args()

    site = mwclient.Site(SITE)
    namespaces = {v: k for k, v in site.namespaces.items()}

    archive_count = 0
    page_count = 0
    ipv4_count = 0
    ipv6_count = 0
    for page in site.allpages(prefix='Sockpuppet investigations',
                              namespace=namespaces['Wikipedia']):
        if args.verbose:
            print('examining', page.page_title)

        page_count += 1
        if args.progress and (page_count % args.progress == 0):
            print('examined %d pages, %d IPv4, %d IPv6, wrote %d archives' %
                  (page_count, ipv4_count, ipv6_count, archive_count))

        m = ARCHIVE_PATTERN.match(page.page_title)
        if not m:
            if args.verbose:
                print('rejecting non-case')
            continue
        username = m.group(1)
        if IPV4_PATTERN.match(username):
            ipv4_count += 1
            if args.verbose:
                print('rejecting IPv4')
            continue
        if IPV6_PATTERN.match(username):
            ipv6_count += 1
            if args.verbose:
                print('rejecting IPv6')
            continue

        path = Path(args.dir, urllib.parse.quote_plus(username))
        if args.verbose:
            print('writing', path)
        with path.open('w') as out:
            print(page.text(), file=out)
        
        archive_count += 1
        if args.limit and (archive_count >= args.limit):
            break

if __name__ == '__main__':
    main()
