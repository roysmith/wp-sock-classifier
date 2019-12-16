#!/usr/bin/env python3

"""Promote a file from a job directory to the data directory.

The file is moved, and a symlink left behind, pointing to the new location.

"""

from pathlib import Path
import argparse
import re
import sys

def main():
    "Main program"
    parser = argparse.ArgumentParser()
    parser.add_argument('job_dir',
                        help='Directory of job to promote.',
                        type=directory_path)
    args = parser.parse_args()

    data_dir = Path.home() / 'sock-classifier' / 'data'

    job_name = args.job_dir.name
    job_type = job_name.split('.')[0]
    source = (args.job_dir / job_type).with_suffix('.out')
    destination = data_dir / job_name.split('_')[1]

    if not source.is_file():
        print("%s is not a file, aborting" % source, file=sys.stderr)
        sys.exit(1)

    if source.is_symlink():
        print("%s is a symlink, aborting" % source, file=sys.stderr)
        sys.exit(1)

    if destination.exists():
        print("%s exists, won't overwrite" % destination, file=sys.stderr)
        sys.exit(1)
    
    print("moving %s -> %s" % (source, destination))
    source.rename(destination)
    destination.chmod(0o444)
    source.symlink_to(destination)
    

def directory_path(arg):
    "Type filter for argparse.add_argument().  Returns a Path object."
    path = Path(arg)
    if not path.is_dir():
        raise argparse.ArgumentTypeError('not a directory')
    return path



if __name__ == '__main__':
    main()
