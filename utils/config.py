"""
Common functionality for all sock-classifier programs.
"""

import argparse
import logging
from pathlib import Path

def provide_logging_cli(parser):
    """Provide the common CLI arguments for logging.

    Parser is an ArgumentParser instance, which is mutated in-place.

    """
    group = parser.add_argument_group(title="logging")
    group.add_argument('--log',
                       help='File to write log messages',
                       type=argparse.FileType('a'),
                       default=str(Path.home() / 'logs/sock-classifier.log'))
    group.add_argument('--job-name',
                       help='job name, used for status reporting')
    group.add_argument('--log-level',
                       help='Logging level',
                       choices=['debug', 'info', 'warning', 'error'],
                       default='info')

def configure_logging(cli_args):
    """Configure logging.

    cli_args is a set of parsed CLI arguments, as returned by
    ArgumentParser.parse_args().

    """
    logging.basicConfig(stream=cli_args.log,
                        level=cli_args.log_level.upper(),
                        format='%(asctime)s %(levelname)s [%(name)s %(process)s] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
