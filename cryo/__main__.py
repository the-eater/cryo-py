"""
cryo - chilling state manager

Usage:
    cryo web [options]
    cryo consumer [options]
    cryo cli ...
    cryo [options]

Options:
    --version -v    Add more verbosity
    --help -h       Show this help
    --config=<value> -c Use given config file
"""
from docopt import docopt
from .web import Runtime as WebRuntime
from . import Config, CLI
import sys

if __name__ == '__main__':
    if sys.argv[1] == 'cli':
        exit(CLI().run(sys.argv))

    arguments = docopt(__doc__, version='cryo 1.0.0')

    if arguments['web']:
        exit(WebRuntime.run(arguments))

