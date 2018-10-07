"""
cryo-cli - CLI util

Usage:
    cryo-cli queue [options] <input>
    cryo-cli get-job [options]
    cryo-cli complete [options] <id> [<output>]
    cryo-cli get-output [options] <id>
    cryo-cli wipe [options] <id>

Options:
    --config=<value> Select config file
"""
from docopt import docopt
from . import Config
from .queue import Queue


class CLI:
    def __init__(self):
        self.config = None

    def run(self, argv):
        args = docopt(__doc__, argv=argv[2:])

        self.config = Config.load(args['--config'] if '--config' in args and args['--config'] else 'config/cryo.toml')

        if args['queue']:
            self.queue(args)

        if args['get-job']:
            self.get_job(args)

        if args['complete']:
            self.complete(args)

        if args['get-output']:
            self.get_output(args)

        if args['wipe']:
            self.wipe(args)

    def get_queue(self):
        etcd_client = self.config.get_etcd_client()
        return Queue(etcd_client)

    def queue(self, args):
        queue = self.get_queue()
        print(queue.add_item(args['<input>']))

    def get_job(self, _):
        queue = self.get_queue()
        print(queue.select())

    def complete(self, args):
        queue = self.get_queue()
        print(queue.complete(args['<id>'], args['<output>']))

    def get_output(self, args):
        queue = self.get_queue()
        print(queue.get_output(args['<id>']))

    def wipe(self, args):
        queue = self.get_queue()
        print(queue.wipe(args['<id>']))