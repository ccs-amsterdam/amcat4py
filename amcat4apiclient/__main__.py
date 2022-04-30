"""
AmCAT4 API Client for Python
"""

import argparse

from amcat4apiclient import AmcatClient


def index_list(client: AmcatClient, _args):
    for index in client.list_indices():
        print(index)


def index_create(client: AmcatClient, args):
    client.create_index(args.name)


def index_delete(client: AmcatClient, args):
    client.delete_index(args.name)


def run_action(args):
    client = AmcatClient(args.host, args.username, args.password)
    args.func(client, args)


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--host", default="http://localhost:5000")
parser.add_argument("--username", default="admin")
parser.add_argument("--password", default="admin")
subparsers = parser.add_subparsers(dest='action', title='action', required=True)

subparsers.add_parser('list', help='List indices').set_defaults(func=index_list)
p = subparsers.add_parser('create', help='Create index')
p.add_argument("name", help="New index name")
p.set_defaults(func=index_create)

p = subparsers.add_parser('delete', help='Delete index')
p.add_argument("name", help="Index to delete")
p.set_defaults(func=index_delete)

run_action(parser.parse_args())
