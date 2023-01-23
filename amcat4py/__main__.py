"""
AmCAT4 API Client for Python
"""

import argparse
import logging

from amcat4py import AmcatClient
from amcat4py.copy_index import copy_documents


def index_list(client: AmcatClient, _args):
    for index in client.list_indices():
        print(index)


def index_create(client: AmcatClient, args):
    client.create_index(args.name)


def index_delete(client: AmcatClient, args):
    client.delete_index(args.name)


def index_copy(client: AmcatClient, args):
    ignore_fields = args.ignore_fields and args.ignore_fields.split(",")
    copy_documents(client, args.src, client, args.dest, ignore_fields)


def run_action(args):
    client = AmcatClient(args.host, args.username, args.password)
    args.func(client, args)


logging.basicConfig(format='[%(levelname)-7s:%(name)-15s] %(message)s', level=logging.INFO)

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

p = subparsers.add_parser('copy', help='Copy index')
p.add_argument("src", help="Index to copy from")
p.add_argument("dest", help="Index to copy to")
p.add_argument("--ignore-fields", help="Comma separated list of fields to ignore")
p.set_defaults(func=index_copy)

run_action(parser.parse_args())
