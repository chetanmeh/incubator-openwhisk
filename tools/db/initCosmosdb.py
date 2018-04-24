#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from collections import namedtuple
import glob
import sys
import os
import argparse
import pydocumentdb.errors as document_errors
import pydocumentdb.document_client as document_client

try:
    import argcomplete
except ImportError:
    argcomplete = False

CLI_DIR = os.path.dirname(os.path.realpath(sys.argv[0]))
# ROOT_DIR is the repository root
ROOT_DIR = os.path.join(os.path.join(CLI_DIR, os.pardir), os.pardir)

DbContext = namedtuple('DbContext', ['client', 'db', 'whisks', 'subjects', 'activations'])


def main():
    args = parse_args()
    (client, db) = init_client(args)

    whisks = init_coll(client, db, "whisks")
    subjects = init_coll(client, db, "subjects")
    activations = init_coll(client, db, "activations")

    db_ctx = DbContext(client, db, whisks, subjects, activations)
    init_auth(db_ctx)


def parse_args():
    parser = argparse.ArgumentParser(description='OpenWhisk CosmosDB bootstrap tool')
    parser.add_argument('--endpoint', help='DB Endpoint url like https://example.documents.azure.com:443/',
                        required=True)
    parser.add_argument('--key', help='DB access key', required=True)
    parser.add_argument('--db', help='Database name under which the collections would be created', required=True)
    if argcomplete:
        argcomplete.autocomplete(parser)
    return parser.parse_args()


def init_auth(ctx):
    for subject in find_default_subjects():
        link = create_link(ctx.db, ctx.subjects, subject['id'])
        try:
            ctx.client.ReadDocument(link)
            print 'Subject already exists : ' + subject['id']
        except document_errors.HTTPFailure, e:
            if e.status_code == 404:
                ctx.client.CreateDocument(ctx.subjects['_self'], subject)
                print 'Created subject : ' + subject['id']
            else:
                raise e


def create_link(db, coll, doc_id):
    return 'dbs/' + db['id'] + '/colls/' + coll['id'] + '/docs/' + doc_id


def find_default_subjects():
    files_dir = os.path.join(ROOT_DIR, "ansible/files")
    for name in glob.glob1(files_dir, "auth.*"):
        auth_file = open(os.path.join(files_dir, name), 'r')
        uuid, key = auth_file.read().strip().split(":")
        subject = name[name.index('.') + 1:]
        doc = {
            'id': subject,
            'subject': subject,
            'namespaces': [
                {
                    'name': subject,
                    'uuid': uuid,
                    'key': key
                }
            ]
        }
        auth_file.close()
        yield doc


def init_client(args):
    client = document_client.DocumentClient(args.endpoint, {'masterKey': args.key})
    query = client.QueryDatabases('SELECT * FROM root r WHERE r.id=\'' + args.db + '\'')
    it = iter(query)

    db = next(it, None)
    if db is None:
        db = client.CreateDatabase({'id': args.db})
        print('Created database "%s"' % args.db)
    return client, db


def init_coll(client, db, coll_name):
    query = client.QueryCollections(db['_self'], 'SELECT * FROM root r WHERE r.id=\'' + coll_name + '\'')
    it = iter(query)
    coll = next(it, None)
    if coll is None:
        collection_definition = {'id': coll_name}
        collection_options = {}  # {'offerThroughput': 10100}
        coll = client.CreateCollection(db['_self'], collection_definition, collection_options)
        print('Created collection "%s"' % coll_name)
    return coll


if __name__ == '__main__':
    main()
