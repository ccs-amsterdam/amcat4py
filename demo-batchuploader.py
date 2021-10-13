#!/usr/bin/env python3
'''
Uploads multiple (gzipped or not) JSON files to AmCAT
'''


import json
import gzip
import argparse
import os
from typing import List, Dict
from tqdm import tqdm
from glob import glob
from amcat4apiclient.amcat4apiclient import AmcatClient



def _chunker(x, chunksize = 100):
    for i in range(0, len(x), chunksize):
        slice_item = slice(i, i + chunksize, 1)
        yield x[slice_item]


def _cleandoc(doc: dict):
    '''
    Ensures that document conforms to AmCAT requirements
    '''
    # rename 'publication_date' to 'date'; handle missing dates
    doc['date'] = doc.pop('publication_date','1900-01-01')
    # handle missing text
    if 'text' not in doc: doc['text']=''
    return doc


        
def read_file(fn, jsonlines=True):
    if not jsonlines:
        raise NotImplementedError("Still need to import logic to support both JSON and JSON-lines")

    if fn[-3:].lower()=='.gz':
        with gzip.open(fn, "rb") as f:
            data = [json.loads(line) for line in f]
    else:
        with open(fn, "rb") as f:
            data = [json.loads(line) for line in f]
    return data
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__,
                                     epilog = "Set the environment variables AMCATUSER and AMCATPASSWORD to use non-default credentials")
    parser.add_argument('index', help="The name of the index ('project') to upload to")
    parser.add_argument('url', default='http://127.0.0.1:5000', help='The address of the AmCAT server')
    parser.add_argument('files', help='Glob pattern of json(.gz) files')

    args = parser.parse_args()
    
    user = os.environ.get("AMCATUSER","admin")
    passwd = os.environ.get("AMCATPASSWORD","admin")

    
    amcat = AmcatClient(args.url, user, passwd)


    allfiles = glob(args.files)

    for fn in tqdm(allfiles):
        print(f"Processing {fn}...")
        data = read_file(fn)

        for chunk in tqdm(_chunker(data)):
            cleanchunk = [_cleandoc(art) for art in chunk]
            r = amcat.upload('incatransfer', cleanchunk)
            print(r.status_code)
