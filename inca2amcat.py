#!/usr/bin/env python3
import json
from tqdm import tqdm
from amcat4apiclient.amcat4apiclient import AmcatClient


amcat = AmcatClient("https://tux01ascor.fmg.uva.nl/amcat4server", "admin", "whatever")
indices = amcat.list_indices()
for index in indices:
    print(index)

    

with open('TEST.json') as f:
    data = [json.loads(line) for line in f]
 

def chunker(x, chunksize = 100):
    for i in range(0, len(x), chunksize):
        slice_item = slice(i, i + chunksize, 1)
        yield x[slice_item]


def cleanart(art):
    art['date'] = art.pop('publication_date','1900-01-01')
    if 'text' not in art:
        art['text']=''
    return art

# data2 = [cleanart(art) for art in data]

for chunk in tqdm(chunker(data)):
    cleanchunk = [cleanart(art) for art in chunk]
    r = amcat.upload('incatransfer', cleanchunk)
    print(r.status_code)
