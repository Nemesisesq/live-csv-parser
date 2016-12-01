import json
import os
import re
from os.path import expanduser, join

from pymongo import MongoClient

mypath = expanduser('~') + '/Dropbox/streamsavvy_live_data'
with open(join(mypath, 'sling_dma_zips_for_networks.json'), 'r') as f:
    zips = json.load(f)
    new = []

    for obj in zips:
        network = obj['chan']
        zips = []

        for k, v in obj.items():
            if re.match(r'zip', k):
                zips.append(v)

        new.append({
            'network': network,
            'zip_codes': zips
        })

    from urllib.parse import urlparse

    url = os.environ["MONGODB_URI"]
    # url = os.environ["REMOTE"]
    # url = os.environ["concur"]
    parsed = urlparse(url)

    # In[233]:

    client = MongoClient(url)
    db = client[parsed.path[1:]]
    collection = db.dma_zips

    collection.remove()

    result  = collection.insert_many(new)

    print(result.inserted_ids)
