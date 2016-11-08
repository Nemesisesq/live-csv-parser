# coding: utf-8

# In[207]:
import os
from os import listdir
from os.path import isfile, join, expanduser
import re
import csv
import json
from pymongo import MongoClient

mypath = expanduser('~') + '/Dropbox/streamsavvy_live_data'
mypath

onlyfiles = [join(mypath, f) for f in listdir(mypath) if isfile(join(mypath, f))]

csvz = [s for s in onlyfiles if re.search('csv', s)]


def servmatch(s):
    if 'sling' in s:
        return 'sling'
    if 'vue' in s:
        return 'playstation vue'


for f in csvz:
    with open(f, newline='') as csvfile:
        print(f)
        r = csv.DictReader(csvfile)
        l = [s for s in r]

        j = f.replace('csv', 'json')

        try:
            for x in l:
                if 'services' in x:
                    x['services'] = [r for r in x["services"].split(' ') if r]

                    x['services'] = [{"service": r, "app": servmatch(r)} for r in x['services']]
        except Exception as e:
            print(e)

        with open(j, 'w') as json_file:
            json.dump(l, json_file)

# In[208]:

price_file = open(join(mypath, 'service_prices.json'))
prices = json.load(price_file)

channel_file = open(join(mypath, 'live_network-service_match.json'))
chans = json.load(channel_file)

# In[209]:

for c in chans:
    for s in c['services']:
        for p in prices:
            if s['service'] == p['service']:
                s['price'] = p

# In[210]:

final_file = open('final_file.json', 'w')
json.dump(chans, final_file)
final_file.close()

# In[232]:

url = 'mongodb://heroku_7c4hfzsn:f1crnk27vi7l6uaetdvfhgriop@ds139665.mlab.com:39665/heroku_7c4hfzsn'
from urllib.parse import urlparse

parsed = urlparse(url)

# In[233]:

# client = MongoClient(url)
# db = client[parsed.path[1:]]
# collection = db.live_streaming_services

# In[229]:

# result = collection.insert_many(chans)

# In[223]:

# result.inserted_ids


# In[230]:

noti_file = open(join(mypath, 'live_notification_content.json'))
notices = json.load(noti_file)

# result = db.notifications.insert_many(notices)

# In[231]:

for chan in chans:
    for serv in chan["services"]:
        for notice in notices:
            if serv['service'] in notice["versions"]:
                serv["template"] = notice


fp = open('result.json', 'w')
json.dump(chans, fp)
fp.close()




from urllib.parse import urlparse

url = os.environ["MONGODB_URI"]
parsed = urlparse(url)

# In[233]:

client = MongoClient(url)
db = client[parsed.path[1:]]
collection = db.live_streaming_services

collection.remove()

result = collection.insert_many(chans)


print(result.inserted_ids)
