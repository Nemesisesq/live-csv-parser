# coding: utf-8

# In[207]:
import csv
import json
import os
import re
from os import listdir
from os.path import isfile, join, expanduser

from pymongo import MongoClient
from titlecase import titlecase

mypath = expanduser('~') + '/Dropbox/streamsavvy_live_data'
mypath

onlyfiles = [join(mypath, f) for f in listdir(mypath) if isfile(join(mypath, f))]

csvz = [s for s in onlyfiles if re.search('csv', s)]


def servmatch(s):
    if 'sling' in s:
        return 'Sling'
    if 'vue' in s:
        return 'Playstation Vue'
    if 'espn' in s:
        return "Watch ESPN"


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

link_file = open(join(mypath, 'live_service_deeplinks.json'))
links = json.load(link_file)

# result = db.notifications.insert_many(notices)

# In[231]:

x = [d for d in notices if d['app_name'] == 'single_tier'][0]

slist = x["service_name"].split(" ")

def make_notice (n, s):
    n["service_name"] = s
    n["app"] = titlecase(s)

    return n

newlist = [make_notice(x, s) for s in slist]

notices += newlist

for chan in chans:
    for serv in chan["services"]:
        serv["link"] = ""

        if re.match(r"ps_vue", serv["service"]):
            serv["app_identifier"] = "ps_vue"

        elif re.match(r"sling", serv["service"]):
            serv["app_identifier"] = "sling"

        else:
            serv["app_identifier"] = serv["service"]

        for notice in notices:
            if serv['service'] in notice["versions"] :
                serv["template"] = notice
                if serv['app'] == "":
                    serv['app'] = titlecase(notice['app_name'])



            elif notice["app_name"] == 'single_tier':
                serv["template"] = notice
                if serv['app'] == "":
                    serv['app'] = titlecase(notice['app_name'])
                    print(serv['app'])

        for link in links:

            link_re = re.compile(link["app_name"], flags=re.I)
            if serv["app"] and link_re.match(serv["app"]):
                serv["link"] = link
            elif serv["app"] == "Playstation Vue" and link["app_name"] == 'ps_vue':
                serv["link"] = link
            elif serv["app"] == 'Watch ESPN' and link["app_name"] == 'watchespn':
                serv["link"] = link
            elif serv["service"] == 'cbs_all_access' and link['app_name'] == 'cbs_all_access':
                serv["link"] = link

fp = open('result.json', 'w')
json.dump(chans, fp)
fp.close()

from urllib.parse import urlparse

# url = os.environ["MONGODB_URI"]
url = os.environ["REMOTE"]
# url = os.environ["concur"]
parsed = urlparse(url)

# In[233]:

client = MongoClient(url)
db = client[parsed.path[1:]]
collection = db.live_streaming_services

collection.remove()

result = collection.insert_many(chans)

print(len(result.inserted_ids))
