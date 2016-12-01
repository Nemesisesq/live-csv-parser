# coding: utf-8

# In[207]:
import csv
import json
import os
import re
from os import listdir
from os.path import isfile, join, expanduser

from fuzzywuzzy import fuzz
from pymongo import MongoClient
from titlecase import titlecase

from convert_csc_to_json import X

mypath = expanduser('~') + '/Dropbox/streamsavvy_live_data'


# In[208]:

X().convert_files()

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
chans


final_file = open('final_file.json', 'w')
json.dump(chans, final_file)
final_file.close()

# In[232]:

url = 'mongodb://heroku_7c4hfzsn:f1crnk27vi7l6uaetdvfhgriop@ds139665.mlab.com:39665/heroku_7c4hfzsn'
from urllib.parse import urlparse

parsed = urlparse(url)
parsed
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

y = None

x = None

for i in notices:
    if i['app_name'] == 'single_tier':
        x = i
    if i['app_name'] == 'sling_extra':
        y = i

#
s_list = x["service_name"].split(" ")

ex_list = y["versions"].split(" ")
#
def make_single_tier_notice(n, s):
    n["service_name"] = s
    n["app"] = titlecase(" ".join(s.split("_")).replace("Hbo", "HBO"))
    n["app_name"] = s

    return n

s_list = [make_single_tier_notice(dict(x), s) for s in s_list]

s_list

def make_sling_extra_notice(param, e):
    param["version"] = e
    param["versions"] = e
    param["app"] = titlecase(" ".join(e.split("_")))
    param['service_name'] = e
    param["app_identifier"] = "sling"
    return param


ex_list =[make_sling_extra_notice (dict(y), e ) for e in ex_list]

ex_list

s_list += ex_list
notices += s_list

with open('mNotice.json', 'w') as f:
    json.dump(notices,f)


for chan in chans:
    for serv in chan["services"]:
        serv["link"] = ""

        if re.match(r"ps_vue", serv["service"]):
            serv["app_identifier"] = "ps_vue"

        elif re.match(r"sling", serv["service"]):
            serv["app_identifier"] = "sling"

        else:
            serv["app_identifier"] = serv["service"]

        if not serv["app"] :
            serv["app"] = titlecase(" ".join(serv["app_identifier"].split("_")))
            serv["app"] = serv["app"].replace("Tv", "TV")
            serv["app"] = serv["app"].replace("Hbo", "HBO")
            serv["app"] = serv["app"].replace("Cbs", "CBS")

        for notice in notices:
            if serv['service'] in notice["versions"] :
                serv["template"] = notice
                if serv['app'] == "":
                    serv['app'] = titlecase(notice['app_name'])



            elif serv['service'] == notice["service_name"]:
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
            elif fuzz.partial_ratio(serv["app_identifier"], link["app_name"]) > 90:
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

print(result.inserted_ids)

