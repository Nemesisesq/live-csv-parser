import csv
import json
import re
from os.path import expanduser, join, isfile

from os import listdir

def servmatch(s):
    if 'sling' in s:
        return 'Sling'
    if 'vue' in s:
        return 'Playstation Vue'
    if 'espn' in s:
        return "Watch ESPN"


class X(object):
    def convert_files(self):
        mypath = expanduser('~') + '/Dropbox/streamsavvy_live_data'
        mypath

        onlyfiles = [join(mypath, f) for f in listdir(mypath) if isfile(join(mypath, f))]

        csvz = [s for s in onlyfiles if re.search('csv', s)]

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
