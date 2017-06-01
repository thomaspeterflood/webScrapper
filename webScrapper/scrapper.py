'''
Created on 31 May 2017

@author: Thomas Flood
'''

import datetime
import time
import traceback

import requests
import simplejson as json
from sqlalchemy import create_engine

from scrapperPackage import configure

# Recommendation to install mysqlclient from here: http://stackoverflow.com/questions/39574813/error-loading-mysqldb-module-no-module-named-mysqldb

engine = create_engine("mysql://{}:{}@{}:{}/{}".format(configure.USER, configure.PASSWORD, configure.URI, configure.PORT, configure.DB), echo=True)

r = requests.get(configure.STATIONS, params={"apiKey": configure.JCD_APIKEY, "contract": configure.NAME})

def write_to_timestampfile(text):
    now = datetime.datetime.now()
    with open('saved_data/bikes_by_timestamp_{}'.format(now).replace(" ", "_").replace(":", "-"), "w") as f:
        f.write(text)

def write_to_singlefile(text):
    with open('saved_data/all_bike_data', "a") as g:
        g.write(text + "\n")

def dystations_to_db(text):
        dystations = json.loads(text)
        print(type(dystations), len(dystations))
        for dynamic in dystations:
            timestvalue=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(dynamic.get('last_update')/1000))
            print(dynamic)
            valsD = (
            int(dynamic.get('bike_stands')), int(dynamic.get('available_bike_stands')), int(dynamic.get('available_bikes')), timestvalue,
            int(dynamic.get('number')), dynamic.get('status'), int(dynamic.get('banking'))
            )
            engine.execute("""insert into dynamic values(%s,%s,%s,%s,%s,%s,%s)""", valsD,)

def main():
    while True:
        try:
            r = requests.get(configure.STATIONS, params={"apiKey": configure.JCD_APIKEY, "contract": configure.NAME})
            now = datetime.datetime.now()
            print(r, now)
            write_to_timestampfile(r.text)
            write_to_singlefile(r.text)
            dystations_to_db(r.text)
            time.sleep(300)
        except:
            print(traceback.format_exc())
            if engine is None:
                return

if __name__ == "__main__":
    main()