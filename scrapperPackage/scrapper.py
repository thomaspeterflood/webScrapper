'''
Created on 31 May 2017

@author: apple
'''
import functools

from flask import Flask, g, render_template, jsonify
from sqlalchemy import create_engine

import requests
from scrapperPackage import configure


app = Flask(__name__, static_url_path='/static')

#r = requests.get(configa.OW_URL, params={"id": configa.OW_CITYID, "units": configa.OW_UNITS, "appid": configa.OW_APIKEY})

def connect_to_database(): 
    db_str = "mysql://{}:{}@{}:{}/{}"
    engine = create_engine(db_str.format(configure.USER, configure.PASSWORD, configure.URI, configure.PORT, configure.DB), echo=True)
    return engine


def get_db(): 
    engine = getattr(g, 'engine', None)                                                                                                                                                                                                                              
    if engine is None:                                                                                                                                                                                                                                                  
        engine = g.engine = connect_to_database()                                                                                                                                                                                                                    
    return engine


@app.route('/')
def index():
    return render_template("index.html")


@app.route("/stations") 
@functools.lru_cache(maxsize=128) 
def get_stations(): 
    engine = get_db() 
    sql = "select * from static;"
    rows = engine.execute(sql).fetchall() 
    print('#found {} stations', len(rows)) 
    return jsonify(stations=[dict(row.items()) for row in rows])


@app.route("/occupancy") 
@functools.lru_cache(maxsize=128) 
def get_occupancy(): 
    engine = get_db() 
    sql = "select * from dynamic ORDER BY last_update DESC LIMIT 250;"
    rows = engine.execute(sql).fetchall()    
    print('#found {} occupancy', len(rows)) 
    return jsonify(occupancy=[dict(row.items()) for row in rows]) 



@app.route("/openweather") 
@functools.lru_cache(maxsize=128) 
def get_weather(): 
    r = requests.get(configure.OW_URL, params={"id": configure.OW_CITYID, "units": configure.OW_UNITS, "appid": configure.OW_APIKEY})
    return r.text


if __name__ == "__main__":
    app.run(debug=True)
