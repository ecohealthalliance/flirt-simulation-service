import logging
import os
import pymongo
import datetime
from simulator import tasks
import celery

__VERSION__ = '0.0.1'

if 'MONGO_URI' in os.environ:
    mongo_url = os.environ['MONGO_URI']
else:
    mongo_url = 'localhost:27017'

if 'MONGO_DB' in os.environ:
    mongo_db_name = os.environ['MONGO_DB']
else:
    mongo_db_name = 'flirt'

db = pymongo.MongoClient(mongo_url)[mongo_db_name]

def main():
    res = celery.group(*[
        tasks.calculate_flows_for_airport.s(i['_id'])
        for i in db.airports.find()
    ])()
    print res.get(timeout=None, interval=0.5)

main()
