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
    months = [
        (8, 2017),
        (9, 2017),
        (10, 2017),
        (11, 2017),
        (12, 2017),
        (1, 2018),
    ]
    for (start_month, start_year), (end_month, end_year) in zip(months, months[1:]):
        start_date = datetime.datetime(start_year, start_month, 1)
        end_date = datetime.datetime(end_year, end_month, 1)
        res = celery.group(*[
            tasks.calculate_flows_for_airport.s(
                i['_id'],
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d'),
                'fmd' + start_date.strftime('-%Y-%m')).set(queue='caching')
            for i in db.airports.find()
        ])()
        # Wait for all sims for the month to complete.
        # Simulating too many months at once can be slow because aggregated direct flights is only cached
        # for a limited number of months.
        res.get(timeout=None, interval=0.5)

main()
