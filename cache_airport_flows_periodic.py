import os
import pymongo
import datetime
from simulator import tasks
import celery
import pandas as pd

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
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sim_group", default='fmd-%Y-%m'
    )
    parser.add_argument(
        "--start_date", default='2017-08-01'
    )
    parser.add_argument(
        "--periods", default='5'
    )
    parser.add_argument(
        "--freq", default='M'
    )
    args = parser.parse_args()
    periods = list(pd.date_range(args.start_date, periods=int(args.periods) + 1, freq=args.freq))
    for current_period, next_period in zip(periods, periods[1:]):
        start_date = current_period.to_period().start_time
        end_date = next_period.to_period().start_time
        res = celery.group(*[
            tasks.calculate_flows_for_airport.s(
                i['_id'],
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d'),
                start_date.strftime(args.sim_group)).set(queue='caching')
            for i in db.airports.find()
        ])()
        print "Waiting for sims to complete for:", str(start_date)
        # Simulating too many months at once can be slow because aggregated direct flights is only cached
        # for a limited number of months.
        res.get(timeout=None, interval=2.0)


if __name__ == '__main__':
    main()
