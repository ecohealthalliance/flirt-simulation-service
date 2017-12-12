import os
import celery
import logging
import pymongo
import datetime
from simulator.AirportFlowCalculator import AirportFlowCalculator
from dateutil import parser as dateparser
from collections import defaultdict
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
    SIMULATED_PASSENGERS = 20000
    date_range_end = datetime.datetime.now()
    date_range_start = date_range_end - datetime.timedelta(14)

    def compute_direct_seat_flows():
        result = defaultdict(dict)
        count = 0
        for pair in db.flights.aggregate([
            {
                "$match": {
                    "departureDateTime": {
                        "$lte": date_range_end,
                        "$gte": date_range_start
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        '$concat': ['$departureAirport', '-', '$arrivalAirport']
                    },
                    'totalSeats': {
                        '$sum': '$totalSeats'
                    }
                }
            }
        ]):
            if pair['totalSeats'] > 0:
                origin, destination = pair['_id'].split('-')
                result[origin][destination] = pair['totalSeats']
                count += 1
        print "total sets:", count
        return result

    direct_seat_flows = compute_direct_seat_flows()

    calculator_with_schedules = AirportFlowCalculator(db, aggregated_seats=direct_seat_flows)
    calculator_aggregated_flows = AirportFlowCalculator(db,
                                                        use_schedules=False,
                                                        aggregated_seats=direct_seat_flows)

    for airport_code in ['CDG', 'TPE', 'SEA', 'JFK', 'CPT']:
        sim_df = pd.DataFrame(calculator_aggregated_flows.calculate(
            airport_code,
            simulated_passengers=int(SIMULATED_PASSENGERS)).values()).sort_values('terminal_flow')
        print sim_df
        sim_df.to_csv(airport_code + '_aggregated_flows.csv')

        sim_df = pd.DataFrame(calculator_with_schedules.calculate(
            airport_code,
            simulated_passengers=int(SIMULATED_PASSENGERS),
            start_date=date_range_start,
            end_date=date_range_end).values()).sort_values('terminal_flow')
        print sim_df
        sim_df.to_csv(airport_code + '_with_schedules.csv')

main()
