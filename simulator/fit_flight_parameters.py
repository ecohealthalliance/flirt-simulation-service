import pandas as pd
import statsmodels.formula.api as sm
import glob
from AirportFlowCalculator import compute_direct_passenger_flows, compute_direct_seat_flows, AirportFlowCalculator
import pymongo
import datetime

# Source:
# https://www.transtats.bts.gov/Fields.asp?Table_ID=293
# Use departures performed / scheduled to correct flows??
df = pd.read_csv(glob.glob('*_T100_SEGMENT_ALL_CARRIER.csv')[0])
df = df.query('SEATS > 0')
df['seats_per_flight'] = df.SEATS / df.DEPARTURES_PERFORMED
df['load_ratio'] = df.PASSENGERS / df.SEATS
# Create a record for each flight so the number of flights wieghts the OLS fit.
flight_load_ratios = pd.DataFrame({
    'seats_per_flight': row.seats_per_flight,
    'load_ratio': row.load_ratio
} for idx, row in df.iterrows() for flight in range(int(row.DEPARTURES_PERFORMED)))
result = sm.ols(formula="load_ratio ~ seats_per_flight", data=flight_load_ratios).fit()
print "Load Ratio Parameters:"
print result.params
print

print "Validating Results:"
seats_per_pasenger = sum(legs * value for legs, value in AirportFlowCalculator.LEG_PROBABILITY_DISTRIBUTION.items())
#total_direct_passengers = sum(direct_passenger_flows[origin_airport_id].values())
#total_passengers = int(float(total_direct_passengers) / seats_per_pasenger)

db = pymongo.MongoClient('localhost:27019')['flirt']
direct_seat_flows = compute_direct_passenger_flows(db, {
    'departureAirport': 'ORD',
    '$and': [{
        'departureDateTime': {
            '$lte': datetime.datetime(2017, 7, 1)
        }
    }, {
        'departureDateTime': {
            '$gte': datetime.datetime(2017, 6, 1)
        }
    }],
    'totalSeats': {
        '$gte': 0
    }
}, result.params.seats_per_flight, result.params.Intercept)
total_direct_passengers = sum(value for x in direct_seat_flows.values() for value in x.values())
print "Predicted direct flight passengers from flight data:", total_direct_passengers
df = df.query('MONTH == 6 and ORIGIN == "ORD"')
print "Predicted passengers from BTS seats:", ((df.seats_per_flight * result.params.seats_per_flight + result.params.Intercept) * df.SEATS).sum()
print "Actual passengers:", df.PASSENGERS.sum()
