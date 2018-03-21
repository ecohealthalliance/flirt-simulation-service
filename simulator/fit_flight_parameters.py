import pandas as pd
import statsmodels.formula.api as sm
import glob

# Source:
# https://www.transtats.bts.gov/Fields.asp?Table_ID=293
# Use departures performed / scheduled to correct flows??
df = pd.read_csv(glob.glob("*_T100_SEGMENT_ALL_CARRIER.csv")[0])
df = df.query("SEATS > 0 and PASSENGERS > 0")
df['seats_per_flight'] = df.SEATS / df.DEPARTURES_PERFORMED
df['load_ratio'] = df.PASSENGERS / df.SEATS
df['departure_ratio'] = df.DEPARTURES_SCHEDULED / df.DEPARTURES_PERFORMED
result = sm.ols(formula="load_ratio ~ seats_per_flight", data=df).fit()
print "Load Ratio Parameters:"
print result.params
print "---"
print "Departure Ratio Parameters:"
result = sm.ols(formula="departure_ratio ~ seats_per_flight", data=df).fit()
print result.params
