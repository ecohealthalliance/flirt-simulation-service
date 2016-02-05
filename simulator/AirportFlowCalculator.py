#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This generates data for the Flight Network Heat Map described here:
https://docs.google.com/document/d/1ajv6hJ_lz9JNpzsNjoKLUhPt7pfv2bmh63J_EhycoYA/edit

The goal is to create a heatmap for where an infectious disease could spread to
given our knowledge of the air traffic network.
"""
import pymongo
from dateutil import parser as dateparser
import datetime
from geopy.distance import great_circle
import math
import random
from pylru import lrudecorator

class AirportFlowCalculator(object):
    # Assumption: We will assume that the probability distribution for the
    # number of legs in a jouney is homogenous across point of origin and
    # time of travel.
    # We assume the probability distribution has the following values:
    LEG_PROBABILITY_DISTRIBUTION = {
        0: 0.0,
        1: 0.6772732,
        2: 0.2997706,
        3: 0.0211374,
        4: 0.0016254,
        5: 0.0001632,
        6: 0.0000215,
        7: 0.0000072,
        8: 0.0000012,
        9: 0.0000002,
        10: 0.0000001
    }
    MEAN_LAYOVER_DELAY_HOURS = 2
    def __init__(self, db,
            weight_by_departure_time=True):
        self.db = db
        self.db.legs.ensure_index('departureAirport._id')
        self.db.legs.ensure_index('effectiveDate')
        self.db.legs.ensure_index('discontinuedDate')
        self.weight_by_departure_time = weight_by_departure_time
        # LEG_PROBABILITY_DISTRIBUTION shows the probability of ending a journey
        # at each leg given one is at the start of the journey.
        # TERMINAL_LEG_PROBABILITIES shows the probability of ending a journey
        # at each leg given one has already reached it.
        self.TERMINAL_LEG_PROBABILITIES = {
            leg_num: (
                leg_prob /
                (1.0 - sum([
                    self.LEG_PROBABILITY_DISTRIBUTION[n]
                    for n in range(1, leg_num)])))
            for leg_num, leg_prob in self.LEG_PROBABILITY_DISTRIBUTION.items()}
        self.max_legs = len(self.LEG_PROBABILITY_DISTRIBUTION) - 1
    @lrudecorator(1000)
    def get_flights_from_airport(self, airport, date):
        """
        Retrieve all the flight that that happened up to 2 days after the given
        date from the database then return them in an array with a distinct
        element for each flight.

        Notes:
        * This function is memoized to redues the number of database queries
          needed.
        """
        query = self.db.legs.find({
            "departureAirport._id": airport,
            "totalSeats": { "$gt" : 0 },
            "effectiveDate": { "$lte" : date },
            "discontinuedDate": { "$gte" : date }
        }, {
            "_id":1,
            "arrivalTimeUTC":1,
            "departureTimeUTC":1,
            "day1":1, "day2":1, "day3":1, "day4":1, "day5":1, "day6":1, "day7":1,
            "arrivalAirport.loc.coordinates":1,
            "arrivalAirport._id":1,
            "departureAirport.loc.coordinates":1,
            "departureAirport._id":1,
            "totalSeats":1,
            "effectiveDate":1, "discontinuedDate": 1})
        results = list(query)
        flights = []
        days_of_the_week = "Monday,Tuesday,Wednesday,Thursday,Friday,Saturday,Sunday".split(",")
        for result in results:
            # we don't need to worry about weekends being interpreted as being
            # in the past because setting the default date on the dateparser
            # ensures that weekdays are interpreted as being during or after
            # the given date.
            for day_of_week in range(date.weekday(), date.weekday() + 3):
                day_of_week = day_of_week % 7
                # The schema here indicates that day1 is monday:
                # http://webcache.googleusercontent.com/search?q=cache:AvkW3eU9PckJ:https://faaco.faa.gov/attachments/Attachment_J-1--Flight_Data_File.doc+&cd=1&hl=en&ct=clnk&gl=tw
                if result["day" + str(day_of_week + 1)]:
                    departure_datetime = dateparser.parse(
                        days_of_the_week[day_of_week] + " " +
                        result["departureTimeUTC"], default=date)
                    arrival_datetime = dateparser.parse(
                        days_of_the_week[day_of_week] + " " +
                        result["arrivalTimeUTC"], default=date)
                    if arrival_datetime < departure_datetime:
                        # next day arrival
                        arrival_datetime += datetime.timedelta(1)
                    # Check that the departure date is within the effective date
                    # range because with the day of week factored in, it is
                    # possible to end up outside of it.
                    if departure_datetime >= result["effectiveDate"] and departure_datetime <= result["discontinuedDate"]:
                        flights.append(dict(result,
                            departure_datetime=departure_datetime,
                            arrival_datetime=arrival_datetime))
        # print "Results:", len(results)
        # print "Flights:", len(flights)
        return flights
    def calculate(self,
            starting_airport,
            simulated_passengers=100,
            store_itins_with_id=None,
            start_date=datetime.datetime.now(),
            end_date=datetime.datetime.now(),
            include_stops_in_itin=False):
        """
        Calculate the probability of a given passenger reaching each destination
        from the departure airport by simulating several voyages.
        """
        self.error = 0.0
        starting_airport_dict = self.db.airports.find_one({'_id': starting_airport})
        if not starting_airport_dict:
            return {}
        @lrudecorator(1000)
        def get_filtered_flights_from_airport(airport, date):
            flights = self.get_flights_from_airport(airport, date)
            # Assumption: People are unlikely to fly a long distance,
            # and then take another flight which goes to a city near their
            # original point of origin, A1. Thus, weight destinations Bn by
            # how much closer they are to the previous destination than A1.
            # NB: Currently I am pruning destinations closer to the origin
            # than the departure airport since developing a meaningful weight
            # would be difficult. To given an example of why meaningful weighting
            # is difficult, a 2 leg flight with a stop in the middle is no more
            # or less likely than one with one log leg that stops near the
            # destination to go to a regional airport, as far as we know,
            # but they have very different distance ratios.
            def distance(A, B):
                # Geopy expects lat, long but the airports are strored in
                # long, lat order
                return great_circle((A[1], A[0]), (B[1], B[0]))
            flights = [
                flight for flight in flights if (
                    distance(starting_airport_dict['loc']['coordinates'],
                        flight['arrivalAirport']['loc']['coordinates']) >=
                    distance(flight['departureAirport']['loc']['coordinates'],
                        flight['arrivalAirport']['loc']['coordinates']))]
            # print "Filtered Flights:", len(flights)
            return flights
        def layover_pmf(hours):
            # Implementation of Poisson PMF based on:
            # http://stackoverflow.com/questions/280797/calculate-poisson-probability-percentage
            p = math.exp(-self.MEAN_LAYOVER_DELAY_HOURS)
            for i in range(int(hours)):
                p *= self.MEAN_LAYOVER_DELAY_HOURS
                p /= i+1
            return p
            # scikit poisson function
            # return poisson(self.MEAN_LAYOVER_DELAY_HOURS).pmf(value)
        def extend_airport_flows(airport_dict_a, airport_dict_b):
            """
            Extend airport_dict_a, a dictionary of airports keyed on airport ids,
            airport_dict_b and merge duplicate keys them by summing the terminal_flow
            values for the airports in both dicts.
            """
            for key, b_airport in airport_dict_b.items():
                if key in airport_dict_a:
                    airport_dict_a[key]['terminal_flow'] += b_airport['terminal_flow']
                else:
                    airport_dict_a[key] = dict(b_airport)
            return airport_dict_a
        def simulate_passenger(
            departure_airport,
            departure_airport_arrival_time,
            legs_sofar=0):
            """
            This function simulates a passenger then returns
            their the airports they stop at. It is a recusive function that calls 
            itself to simulate transfers on multi-leg flights.
            """
            flights = get_filtered_flights_from_airport(departure_airport,
                datetime.datetime(
                    departure_airport_arrival_time.year,
                    departure_airport_arrival_time.month,
                    departure_airport_arrival_time.day))
            # only include flights that the passenger arrived prior to
            flights = [
                flight for flight in flights
                if departure_airport_arrival_time < flight['departure_datetime']]
            # Weight flights from the origin city (A1) based on the summed
            # weekly flow between A and all other destinations (B1).
            # Specifically, sum the number of seats on each flight between
            # airport A and each B airport over the course of a week,
            # and divide this by the total number of seats leaving A over that week.
            # A small value is added to avoid division by zero errors when
            # there are no outbound flights.
            # However, these situations might cause some error since
            # there is nowhere for the passengers we expect to transfer
            # to go.
            cumulative_outbound_seats = sum([
                flight['totalSeats'] for flight in flights])
            # Assumption: People are likely to take flights that occur shortly
            # after they arrived at an airport. This may differ for, say,
            # flights crossing an administrative boundary, but at first pass,
            # we will assume that it is the same for all flights.
            # If person x, is arriving in FOO from destination unknown,
            # and is going to catch a connecting flight,
            # it is more likely that they are there to catch the connecting
            # flight to BAR which leaves an hour after their arrival than
            # the connecting flight to BAZ which leaves twelve hours after their arrival.
            # So, the airport inflows on multileg journeys are weighted by
            # where the layover time falls on the poisson distribution.
            if self.weight_by_departure_time:
                layover_probs = [
                    layover_pmf(
                        float((flight['departure_datetime'] -
                        departure_airport_arrival_time).total_seconds()) / 3600)
                    for flight in flights]
                time_weighted_cumulative_outbound_seats = sum([
                    flight['totalSeats'] * prob
                    for flight, prob in zip(flights, layover_probs)])
                # Filter out flights with a zero probability
                filtered_flights = []
                filtered_probs = []
                for flight, prob in zip(flights, layover_probs):
                    if prob > 0:
                        filtered_flights.append(flight)
                        filtered_probs.append(prob)
                flights = filtered_flights
                layover_probs = filtered_probs
            if len(flights) == 0:
                # There are no flights, so we assume the passenger leaves
                # the airport, unless it is the starting airport,
                # in which case the passenger is lost.
                if starting_airport != departure_airport:
                    return [self.db.airports.find_one({'_id': departure_airport})]
                else:
                    return [None]
            inflow_sofar = 0.0
            for idx, flight in enumerate(flights):
                # An airport's inflow is the number of passengers from the 
                # starting airport that are likely to end their trip at it.
                # This value is just for the current flight. There could be more
                # inflow from other flights which will be combined later.
                if self.weight_by_departure_time:
                    inflow = (
                        float(flight['totalSeats']) * layover_probs[idx] /
                        time_weighted_cumulative_outbound_seats)
                else:
                    inflow = (
                        float(flight['totalSeats']) /
                        cumulative_outbound_seats)
                terminal_flow = inflow * self.TERMINAL_LEG_PROBABILITIES[legs_sofar + 1] / (1.0 - inflow_sofar)
                outflow = inflow * (1.0 - self.TERMINAL_LEG_PROBABILITIES[legs_sofar + 1]) / (1.0 - inflow_sofar)
                random_number = random.random()
                if legs_sofar < self.max_legs and random_number <= outflow:
                    # Find airports that could be arrived at through transfers.
                    return [self.db.airports.find_one({'_id': departure_airport})] + simulate_passenger(
                        flight['arrivalAirport']['_id'],
                        departure_airport_arrival_time=flight['arrival_datetime'],
                        legs_sofar=legs_sofar + 1)
                elif random_number > (1.0 - terminal_flow):
                    return [self.db.airports.find_one({'_id': departure_airport}), flight['arrivalAirport']]
                else:
                    inflow_sofar += inflow
            # If we reach this point the passenger is lost.
            # This occurs mainly due to the max_legs cutoff.
            # It may also occur due to floating point error.
            return [None]
        airports = {}
        lost_passengers = 0
        for i in range(simulated_passengers):
            random_start_time = start_date + datetime.timedelta(
                seconds=random.randint(0, round((end_date - start_date).total_seconds())))
            itinerary = simulate_passenger(
                starting_airport,
                # A random datetime within the given range is chosen.
                departure_airport_arrival_time=random_start_time)
            if store_itins_with_id:
                itin = {
                    "origin": itinerary[0]['_id'],
                    "destination": itinerary[-1]['_id'],
                    "simulationId": store_itins_with_id
                }
                if include_stops_in_itin:
                    itin["stops"] = [airport["_id"] for airport in itinerary]
                self.db.simulated_itineraries.insert(itin)
            terminal_airport = itinerary[-1]
            if not terminal_airport:
                lost_passengers += 1
                continue
            terminal_airport = dict(terminal_airport,
                terminal_flow=1.0/simulated_passengers)
            extend_airport_flows(airports, { terminal_airport['_id'] : terminal_airport })
        self.error = float(lost_passengers) / simulated_passengers
        return airports
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mongo_url", default='localhost'
    )
    parser.add_argument(
        "--db_name", default='grits'
    )
    parser.add_argument(
        "--starting_airport", default='BNA',
        help="The airport code of the initial airport."
    )
    parser.add_argument(
        "--store_itins_with_id", default=None,
        help="""If set, a simulated_itineraries collection will be created in
        the given database from the simulated passengers,
        and the documents added by the simulation will all
        have the given id as their simulationId."""
    )
    parser.add_argument(
        "--start_date", default=None,
        help="""The simulated passengers will arrive at the initial airport
        at a random time between the start and end date.
        """
    )
    parser.add_argument(
        "--end_date", default=None,
        help="""The simulated passengers will arrive at the initial airport
        at a random time between the start and end date.
        """
    )
    parser.add_argument(
        "--simulated_passengers", default=1000,
        help="""The number of passengers to simulate.
        """
    )
    
    args = parser.parse_args()
    print ("Calculating probabilities of a single passenger reaching each airport" +
        " from " + args.starting_airport)
    print "This will probably take a few minutes..."
    start_date = datetime.datetime.now()
    if args.start_date:
        start_date = dateparser.parse(args.start_date)
    end_date = start_date
    if args.end_date:
        end_date = dateparser.parse(args.end_date)
    cumulative_probability = 0.0
    calculator = AirportFlowCalculator(
        pymongo.MongoClient(args.mongo_url)[args.db_name]
    )
    for airport_id, airport in calculator.calculate(
            args.starting_airport,
            simulated_passengers=int(args.simulated_passengers),
            store_itins_with_id=args.store_itins_with_id,
            start_date=start_date,
            end_date=end_date
            ).items():
        print airport_id, airport['terminal_flow']
        cumulative_probability += airport['terminal_flow']
    # This is a sanity check. cumulative_probability should sum to almost 1.
    print "Cumulative Probability:", cumulative_probability
