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
from collections import defaultdict


# Memoization speeds up the simulation but its use is limited by memory consumption.
# Using slotted objects reduces the size of the flights stored in memory
# allowing more of them to be cached.
class LightweightFlight(object):
    __slots__ = [
        'total_seats',
        'departure_datetime',
        'arrival_datetime',
        'arrival_airport']

    def __init__(self, flight_dict):
        self.total_seats = flight_dict['totalSeats']
        self.departure_datetime = flight_dict['departureDateTime']
        self.arrival_datetime = flight_dict['arrivalDateTime']
        self.arrival_airport = flight_dict['arrivalAirport']


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

    def __init__(self, db, weight_by_departure_time=True, outgoing_seat_totals=None, aggregated_seats=None):
        self.db = db
        self.db.flights.ensure_index('departureAirport')
        self.db.flights.ensure_index([('departureAirport', pymongo.ASCENDING), ('departureDateTime', pymongo.ASCENDING)])
        airports = {}
        for airport in self.db.airports.find():
            airports[airport['_id']] = airport['loc']['coordinates']
        # TODO: Pre-compute geographically reasonable itineraries
        self.airports = airports
        self.weight_by_departure_time = weight_by_departure_time
        self.outgoing_seat_totals = outgoing_seat_totals
        self.aggregated_seats = aggregated_seats
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

    @lrudecorator(20000)
    def get_flights_from_airport(self, airport, date):
        """
        Retrieve all the flight that that happened up to 2 days after the given
        date from the database then return them in an array with a distinct
        element for each flight.

        Notes:
        * This function is memoized to redues the number of database queries
          needed.
        """
        query_results = self.db.flights.find({
            "departureAirport": airport,
            "totalSeats": {"$gt": 0},
            "departureDateTime": {
                "$gte": date,
                "$lte": date + datetime.timedelta(1)
            }
        }, {
            "_id": 1,
            "departureDateTime": 1,
            "arrivalDateTime": 1,
            "arrivalAirport": 1,
            "totalSeats": 1,
        })
        flights = []
        for result in query_results:
            flights.append(LightweightFlight(result))
        print "Flights:", len(flights)
        return flights

    def calculate_itins(self,
                        starting_airport,
                        simulated_passengers=100,
                        start_date=datetime.datetime.now(),
                        end_date=datetime.datetime.now()):
        """
        Calculate the probability of a given passenger reaching each destination
        from the departure airport by simulating several voyages.
        """

        def layover_pmf(hours):
            # Implementation of Poisson PMF based on:
            # http://stackoverflow.com/questions/280797/calculate-poisson-probability-percentage
            p = math.exp(-self.MEAN_LAYOVER_DELAY_HOURS)
            for i in range(int(hours)):
                p *= self.MEAN_LAYOVER_DELAY_HOURS
                p /= i + 1
            return p

        def simulate_passenger(itin_sofar, departure_airport_arrival_time):
            """
            This function simulates a passenger then returns
            their the airports they stop at. It is a recusive function that calls 
            itself to simulate transfers on multi-leg flights.
            """
            departure_airport = itin_sofar[-1]
            flights = self.get_flights_from_airport(departure_airport,
                                                    datetime.datetime(
                                                        departure_airport_arrival_time.year,
                                                        departure_airport_arrival_time.month,
                                                        departure_airport_arrival_time.day))
            # only include flights that the passenger arrived prior to
            flights = [
                flight for flight in flights
                if departure_airport_arrival_time < flight.departure_datetime]
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
                flight.total_seats for flight in flights])
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
                        float((flight.departure_datetime -
                               departure_airport_arrival_time).total_seconds()) / 3600)
                    for flight in flights]
                time_weighted_cumulative_outbound_seats = sum([
                    flight.total_seats * prob
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
            # departure_airport_obj = self.db.airports.find_one({'_id': departure_airport})
            if len(flights) == 0:
                # There are no flights, so we assume the passenger leaves
                # the airport.
                return [departure_airport]
            inflow_sofar = 0.0
            for idx, flight in enumerate(flights):
                # An airport's inflow is the number of passengers from the 
                # starting airport that are likely to end their trip at it.
                # This value is just for the current flight. There could be more
                # inflow from other flights which will be combined later.
                if self.weight_by_departure_time:
                    inflow = (
                        float(flight.total_seats) * layover_probs[idx] /
                        time_weighted_cumulative_outbound_seats)
                else:
                    inflow = (
                        float(flight.total_seats) /
                        cumulative_outbound_seats)
                terminal_flow = inflow * self.TERMINAL_LEG_PROBABILITIES[len(itin_sofar)] / (1.0 - inflow_sofar)
                outflow = inflow * (1.0 - self.TERMINAL_LEG_PROBABILITIES[len(itin_sofar)]) / (1.0 - inflow_sofar)
                random_number = random.random()
                if len(itin_sofar) - 1 < self.max_legs and random_number <= outflow:
                    # Find airports that could be arrived at through transfers.
                    return simulate_passenger(
                        itin_sofar + [flight.arrival_airport],
                        departure_airport_arrival_time=flight.arrival_datetime)
                elif random_number > (1.0 - terminal_flow):
                    return itin_sofar + [flight.arrival_airport]
                else:
                    inflow_sofar += inflow
            # The function might not return in the for loop above due to the
            # max_legs cutoff or floating point error.
            # In this case we assume the passenger stops at the departure airport.
            return itin_sofar

        def simulate_passenger_on_aggregate_flows(itin_sofar):
            """
            This function simulates a passenger using the aggregate number of direct flight seats.
            """
            seat_portion_so_far = 0.0
            departure_airport = itin_sofar[-1]
            for idx, (destination, seats) in enumerate(self.aggregated_seats[departure_airport].items()):
                seat_portion_for_dest = float(seats) / self.outgoing_seat_totals[departure_airport]
                terminal_dest_portion = seat_portion_for_dest * self.TERMINAL_LEG_PROBABILITIES[len(itin_sofar)] / (
                1.0 - seat_portion_so_far)
                ongoing_portion = seat_portion_for_dest * (
                1.0 - self.TERMINAL_LEG_PROBABILITIES[len(itin_sofar) - 1]) / (1.0 - seat_portion_so_far)
                random_number = random.random()
                if len(itin_sofar) - 1 < self.max_legs and random_number <= ongoing_portion:
                    # Find airports that could be arrived at through transfers.
                    return simulate_passenger_on_aggregate_flows(itin_sofar + [destination])
                elif random_number > (1.0 - terminal_dest_portion):
                    return itin_sofar + [destination]
                else:
                    seat_portion_so_far += seat_portion_for_dest
            # The function might not return in the for loop above due to the
            # max_legs cutoff or floating point error.
            # In this case we assume the passenger stops at the departure airport.
            return itin_sofar

        if self.aggregated_seats:
            for i in range(simulated_passengers):
                itinerary = simulate_passenger_on_aggregate_flows([starting_airport])
                yield itinerary
        else:
            for i in range(simulated_passengers):
                random_start_time = start_date + datetime.timedelta(
                    seconds=random.randint(0, round((
                                                        datetime.timedelta(days=1) + end_date - start_date
                                                    ).total_seconds())))
                itinerary = simulate_passenger(
                    [starting_airport],
                    # A random datetime within the given range is chosen.
                    departure_airport_arrival_time=random_start_time)
                yield itinerary

    def calculate(self,
                  starting_airport,
                  simulated_passengers=100,
                  start_date=datetime.datetime.now(),
                  end_date=datetime.datetime.now()):
        terminal_passengers_by_airport = defaultdict(int)
        for itinerary in self.calculate_itins(starting_airport, simulated_passengers, start_date, end_date):
            print itinerary
            if len(itinerary) > 1:
                terminal_airport = itinerary[-1]
                terminal_passengers_by_airport[terminal_airport] += 1
        return {
            airport: dict(
                _id=airport,
                terminal_flow=float(passengers) / simulated_passengers)
            for airport, passengers in terminal_passengers_by_airport.items()
        }


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mongo_url", default='localhost'
    )
    parser.add_argument(
        "--db_name", default='grits-net-meteor'
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
