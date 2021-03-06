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
import numpy

# Paramters derived from fit_flight_parameters.py
A_load_ratio = 0.000861
b_load_ratio = 0.674728

def compute_direct_seat_flows(db, match_query):
    result = defaultdict(dict)
    for pair in db.flights.aggregate([
        {
            '$match': match_query
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
    return result


def compute_direct_passenger_flows(
    db, match_query,
    A_load_ratio_p=A_load_ratio, b_load_ratio_p=b_load_ratio):
    result = defaultdict(dict)
    for pair in db.flights.aggregate([
        {
            '$match': match_query
        }, {
            '$group': {
                '_id': {
                    '$concat': ['$departureAirport', '-', '$arrivalAirport']
                },
                'totalPassengers': {
                    '$sum': {
                        '$multiply': [
                            {
                                '$sum': [
                                    {
                                        '$multiply' : [
                                            A_load_ratio_p,
                                            '$totalSeats'
                                        ]
                                    }, b_load_ratio_p
                                ]
                            }, '$totalSeats'
                        ]
                    }
                }
            }
        }
    ]):
        if pair['totalPassengers'] > 0:
            origin, destination = pair['_id'].split('-')
            result[origin][destination] = pair['totalPassengers']
    return result

def compute_airport_distances(airport_to_coords_items):
    """
    :param airport_to_coords_items: A array of airports and their coordinates alphabetically sorted by code.
    :return: A distance matrix where the row/column index corresponds to the index of the airport in the array.
    """
    dist_mat = numpy.zeros(shape=(len(airport_to_coords_items), len(airport_to_coords_items)))
    for idx, (airport_a, (airport_a_long, airport_a_lat)) in enumerate(airport_to_coords_items):
        j = idx
        for airport_b, (airport_b_long, airport_b_lat) in airport_to_coords_items[idx:]:
            dist_mat[idx, j] = great_circle(
                (airport_a_lat, airport_a_long),
                (airport_b_lat, airport_b_long)).kilometers
            j += 1
    # Make distance matrix symmetrical.
    dist_mat += dist_mat.T
    return dist_mat

def is_logical(airport_distance_matrix, airport_a, airport_b, intermediate_airport):
    # In logical layovers the intermediate airport is closer to the destination or
    # it is closer to the origin than the destination is to the origin.
    ab_distance = airport_distance_matrix.item(airport_a, airport_b)
    ia_distance = airport_distance_matrix.item(airport_a, intermediate_airport)
    ib_distance = airport_distance_matrix.item(airport_b, intermediate_airport)
    return ib_distance < ab_distance or ia_distance < ab_distance


# Memoization speeds up the simulation but its use is limited by memory consumption.
# Using slotted objects reduces the size of the flights stored in memory
# allowing more of them to be cached.
class LightweightFlight(object):
    __slots__ = [
        'passengers',
        'total_seats',
        'departure_datetime',
        'arrival_datetime',
        'arrival_airport']

    def __init__(self, flight_dict):
        load_ratio = A_load_ratio * flight_dict['totalSeats'] + b_load_ratio
        self.passengers = load_ratio * flight_dict['totalSeats']
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

    def __init__(self, db, weight_by_departure_time=True, aggregated_seats=None, use_schedules=True, use_layover_checking=True):
        self.use_schedules = use_schedules
        self.db = db
        self.db.flights.ensure_index('departureAirport')
        self.db.flights.ensure_index(
            [('departureAirport', pymongo.ASCENDING), ('departureDateTime', pymongo.ASCENDING)])
        self.use_layover_checking = use_layover_checking
        if self.use_layover_checking:
            if aggregated_seats:
                active_airports = set()
                for origin, destinations in aggregated_seats.items():
                    active_airports.add(origin)
                    active_airports.update(destinations)
                airport_to_coords = {}
                for airport in self.db.airports.find():
                    if airport['_id'] in active_airports:
                        airport_to_coords[airport['_id']] = airport['loc']['coordinates']
            else:
                airport_to_coords = {airport['_id']: airport['loc']['coordinates']
                                     for airport in self.db.airports.find()}
            airport_to_coords_items = sorted(airport_to_coords.items(), key=lambda x: x[0])
            self.airport_to_coords_items = airport_to_coords_items
            self.airport_to_idx = {airport: idx for idx, (airport, noop) in enumerate(airport_to_coords_items)}
            self.airport_distance_matrix = compute_airport_distances(airport_to_coords_items)
        self.weight_by_departure_time = weight_by_departure_time
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

    def get_itinerary_distance(self, itinerary):
        idx_itinerary = [self.airport_to_idx.get(airport) for airport in itinerary]
        idx_itinerary = filter(lambda x: x, idx_itinerary)
        total_distance = 0.0
        for a, b in zip(idx_itinerary, idx_itinerary[1:]):
            total_distance += self.airport_distance_matrix.item(a, b)
        return total_distance

    def check_logical_layovers(self, itinerary):
        """
        Check that the last 3 airports in the itinerary form a logical layover and that every airport in the itinerary
        is a logical layover between first and final airports.
        The criteria used to determine if a layover is logical is essentially
        to draw a circle around the start and end airports with a radius equal to the distance between them
        and only allow layovers airports within at least one of the two circles.
        Another way to put it is that if a layover flight leg both takes longer than a direct flight to the destination
        would and puts the passenger at a location further from the destination than they were initially it is illogical.
        """
        idx_itinerary = [self.airport_to_idx.get(airport) for airport in itinerary]
        origin = idx_itinerary[0]
        destination = idx_itinerary[-1]
        if destination is None:
            # When the airport location is unknown the layover cannot be checked.
            return True
        if origin == destination:
            return False
        layovers = filter(lambda x: x, idx_itinerary[1:-1])
        # Check last 3 airports in long itineraries.
        if len(layovers) > 2 and not is_logical(self.airport_distance_matrix, layovers[-2], destination, layovers[-1]):
            return False
        if origin is None:
            return True
        result = all([
            is_logical(self.airport_distance_matrix, origin, destination, intermediate)
            for intermediate in layovers])
        return result

    @lrudecorator(30000)
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
        # print "Flights:", len(flights)
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

            if len(itin_sofar) - 1 >= self.max_legs:
                return itin_sofar
            if self.use_layover_checking:
                # only include flights with logical layovers
                flights = [
                    flight for flight in flights
                    if self.check_logical_layovers(itin_sofar + [flight.arrival_airport])]
            # only include flights that the passenger arrived prior to
            flights = [
                flight for flight in flights
                if departure_airport_arrival_time < flight.departure_datetime]
            # Weight flights from the origin city (A1) based on the summed
            # direct flow between A and all other destinations (B1).
            # However, these situations might cause some error since
            # there is nowhere for the passengers we expect to transfer
            # to go.
            cumulative_outbound_passengers = sum([
                flight.passengers for flight in flights])
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
                time_weighted_cumulative_outbound_passengers = sum([
                    flight.passengers * prob
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
                # the airport.
                return itin_sofar

            inflow_sofar = 0.0
            for idx, flight in enumerate(flights):
                # An airport's inflow is the number of passengers from the 
                # starting airport that are likely to end their trip at it.
                # This value is just for the current flight. There could be more
                # inflow from other flights which will be combined later.
                if self.weight_by_departure_time:
                    inflow = (
                        float(flight.passengers) * layover_probs[idx] /
                        time_weighted_cumulative_outbound_passengers)
                else:
                    inflow = (
                        float(flight.passengers) /
                        cumulative_outbound_passengers)
                terminal_flow = inflow * self.TERMINAL_LEG_PROBABILITIES[len(itin_sofar)] / (1.0 - inflow_sofar)
                outflow = inflow * (1.0 - self.TERMINAL_LEG_PROBABILITIES[len(itin_sofar)]) / (1.0 - inflow_sofar)
                random_number = random.random()
                if random_number <= outflow:
                    # Find airports that could be arrived at through transfers.
                    return simulate_passenger(
                        itin_sofar + [flight.arrival_airport],
                        departure_airport_arrival_time=flight.arrival_datetime)
                elif random_number > (1.0 - terminal_flow):
                    return itin_sofar + [flight.arrival_airport]
                else:
                    inflow_sofar += inflow
            # The function might not return in the for loop above due to floating point error.
            # In this case we assume the passenger stops at the last arrival airport iterated over.
            return itin_sofar + [flight.arrival_airport]

        def simulate_passenger_on_aggregate_flows(itin_sofar):
            """
            This function simulates a passenger using the aggregate number of direct flight seats.
            """
            seat_portion_so_far = 0.0
            departure_airport = itin_sofar[-1]
            if self.use_layover_checking:
                initial_origin = itin_sofar[0]
                layover_set = set(itin_sofar[1:])
                valid_destinations = {}
                for destination, seats in self.aggregated_seats[departure_airport].items():
                    # filter out itineraries that have illogical layovers.
                    if self.check_logical_layovers(itin_sofar + [destination]):
                        if initial_origin == destination:
                            raise Exception("Circular itinerary")
                        valid_destinations[destination] = seats
            else:
                valid_destinations = self.aggregated_seats[departure_airport]
            outgoing_seat_total = sum(valid_destinations.values())
            for destination, seats in valid_destinations.items():
                seat_portion_for_dest = float(seats) / outgoing_seat_total
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
            # The function might not return in the for loop above due to floating point error.
            # In this case we assume the passenger stops at the last destination iterated over.
            return itin_sofar + [destination]

        if self.aggregated_seats:
            if len(self.aggregated_seats[starting_airport]) == 0:
                # No outgoing flights for airport
                return
        no_flight_sims = 0
        successful_sims = 0
        while successful_sims < simulated_passengers:
            if not self.use_schedules:
                itinerary = simulate_passenger_on_aggregate_flows([starting_airport])
            else:
                random_start_time = start_date + datetime.timedelta(
                    seconds=random.randint(0, round((
                                                        datetime.timedelta(days=1) + end_date - start_date
                                                    ).total_seconds())))
                itinerary = simulate_passenger(
                    [starting_airport],
                    # A random datetime within the given range is chosen.
                    departure_airport_arrival_time=random_start_time)
            if len(itinerary) > 1:
                no_flight_sims = 0
                successful_sims += 1
                yield itinerary
            elif no_flight_sims < simulated_passengers:
                no_flight_sims += 1
            else:
                # itineraries with a single airport will keep occuring if there are no outgoing flights,
                # so the simulation can be stopped.
                return

    def calculate(self,
                  starting_airport,
                  simulated_passengers=100,
                  start_date=datetime.datetime.now(),
                  end_date=datetime.datetime.now()):
        terminal_passengers_by_airport = defaultdict(int)
        trip_distances_by_airport = defaultdict(float)
        trip_legs_by_airport = defaultdict(int)
        for itinerary in self.calculate_itins(starting_airport, simulated_passengers, start_date, end_date):
            terminal_airport = itinerary[-1]
            terminal_passengers_by_airport[terminal_airport] += 1
            trip_distances_by_airport[terminal_airport] += self.get_itinerary_distance(itinerary)
            trip_legs_by_airport[terminal_airport] += len(itinerary) - 1
        return {
            airport: dict(
                _id=airport,
                terminal_flow=float(passengers_for_airport) / simulated_passengers,
                average_legs=float(trip_legs_by_airport[airport]) / passengers_for_airport,
                average_distance=float(trip_distances_by_airport[airport]) / passengers_for_airport)
            for airport, passengers_for_airport in terminal_passengers_by_airport.items()
        }


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mongo_url", default='localhost'
    )
    parser.add_argument(
        "--db_name", default='flirt'
    )
    parser.add_argument(
        "--starting_airport", default='BNA',
        help="The airport code of the initial airport."
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
    end_date = start_date + datetime.timedelta(days=14)
    if args.end_date:
        end_date = dateparser.parse(args.end_date)
    print start_date, end_date
    aggregated_seats = compute_direct_passenger_flows(
        pymongo.MongoClient(args.mongo_url)[args.db_name], {
            "departureDateTime": {
                "$lte": end_date,
                "$gte": start_date
            }
        })
    cumulative_probability = 0.0
    calculator = AirportFlowCalculator(
        pymongo.MongoClient(args.mongo_url)[args.db_name],
        aggregated_seats=aggregated_seats
    )
    for airport_id, airport in calculator.calculate(
            args.starting_airport,
            simulated_passengers=int(args.simulated_passengers),
            start_date=start_date,
            end_date=end_date
    ).items():
        print airport_id, airport['terminal_flow']
        cumulative_probability += airport['terminal_flow']
    # This is a sanity check. cumulative_probability should sum to almost 1.
    print "Cumulative Probability:", cumulative_probability
