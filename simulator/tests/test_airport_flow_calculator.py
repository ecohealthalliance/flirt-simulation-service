# coding=utf8
import unittest
from testhelpers import TestHelpers
from ..AirportFlowCalculator import AirportFlowCalculator, compute_airport_distances, is_logical, \
    compute_direct_seat_flows
from .. import config
import pymongo
import math
import datetime
import numpy as np

class TestAirportFlowCalculator(unittest.TestCase, TestHelpers):
    SIMULATED_PASSENGERS = 2000

    @classmethod
    def setUpClass(self):
        self.db = pymongo.MongoClient(config.mongo_uri)[config.mongo_db_name]
        start = datetime.datetime(2017, 2, 1)
        end = datetime.datetime(2017, 2, 2)
        direct_seat_flows = compute_direct_seat_flows(self.db, {
            "departureDateTime": {
                "$lte": end,
                "$gte": start
            }
        })
        self.calculator = AirportFlowCalculator(self.db, aggregated_seats=direct_seat_flows)
        self.itineraries = list(self.calculator.calculate_itins(
            "BNA",
            simulated_passengers=self.SIMULATED_PASSENGERS,
            start_date=start,
            end_date=end))
        counts = {}
        leg_dist = {}
        for itin in self.itineraries:
            assert itin[0] != itin[-1]
            counts[itin[-1]] = counts.get(itin[-1], 0) + 1
            leg_dist[len(itin) - 1] = leg_dist.get(len(itin) - 1, 0) + 1
        self.probs = {k: float(v) / self.SIMULATED_PASSENGERS for k, v in counts.items()}
        self.leg_dist = {k: float(v) / self.SIMULATED_PASSENGERS for k, v in leg_dist.items()}

    def test_airport_flow_perservation(self):
        """
        The values at all the terminal probs should sum to near the number of
        initial passengers. The difference should be equal to the error recorded
        by the AirportFlowCalculator.
        """
        cumulative_terminal_flow = 0.0
        for airport_id, flow in self.probs.items():
            cumulative_terminal_flow += flow
        self.assertEqual(
            round(cumulative_terminal_flow, 2), 1.0)

    def test_origin_probability(self):
        """
        The origin should have a probability of zero
        """
        self.assertEqual(self.probs.get("BNA", 0), 0)

    # def test_airport_flows(self):
    #     # This test will fail if the flight data in the database changes.
    #     # print { str(k): round(v, 3) for k, v in self.probs.items() if v > 0.01 }
    #     expected_probs = {'MSY': 0.015, 'ATL': 0.022, 'BOS': 0.017, 'FLL': 0.058, 'DEN': 0.05, 'DTW': 0.033,
    #                       'JAX': 0.028, 'DAL': 0.092, 'ECP': 0.035, 'BWI': 0.023, 'PIT': 0.038, 'MDW': 0.027,
    #                       'SAT': 0.066, 'IAH': 0.026, 'TPA': 0.023, 'HOU': 0.025, 'EWR': 0.026, 'MCO': 0.03,
    #                       'MCI': 0.036, 'PHL': 0.047, 'LGA': 0.027, 'ORD': 0.02, 'LAS': 0.02}
    #     for airport_id, prob in expected_probs.items():
    #         # Binomial distribution standard deviaion as a percentage
    #         standard_deviation = math.sqrt(self.SIMULATED_PASSENGERS * prob * (1 - prob)) / self.SIMULATED_PASSENGERS
    #         err_msg = (airport_id + " has probability " + str(self.probs.get(airport_id, 0.0)) +
    #                    " which differs from the expected probability " + str(prob) +
    #                    " by more than 4 times its standard deviation " + str(standard_deviation))
    #         self.assertTrue(
    #             self.probs.get(airport_id, 0.0) <= prob + 4 * standard_deviation, err_msg)
    #         self.assertTrue(
    #             self.probs.get(airport_id, 0.0) >= prob - 4 * standard_deviation, err_msg)

    def test_leg_distribution(self):
        for legs, leg_prob in self.leg_dist.items():
            # due to things like airports with no outgoing flights, there
            # may be some significant deviation from the distribution.
            prob_diff = self.calculator.LEG_PROBABILITY_DISTRIBUTION[legs] - leg_prob
            self.assertTrue(abs(prob_diff) <= 0.03, "probability of " + str(legs) +
                            " legs differs by more than 3 from expected distribution. Difference: " + str(prob_diff))

    def test_distance_matrix(self):
        airport_to_coords = {}
        for airport in self.db.airports.find({
            '_id': {
                '$in': ['SEA', 'NRT', 'TPE', 'BNA', 'ECP', 'JFK', 'CDG', 'CPT', 'LAX', 'MEX']
            }
        }):
            airport_to_coords[airport['_id']] = airport['loc']['coordinates']
        airport_to_coords_items = sorted(airport_to_coords.items(), key=lambda x: x[0])
        dist_mat = compute_airport_distances(airport_to_coords_items)
        for i, (airport, noop) in enumerate(airport_to_coords_items):
            if airport == "NRT": nrt_idx = i
            if airport == "SEA": sea_idx = i
            if airport == "TPE": tpe_idx = i
            if airport == "BNA": bna_idx = i
            if airport == "ECP": ecp_idx = i
        self.assertTrue(is_logical(dist_mat, nrt_idx, sea_idx, tpe_idx))
        self.assertFalse(is_logical(dist_mat, nrt_idx, tpe_idx, sea_idx))
        self.assertFalse(is_logical(dist_mat, bna_idx, bna_idx, ecp_idx))
        # Check that the furthest airport form any airport is no less than half of the distance between
        # the furthest apart airports.
        max_dist_per_airport = np.max(dist_mat, axis=1)
        print np.min(max_dist_per_airport)
        self.assertTrue(np.max(max_dist_per_airport) <= 2 * np.min(max_dist_per_airport))
        # test the number of logical layovers increases on average with distance.
        logical_layover_histogram = []
        for a in np.argsort(dist_mat[0,:]):
            logical_layover_histogram.append(len([
                i for i in range(dist_mat.shape[0])
                if is_logical(dist_mat, 0, a, i)
            ]))
        print logical_layover_histogram
        self.assertTrue(sum(logical_layover_histogram[:5]) < sum(logical_layover_histogram[5:]))