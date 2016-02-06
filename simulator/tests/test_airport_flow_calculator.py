# coding=utf8
import unittest
from testhelpers import TestHelpers
from AirportFlowCalculator import AirportFlowCalculator
import pymongo
import math
import datetime
class TestAirportFlowCalculator(unittest.TestCase, TestHelpers):
    SIMULATED_PASSENGERS = 2000
    @classmethod
    def setUpClass(self):
        self.db = pymongo.MongoClient("localhost")["grits"]
        self.db.simulated_itineraries.delete_many({"simulationId": "test"})
        self.calculator = AirportFlowCalculator(self.db)
        self.probs = {
            k: v.get('terminal_flow', 0.0)
            for k, v in self.calculator.calculate(
                "BNA", simulated_passengers=self.SIMULATED_PASSENGERS,
                store_itins_with_id="test", include_stops_in_itin=True,
                start_date=datetime.datetime(2016, 2, 1),
                end_date=datetime.datetime(2016, 2, 1)).items()}
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
            round(cumulative_terminal_flow + self.calculator.error, 2), 1.0)
    def test_origin_probability(self):
        """
        The origin should have a probability of zero
        """
        self.assertEqual(self.probs.get("BNA", 0), 0)
    def test_airport_flows(self):
        # This test will fail if the flight data in the database changes.
        print { str(k): round(v, 3) for k, v in self.probs.items() if v > 0.01 }
        expected_probs = {'ATL': 0.05, 'FLL': 0.06, 'DEN': 0.06, 'DTW': 0.02, 'TPA': 0.03, 'DAL': 0.06, 'ECP': 0.05, 'BWI': 0.05, 'PIT': 0.05, 'MDW': 0.03, 'SAT': 0.06, 'IAH': 0.02, 'JAX': 0.04, 'IAD': 0.02, 'HOU': 0.03, 'EWR': 0.01, 'MCI': 0.05, 'PHL': 0.06, 'LGA': 0.02, 'YYZ': 0.01, 'ORD': 0.01, 'LAS': 0.03}
        for airport_id, prob in expected_probs.items():
            # Binomial distribution standard deviaion as a percentage
            standard_deviation = math.sqrt(self.SIMULATED_PASSENGERS * prob * (1 - prob)) / self.SIMULATED_PASSENGERS
            err_msg = (airport_id + " has probability " + str(self.probs.get(airport_id, 0.0)) +
                " which differs from the expected probability " + str(prob) +
                " by more than 4 times its standard deviation " + str(standard_deviation))
            self.assertTrue(
                self.probs.get(airport_id, 0.0) <=  prob + 4 * standard_deviation, err_msg)
            self.assertTrue(
                self.probs.get(airport_id, 0.0) >=  prob - 4 * standard_deviation, err_msg)
    def test_leg_distribution(self):
        results = self.db.simulated_itineraries.aggregate([
          { "$match" : { "simulationId": "test" } },
          {
            "$group" : {
                "_id": { "$size": "$stops" },
                "total" : { "$sum" : 1 }
            }
          }
        ])
        for result in results:
            legs = result['_id'] - 1
            leg_prob = float(result['total']) / self.SIMULATED_PASSENGERS
            print leg_prob
            prob_diff = self.calculator.LEG_PROBABILITY_DISTRIBUTION[legs] - leg_prob
            self.assertTrue(abs(prob_diff) <= 0.02, "probability of " + str(legs) +
                " legs differs by more than 2% from expected distribution. Difference: " + str(prob_diff))
