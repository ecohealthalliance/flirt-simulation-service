import celery
import logging
import pymongo
import datetime
from AirportFlowCalculator import AirportFlowCalculator
from dateutil import parser as dateparser
import config
import smtplib
from email.mime.text import MIMEText
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BROKER_URL = config.mongo_uri + '/taskstest'

celery_tasks = celery.Celery('tasks', broker=BROKER_URL)
celery_tasks.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='json',
    CELERY_RESULT_BACKEND = BROKER_URL,
    CELERY_MONGODB_BACKEND_SETTINGS = {
        'database': 'tasks',
        'taskmeta_collection': 'taskmeta',
    }
)

SIMULATED_PASSENGERS = 1000

date_range_end = datetime.datetime.now()
date_range_start = date_range_end - datetime.timedelta(14)


def compute_direct_seat_flows():
    db = pymongo.MongoClient(config.mongo_uri)[config.mongo_db_name]
    result = defaultdict(dict)
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
    return result


direct_seat_flows = compute_direct_seat_flows()
total_outbound_seats = {
    airport_id: sum(arrival_seats.values())
    for airport_id, arrival_seats in direct_seat_flows.items() }

# There are initialized in tasks to prevent the db connection from being created pre-fork.
db = None
my_airport_flow_calculator = None

@celery_tasks.task(name='tasks.calculate_flows_for_airport')
def calculate_flows_for_airport(origin_airport_id):
    global my_airport_flow_calculator
    global db
    if my_airport_flow_calculator is None:
        db = pymongo.MongoClient(config.mongo_uri)[config.mongo_db_name]
        my_airport_flow_calculator = AirportFlowCalculator(db)
    results = my_airport_flow_calculator.calculate(
        origin_airport_id, simulated_passengers=SIMULATED_PASSENGERS)
    # Drop all results for origin airport
    db.passengerFlows.remove({
        'departureAirport': origin_airport_id
    })

    seats_per_pasenger = sum(legs * value for legs, value in AirportFlowCalculator.LEG_PROBABILITY_DISTRIBUTION.items())
    total_seats = sum(direct_seat_flows[origin_airport_id].values())
    total_passengers = int(float(total_seats) / seats_per_pasenger)

    print total_passengers
    if len(results) > 0:
        db.passengerFlows.insert_many({
            'departureAirport': origin_airport_id,
            'arrivalAirport': k,
            'estimatedPassengers': v['terminal_flow'] * total_passengers,
            'recordDate': datetime.datetime.now(),
            'startDateTime': date_range_start,
            'endDateTime': date_range_end
        } for k, v in results.items())

@celery_tasks.task(name='tasks.simulate_passengers')
def simulate_passengers(simulation_id, origin_airport_id, number_of_passengers, start_date, end_date):
    global db
    if db is None:
        db = pymongo.MongoClient(config.mongo_uri)[config.mongo_db_name]
    # datetime objects cannot be passed to tasks, so they are passed in as strings.
    start_date = dateparser.parse(start_date)
    end_date = dateparser.parse(end_date)
    for itinerary in my_airport_flow_calculator.calculate_itins(
        origin_airport_id,
        simulated_passengers=number_of_passengers,
        start_date=start_date,
        end_date=end_date):
        itin = {
            "origin": itinerary[0],
            "destination": itinerary[-1],
            "simulationId": simulation_id
        }
        db.simulated_itineraries.insert(itin)
    return simulation_id

@celery_tasks.task(name='tasks.callback')
def callback(data, email, simId):
    if not email == None:
        print "Sending notificaiton email to: {0}".format(email)
        print "For simulation https://{0}/simulation/{1}".format(config.flirt_base,simId)
        email_from = "support@eha.io"
        email_subject = "FLIRT simulation complete"
        email_text = """Your FLIRT simulation has completed.  Please click the link below to view the results:
        
        https://flirt.eha.io/simulation/{0}
        """.format(simId)
        msg = MIMEText(email_text)
        msg['Subject'] = email_subject
        msg['From'] = email_from
        msg['To'] = email
        msg['Body'] = email_text
        s = smtplib.SMTP_SSL(config.smtp, config.smtp_port)
        s.login(config.smtp_user, config.smtp_password)
        s.sendmail(email_from, email, msg.as_string())
        s.quit()
