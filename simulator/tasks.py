import celery
import logging
import pymongo
import datetime
from AirportFlowCalculator import AirportFlowCalculator, compute_direct_seat_flows
from dateutil import parser as dateparser
import config
import smtplib
from email.mime.text import MIMEText

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BROKER_URL = config.mongo_uri + '/tasks'

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

direct_seat_flows = None
prior_start_date = None
db = None
my_airport_flow_calculator = None
current_mode = None


def maybe_update_direct_seat_flows(start_date, end_date):
    global direct_seat_flows
    global prior_start_date
    if direct_seat_flows is None or prior_start_date != start_date:
        prior_start_date = start_date
        direct_seat_flows = compute_direct_seat_flows(
            pymongo.MongoClient(config.mongo_uri)[config.mongo_db_name], {
                "departureDateTime": {
                    "$lte": end_date,
                    "$gte": start_date
                }
            })

def maybe_initialize_variables():
    """
    Initialize global variables that can be reused between tasks and if required.
    """
    global my_airport_flow_calculator
    global db
    all_time_direct_seat_flows = compute_direct_seat_flows(db, {})
    if my_airport_flow_calculator is None:
        db = pymongo.MongoClient(config.mongo_uri)[config.mongo_db_name]
        my_airport_flow_calculator = AirportFlowCalculator(db, aggregated_seats=all_time_direct_seat_flows)
    return my_airport_flow_calculator, db

@celery_tasks.task(name='tasks.calculate_flows_for_airport_14_days')
def calculate_flows_for_airport_14_days(origin_airport_id, start_date):
    SIMULATED_PASSENGERS = 10000
    period_days = 14
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = start_date + datetime.timedelta(period_days)
    maybe_update_direct_seat_flows(start_date, end_date)
    maybe_initialize_variables()
    # Drop all results for origin airport
    db.passengerFlows.delete_many({
        'departureAirport': origin_airport_id
    })
    results = my_airport_flow_calculator.calculate(
        origin_airport_id,
        simulated_passengers=SIMULATED_PASSENGERS,
        start_date=start_date,
        end_date=end_date)
    if len(results) > 0:
        seats_per_pasenger = sum(legs * value for legs, value in AirportFlowCalculator.LEG_PROBABILITY_DISTRIBUTION.items())
        total_seats = sum(direct_seat_flows[origin_airport_id].values())
        total_passengers = int(float(total_seats) / seats_per_pasenger)
        db.passengerFlows.insert_many({
            'departureAirport': origin_airport_id,
            'arrivalAirport': k,
            'estimatedPassengers': v['terminal_flow'] * total_passengers,
            'recordDate': datetime.datetime.now(),
            'startDateTime': start_date,
            'endDateTime': end_date,
            'periodDays': period_days
        } for k, v in results.items())
        return len(results)
    else:
        print "No flights from: " + origin_airport_id
        return 0

@celery_tasks.task(name='tasks.simulate_passengers')
def simulate_passengers(simulation_id, origin_airport_id, number_of_passengers, start_date, end_date):
    maybe_initialize_variables()
    # datetime objects cannot be passed to tasks, so they are passed in as strings.
    start_date = dateparser.parse(start_date)
    end_date = dateparser.parse(end_date)
    itins_found = False
    for itinerary in my_airport_flow_calculator.calculate_itins(
        origin_airport_id,
        simulated_passengers=number_of_passengers,
        start_date=start_date,
        end_date=end_date):
        itins_found = True
        itin = {
            "origin": itinerary[0],
            "destination": itinerary[-1],
            "simulationId": simulation_id
        }
        db.simulated_itineraries.insert(itin)
    if not itins_found:
        raise Exception("No itineraries could be generated for the given parameters")
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
