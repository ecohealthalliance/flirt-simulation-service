import celery
import logging
import pymongo
import datetime
from AirportFlowCalculator import AirportFlowCalculator, compute_direct_passenger_flows
from dateutil import parser as dateparser
import config
import smtplib
from email.mime.text import MIMEText
from pylru import lrudecorator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

celery_tasks = celery.Celery('tasks', broker=config.broker_url)
celery_tasks.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='json',
    CELERY_RESULT_BACKEND = config.broker_url,
    CELERY_MONGODB_BACKEND_SETTINGS = {
        'database': 'tasks',
        'taskmeta_collection': 'taskmeta',
    }
)

@lrudecorator(3)
def get_direct_passenger_flows(start_date, end_date):
    return compute_direct_passenger_flows(
        pymongo.MongoClient(config.mongo_uri)[config.mongo_db_name], {
            "departureDateTime": {
                "$lte": end_date,
                "$gte": start_date
            }
        })

@lrudecorator(1)
def get_database():
    db = pymongo.MongoClient(config.mongo_uri)[config.mongo_db_name]
    db.passengerFlows.ensure_index('simGroup')
    return db

@lrudecorator(1)
def get_airport_flow_calculator():
    """
    Initialize global variables that can be reused between tasks and if required.
    """
    db = get_database()
    all_time_direct_passenger_flows = compute_direct_passenger_flows(db, {})
    return AirportFlowCalculator(db, aggregated_seats=all_time_direct_passenger_flows)

@celery_tasks.task(name='tasks.calculate_flows_for_airport')
def calculate_flows_for_airport(origin_airport_id, start_date, end_date, sim_group):
    """
    Calculate the numbers of passengers that flow from the given origin to every other airport
    over the interval starting at start_date and store them in the passengerFlows collection.
    """
    SIMULATED_PASSENGERS = 10000
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    period_days = (end_date - start_date).days
    direct_passenger_flows = get_direct_passenger_flows(start_date, end_date)
    db = get_database()
    my_airport_flow_calculator = get_airport_flow_calculator()
    # Drop all results for origin airport
    db.passengerFlows.delete_many({
        'departureAirport': origin_airport_id,
        'simGroup': sim_group
    })
    results = my_airport_flow_calculator.calculate(
        origin_airport_id,
        simulated_passengers=SIMULATED_PASSENGERS,
        start_date=start_date,
        end_date=end_date)
    if len(results) > 0:
        seats_per_pasenger = sum(legs * value for legs, value in AirportFlowCalculator.LEG_PROBABILITY_DISTRIBUTION.items())
        total_direct_passengers = sum(direct_passenger_flows[origin_airport_id].values())
        total_passengers = int(float(total_direct_passengers) / seats_per_pasenger)
        db.passengerFlows.insert_many({
            'departureAirport': origin_airport_id,
            'arrivalAirport': k,
            'estimatedPassengers': v['terminal_flow'] * total_passengers,
            'averageDistance': v['average_distance'],
            'recordDate': datetime.datetime.now(),
            'startDateTime': start_date,
            'endDateTime': end_date,
            'periodDays': period_days,
            'simGroup': sim_group
        } for k, v in results.items())
        return len(results)
    else:
        print "No flights from: " + origin_airport_id
        return 0

@celery_tasks.task(name='tasks.simulate_passengers')
def simulate_passengers(simulation_id, origin_airport_id, number_of_passengers, start_date, end_date):
    db = get_database()
    my_airport_flow_calculator = get_airport_flow_calculator()
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
