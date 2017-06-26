import celery
import logging
import pymongo
import datetime
from AirportFlowCalculator import AirportFlowCalculator
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

db = pymongo.MongoClient(config.mongo_uri)[config.mongo_db_name]
my_airport_flow_calculator = AirportFlowCalculator(db)

SIMULATED_PASSENGERS = 1000

@celery_tasks.task(name='tasks.calculate_flows_for_airport')
def calculate_flows_for_airport(airport_id):
    results = my_airport_flow_calculator.calculate(
        airport_id, simulated_passengers=SIMULATED_PASSENGERS)
    db.heatmap.find_one_and_replace(
        {'_id': airport_id},
        dict(
            { k: v['terminal_flow'] for k, v in results.items() },
            lastModified=datetime.datetime.now(),
            simulatedPassengers=SIMULATED_PASSENGERS,
            version='0.0.1'),
        upsert=True)
    return airport_id, len(results)

@celery_tasks.task(name='tasks.simulate_passengers')
def simulate_passengers(simulation_id, origin_airport_id, number_of_passengers, start_date, end_date):
    # datetime objects cannot be passed to tasks, so they are passed in as strings.
    start_date = dateparser.parse(start_date)
    end_date = dateparser.parse(end_date)
    results = my_airport_flow_calculator.calculate(
        origin_airport_id,
        store_itins_with_id=simulation_id,
        start_date=start_date,
        end_date=end_date,
        simulated_passengers=number_of_passengers)
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
