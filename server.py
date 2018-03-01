import logging
import os
import os.path
import hashlib
import json
import collections
import motor
import pymongo
import tornado.web
import tornado.ioloop
import tornado.httpserver
from tornado.options import define, options
from tornado import gen
from bson import json_util
from cerberus import Validator
import datetime
from simulator import tasks
import celery

__VERSION__ = '0.0.3'

if 'SIMULATION_PORT' in os.environ:
        _port = int(os.environ['SIMULATION_PORT'])
else:
        _port=45000

if 'MONGO_HOST' in os.environ:
        _mongo_host = os.environ['MONGO_HOST']
else:
        _mongo_host='localhost'

if 'MONGO_PORT' in os.environ:
        _mongo_port = int(os.environ['MONGO_PORT'])
else:
        _mongo_port=27017

if 'MONGO_DB' in os.environ:
        _mongo_db = os.environ['MONGO_DB']
else:
        _mongo_db='grits-net-meteor'

define('port', default=_port, help='try running on a given port', type=int)
define('debug', default=True, help='enable debugging', type=bool)
define('mongo_host', default=_mongo_host, help='mongo server hostname', type=str)
define('mongo_port', default=_mongo_port, help='mongo server port number', type=int)
define('mongo_database', default=_mongo_db, help='mongo database name', type=str)
define('node_collection', default='airports', help='mongo node collection name', type=str)

class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return self.application.db

    @property
    def nodes(self):
        return self.application.nodes

class HomeHandler(BaseHandler):
    def get(self):
        self.write({'version':self.application.settings['version']})

class SimulationRecord():
    """ class that represents the mondoDB simulation document """
    @property
    def post_parameters(self):
        """ list of items that are expected via post"""
        return ['departureNodes', 'numberPassengers', 'startDate', 'endDate', 'submittedBy', 'notificationEmail']

    @property
    def schema(self):
        """ the cerberus schema definition used for validation """
        return {
            # _id will be assigned by mongo
            'simId': { 'type': 'string', 'required': True},
            'departureNodes': { 'type': 'list', 'required': True, 'minlength': 1, 'allowed': self.nodes, 'schema': {'type': 'string'}},
            'numberPassengers': { 'type': 'integer', 'required': True},
            'startDate': { 'type': 'datetime', 'required': True},
            'endDate': { 'type': 'datetime', 'required': True},
            'submittedBy': {'type': 'string', 'regex': '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', 'required': True},
            'submittedTime': { 'type': 'datetime', 'required': True},
            'notificationEmail': { 'type': 'string', 'regex': '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', 'required': False}}

    def __init__(self, nodes):
        self.nodes = nodes
        self.fields = {} #collections.OrderedDict()
        self.validator = Validator(self.schema)

    def gen_key(self):
        """ generate a unique key for this record """
        h = hashlib.md5()
        try:
            h.update(str(self.fields['departureNodes']))
            h.update(str(self.fields['numberPassengers']))
            h.update(str(self.fields['startDate']))
            h.update(str(self.fields['endDate']))
            h.update(str(__VERSION__))
            return h.hexdigest()
        except:
            return None

    def is_valid(self):
        return self.validator.validate(self.fields)

    def create(self, req):
        for param in self.post_parameters:
            try:
                raw_value = req.get_argument(param)
            except:
                continue

            data_type = self.schema[param]['type'].lower()

            if data_type == 'list':
                if SimulationRecord.could_be_list(raw_value):
                    self.fields[param] = [x.strip() for x in raw_value.split(',')]
                else:
                    self.fields[param] = None
                continue

            if data_type == 'string':
                if SimulationRecord.is_empty_str(raw_value):
                    self.fields[param] = None
                else:
                    self.fields[param] = raw_value
                continue

            if data_type == 'integer':
                if SimulationRecord.could_be_int(raw_value):
                    self.fields[param] = int(raw_value)
                else:
                    self.fields[param] = None
                continue

            if data_type == 'datetime':
                datetime_format = '%d/%m/%Y'
                if SimulationRecord.could_be_datetime(raw_value, datetime_format):
                    self.fields[param] = datetime.datetime.strptime(raw_value, datetime_format)
                else:
                    self.fields[param] = None
                continue

        # default values
        self.fields['submittedTime'] = datetime.datetime.utcnow()
        self.fields['simId'] = self.gen_key()

    def validation_errors(self):
        errors = self.validator.errors
        for key, value in errors.iteritems():
            # the standard error for a regex isn't human readable
            if key == 'submittedBy':
                errors[key] = 'value is not a valid e-mail address'
        return errors

    def to_json(self):
        """ dumps the records fields into JSON format """
        return json.dumps(self.fields, default=json_util.default)

    @staticmethod
    def could_be_list(val):
        """ determines if the val is an instance of list """
        if val == None:
            return False
        if isinstance(val, list):
            return True
        if isinstance(val, str) or isinstance(val, unicode):
            lst = [x.strip() for x in val.split(',')]
            if isinstance(lst, list):
                if len(lst) > 0:
                    return True
                else:
                    return False
            else:
                return False
        return False

    @staticmethod
    def could_be_int(val):
        """ determines if the val is an instance of int or could be coerced
        to an int from a string """
        if val == None:
            return False

        if isinstance(val, int):
            return True

        # allow coercion from str
        if isinstance(val, (str, unicode)):
            try:
                i = int(val)
                if not isinstance(i, int):
                    raise ValueError
                else:
                    return True
            except:
                return False

        # otherwise
        return False

    @staticmethod
    def could_be_datetime(val, fmt):
        """ determines if the val is an instance of datetime or could be coerced
        to a datetime from a string with the provided format"""

        if val == None or fmt == None:
            return False

        if isinstance(val, datetime.datetime):
            return True

        if isinstance(val, (str, unicode)):
            if SimulationRecord.is_empty_str(val) or SimulationRecord.is_empty_str(fmt):
                return False

            try:
                d = datetime.datetime.strptime(val, fmt)
                if not isinstance(d, datetime.datetime):
                    raise ValueError
                else:
                    return True
            except Exception as e:
                logging.error(e)
                return False

        #otherwise
        return False

    @staticmethod
    def is_empty_str(val):
        """ check if the val is an empty string"""
        s = str(val)
        if not isinstance(s, str):
            return False
        if not s.strip():
            return True
        else:
            return False

class SimulationHandler(BaseHandler):
    @tornado.web.asynchronous
    def post(self):
        logging.info("Simulation request received")
        outgoing_seat_counts = {}
        def get_outgoing_seat_counts(callback):
            cursor = self.db.legs.aggregate([
                {
                    '$match' : {
                        'departureAirport._id' : {
                            '$in' : self.simulationRecord.fields['departureNodes']
                        },
                        'effectiveDate': {
                            "$lte" : self.simulationRecord.fields['endDate']
                        },
                        'discontinuedDate': {
                            "$gte" : self.simulationRecord.fields['startDate']
                        }
                    }
                }, {
                    '$project' : {
                        'departureAirport._id' : 1,
                        'totalSeats' : 1,
                        'weeklyFrequency' : {
                            '$sum': [
                                { '$cond': [ '$day1', 1, 0 ] },
                                { '$cond': [ '$day2', 1, 0 ] },
                                { '$cond': [ '$day3', 1, 0 ] },
                                { '$cond': [ '$day4', 1, 0 ] },
                                { '$cond': [ '$day5', 1, 0 ] },
                                { '$cond': [ '$day6', 1, 0 ] },
                                { '$cond': [ '$day7', 1, 0 ] },
                            ]
                        },
                        'weeklyRepeats' : {
                            '$let' : {
                                'vars' : {
                                    'millisStartToEnd' : {
                                        '$subtract': [
                                            { '$min' : [
                                                self.simulationRecord.fields['endDate'],
                                                '$discontinuedDate'
                                                ] },
                                            { '$max' : [
                                                self.simulationRecord.fields['startDate'],
                                                '$effectiveDate'
                                                ] }
                                        ]
                                    }
                                },
                                'in' : {
                                    '$divide' : [
                                        {
                                            '$add' : [
                                                '$$millisStartToEnd',
                                                # one day in milliseconds to
                                                # account for the end day.
                                                24 * 60 * 60 * 1000
                                            ]
                                        },
                                        # one week in milliseconds
                                        7 * 24 * 60 * 60 * 1000
                                    ]
                                }
                            }
                        }
                    }
                }, {
                    '$group' : {
                        '_id' : '$departureAirport._id',
                        # This is an aproximation because only the fraction of
                        # days the flight runs on in the start/end weeks is
                        # computed rather than counting how many days the flight
                        # is schedule on that occur before/afer the day of the
                        # week that the flight starts/ends on.
                        'totalSeats' : {
                            '$sum' : { '$multiply' : ['$totalSeats', '$weeklyFrequency', '$weeklyRepeats'] }
                        }
                    }
                }
            ])
            def _aggregation_complete(docs, err):
                if err:
                    logging.info("Error:")
                    logging.info(err)
                    raise err
                for doc in docs:
                    outgoing_seat_counts[doc['_id']] = doc['totalSeats']
                callback()
            cursor.to_list(None, callback=_aggregation_complete)

        def _queue_simulation():
            # get parameters for the job(s)
            task_ids = []
            logging.info("Outgoing seat counts:")
            logging.info(outgoing_seat_counts)
            total_seat_count = sum(outgoing_seat_counts.values())
            num_departures = len(self.simulationRecord.fields['departureNodes'])
            if num_departures == 0:
                logging.info("No seats for the given airports:")
                return
            if total_seat_count == 0:
                logging.info("No seats for the given airports:")
                logging.info(self.simulationRecord.fields['departureNodes'])
                return
            num_passengers = self.simulationRecord.fields['numberPassengers']
            sim_id = self.simulationRecord.fields['simId']
            start = str(self.simulationRecord.fields['startDate'])
            end = str(self.simulationRecord.fields['endDate'])
            email = self.simulationRecord.fields.get('notificationEmail', None)
            arg_list = []
            for node in self.simulationRecord.fields['departureNodes']:
                node_passengers = int(round(float(num_passengers * outgoing_seat_counts.get(node, 0)) / total_seat_count))
                arg_list.append({'origin_airport_id': node, 'number_of_passengers': node_passengers})
            #take all of the args from art_list and use them to create a chord of tasks for calls to simulate_passengers
            res = celery.chord(
                tasks.simulate_passengers.s(sim_id,i['origin_airport_id'],i['number_of_passengers'],start,end)
                for i in arg_list
                )(tasks.callback.s(email, sim_id))
            task_ids = [task.id for task in res.parent.results]
            logging.info('simId: %s, task_ids: %r', sim_id, task_ids)
            return task_ids

        def _on_insert(message, error):
            if error:
                logging.error('error: %r', error)
                self.write({
                    'error': True,
                    'message': 'database error'
                })
                self.finish()
                return

            self.write({'simId': self.simulationRecord.fields['simId']})
            self.finish()

        def _on_find(message, error):
            if error:
                logging.error('error: %r', error)
                self.write({
                    'error': True,
                    'message': 'database error'
                })
                self.finish()
                return
            if message:
                self.write({'simId': message['simId']})
                self.finish()
            else:
                def _seat_counts_gotten():
                    self.simulationRecord.fields['taskIds'] = _queue_simulation()
                    self.db.simulations.insert(self.simulationRecord.fields, callback=_on_insert)
                get_outgoing_seat_counts(callback=_seat_counts_gotten)
        self.simulationRecord = SimulationRecord(self.nodes)
        self.simulationRecord.create(self)
        if not self.simulationRecord.is_valid():
            self.write({
                'error': True,
                'message': 'invalid parameters',
                'details': self.simulationRecord.validation_errors()
            })
            self.finish()
            return

        self.db.simulations.find_one({'simId': self.simulationRecord.fields['simId']}, callback=_on_find)
        return

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", HomeHandler),
            (r"/simulator", SimulationHandler),
        ]
        settings = dict(
            version='0.0.1',
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            debug=options.debug,
        )

        # Mongo connection
        client = motor.motor_tornado.MotorClient(options.mongo_host, options.mongo_port)
        self.db = client[options.mongo_database]

        # ensure index on simId
        self.db.simulations.create_index([
            ("simId", pymongo.ASCENDING)
        ], unique=True, name="idxSimulations_simId")

        self.nodes = []
        @gen.coroutine
        def get_nodes():
            cursor = self.db[options.node_collection].find({}, {'_id': 1})
            while (yield cursor.fetch_next):
                doc = cursor.next_object()
                self.nodes.append(doc['_id'])
        # Run a couple sims on start up to warm up the simulator
        start_date = datetime.datetime.now()
        end_date = start_date + datetime.timedelta(7)
        warmup_result = celery.group(
            tasks.simulate_passengers.s(
                'warmup',
                'LAX',
                200,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            ),
            tasks.simulate_passengers.s(
                'warmup',
                'ATL',
                200,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )
        )()
        # Wait for warmup sims to finish.
        warmup_result.get(timeout=None, interval=2.0)
        tornado.ioloop.IOLoop.current().run_sync(get_nodes)
        logging.info('Ready to simulate [%s] nodes', len(self.nodes))
        super(Application, self).__init__(handlers, **settings)

def main():
    tornado.options.parse_command_line()
    logging.info('port: %r', options.port)
    logging.info('mongo_host: %r', options.mongo_host)
    logging.info('mongo_port: %r', options.mongo_port)
    logging.info('mongo_database: %r', options.mongo_database)
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()
