import logging
import os.path
import uuid
import json
import collections
import motor
import tornado.web
import tornado.ioloop
import tornado.httpserver
from tornado.options import define, options
from bson import json_util
from cerberus import Validator
from datetime import datetime

define('port', default=45000, help='try running on a given port', type=int)
define('debug', default=True, help='enable debugging', type=bool)
define('mongohost', default='localhost', help='mongo host name', type=str)
define('mongoport', default=27017, help='mongo host name', type=int)
define('mongodb', default='grits', help='mongo host name', type=str)

class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return self.application.db

class HomeHandler(BaseHandler):
    def get(self):
        self.write({'version':self.application.settings['version']})

class SimulationRecord():
    """ class that represents the mondoDB simulation document """
    @property
    def post_parameters(self):
        """ list of items that are expected via post"""
        return ['departureNode', 'numberPassengers', 'startDate', 'endDate', 'submittedBy']

    @property
    def schema(self):
        """ the cerberus schema definition used for validation """
        return {
            # _id will be assigned by mongo
            'simId': {'type': 'string', 'required': True},
            'departureNode': { 'type': 'string', 'required': True},
            'numberPassengers': { 'type': 'integer', 'required': True},
            'startDate': { 'type': 'datetime', 'required': True},
            'endDate': { 'type': 'datetime', 'required': True},
            'maxNumberLegs': {'type': 'integer', 'nullable': True, 'required': True},
            'maxLayoverTime': {'type': 'integer', 'nullable': True, 'required': True},
            'submittedBy': {'type': 'string', 'regex': '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', 'required': True},
            'submittedTime': { 'type': 'datetime', 'required': True}}

    def __init__(self):
        self.fields = {} #collections.OrderedDict()
        self.validator = Validator(self.schema)

    def is_valid(self):
        return self.validator.validate(self.fields)

    def create(self, req):
        for param in self.post_parameters:
            try:
                raw_value = req.get_argument(param)
            except:
                continue

            data_type = self.schema[param]['type'].lower()

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
                    self.fields[param] = datetime.strptime(raw_value, datetime_format)
                else:
                    self.fields[param] = None
                continue

        # default values
        self.fields['maxLayoverTime'] = 8
        self.fields['maxNumberLegs'] = 5
        self.fields['submittedTime'] = datetime.utcnow()
        self.fields['simId'] = str(uuid.uuid4())

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

        if isinstance(val, datetime):
            return True

        if isinstance(val, (str, unicode)):
            if SimulationRecord.is_empty_str(val) or SimulationRecord.is_empty_str(fmt):
                return False

            try:
                d = datetime.strptime(val, fmt)
                if not isinstance(d, datetime):
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
        self.simulationRecord = SimulationRecord()
        self.simulationRecord.create(self)
        if not self.simulationRecord.is_valid():
            self.write({
                'error': True,
                'message': 'invalid parameters',
                'details': self.simulationRecord.validation_errors()
            })
            self.finish()
            return

        # valid record, start the job and insert record into mongo
        self.db.simulations.insert(self.simulationRecord.fields, callback=self._on_response)

    def _on_response(self, message, error):
        if error:
            logging.info('error: %r', error)
            self.write({
                'error': True,
                'message': 'database error'
            })
            self.finish()
            return
        self.write({'simId': self.simulationRecord.fields['simId']})
        self.finish()

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

        # Have one global connection to mongo DB across all handlers
        client = motor.motor_tornado.MotorClient(options.mongohost, options.mongoport)
        self.db = client[options.mongodb]

        super(Application, self).__init__(handlers, **settings)

def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()
