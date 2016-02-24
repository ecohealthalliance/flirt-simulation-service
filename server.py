import logging
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
from datetime import datetime
from simulator import tasks
import pymongo
import pandas as pd
import cachetools

define('port', default=45000, help='try running on a given port', type=int)
define('debug', default=True, help='enable debugging', type=bool)
define('mongo_host', default='10.0.0.175', help='mongo server hostname', type=str)
define('mongo_port', default=27017, help='mongo server port number', type=int)
define('mongo_database', default='grits', help='mongo database name', type=str)
define('promed_db_name', default='promed', help='name of mongo database with promed records', type=str)

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
            'simId': { 'type': 'string', 'required': True},
            'departureNode': { 'type': 'string', 'required': True},
            'numberPassengers': { 'type': 'integer', 'required': True},
            'startDate': { 'type': 'datetime', 'required': True},
            'endDate': { 'type': 'datetime', 'required': True},
            'submittedBy': {'type': 'string', 'regex': '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', 'required': True},
            'submittedTime': { 'type': 'datetime', 'required': True}}

    def __init__(self):
        self.fields = {} #collections.OrderedDict()
        self.validator = Validator(self.schema)

    def gen_key(self):
        """ generate a unique key for this record """
        h = hashlib.md5()
        try:
            h.update(self.fields['departureNode'])
            h.update(str(self.fields['numberPassengers']))
            h.update(str(self.fields['startDate']))
            h.update(str(self.fields['endDate']))
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
        self.fields['submittedTime'] = datetime.utcnow()
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
        def _queue_simulation():
            # get parameters for the job
            sim_id = self.simulationRecord.fields['simId']
            departure_node = self.simulationRecord.fields['departureNode']
            num_passengers = self.simulationRecord.fields['numberPassengers']
            start = str(self.simulationRecord.fields['startDate'])
            end = str(self.simulationRecord.fields['endDate'])
            # send the job to the queue
            res = tasks.simulate_passengers.delay(sim_id, departure_node, num_passengers, start, end)
            return res.id

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
                self.simulationRecord.fields['taskId'] = _queue_simulation()
                self.db.simulations.insert(self.simulationRecord.fields, callback=_on_insert)

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

        self.db.simulations.find_one({'simId': self.simulationRecord.fields['simId']}, callback=_on_find)
        return

@cachetools.func.ttl_cache(maxsize=1, ttl=60 * 60 * 24) # 24 hour ttl value
def get_blindspot_data(db):
    q = db.posts.aggregate([
      { "$unwind" : "$articles" },
      { "$unwind": "$articles.geoannotations" },
      {
        "$group" : {
            "_id" : {
                "$concat":[
                    "$articles.geoannotations.country code",
                    ";;",
                    {"$substr": [{ "$year": "$promedDate" }, 0, 4]}
                ]
            },
            "mentions" : { "$sum" : 1 }
        }
      }
    ])
    mention_data = pd.DataFrame([
        dict(
            CC=r['_id'].split(";;")[0],
            year=r['_id'].split(";;")[1],
            mentions=r['mentions'])
        for r in q['result']])
    # The country table comes from geonames.org
    # It is used for population data.
    country_table = pd.read_csv(
        "country_table.tsv",
        sep='\t',
        # The NA ISO code for Namibia will be parsed as an NA value without this.
        keep_default_na=False)
    # Create a table with a row for every country-year combinaton
    country_table = country_table[['ISO', 'Country', 'Area(in sq km)', 'Population']]
    country_table['Population'] = country_table['Population'].convert_objects(convert_numeric=True)
    year_table = pd.DataFrame(pd.unique(mention_data.year), columns=['year'])
    year_table['empty'] = year_table.year.isnull()
    country_table['empty'] = country_table.ISO.isnull()
    country_year_table = year_table.merge(country_table, on='empty', how='inner')
    result = country_year_table.merge(
        mention_data,
        left_on=["ISO", "year"],
        right_on=["CC", "year"],
        how='left')
    result["mentions"] = result["mentions"].fillna(0)
    result["mentions per capita"] = result["mentions"] / result["Population"]
    return {
        "result": result[[
            "ISO",
            "Country",
            "Population",
            "Area(in sq km)",
            "mentions",
            "mentions per capita",
            "year"]].to_dict(orient="index").values()
    }
class BlindspotsHandler(BaseHandler):
    def post(self):
        print 1
        self.write(get_blindspot_data(self.application.promed_db))
        return self.finish()

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", HomeHandler),
            (r"/simulator", SimulationHandler),
            (r"/blindspots", BlindspotsHandler),
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
        self.promed_db = pymongo.MongoClient(options.mongo_host, options.mongo_port)[options.promed_db_name]
        # ensure index on simId
        self.db.simulations.create_index([
            ("simId", pymongo.ASCENDING)
        ], unique=True, name="idxSimulations_simId")

        super(Application, self).__init__(handlers, **settings)

def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()
