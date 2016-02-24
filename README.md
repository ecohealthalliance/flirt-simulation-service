# flirt-simulation-service

This microservice provides an interface for creating a `flight_network_heatmap`
simulation. Before starting the service, one should create a
[virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/)
within the cloned repository and install the requirements.

```
  git clone git@github.com:ecohealthalliance/flirt-simulation-service.git
  cd flirt-simulation-service
  virtualenv env
  source env/bin/activate
  pip install -r requirements.txt
```

## Tornado web service

To web service uses [Tornado](http://tornadoweb.org). The server may be
started with the default option with the following command:

```
python server.py
```

This will start a Tornado server on port 45000 and save simulation requests to
mongodb running on `localhost:27017` and the database `grits`.  One may change
the default values by passing command line arguments.

```
python server.py --port 45000 --mongo_host 127.0.0.1 --mongo_port 27017 --mongo_database grits
```

## Celery queue

The simulator uses a distributed queue to calculate the results.  Therefore, at
least one worker server should be started using the steps below.

```
cd simulator/
celery worker -A tasks --loglevel=INFO --concurrency=2
```

## POST Requests

After the Tornado and Celery have been started, the web service will be ready
to accept POST requests at `/simulator` URL.  The following parameters are
required:

```
{
  'departureNodes': { 'type': 'list', 'required': True, 'minlength': 1, 'allowed': self.nodes, 'schema': {'type': 'string'}},
  'numberPassengers': { 'type': 'integer', 'required': True},
  'startDate': { 'type': 'datetime', 'required': True},
  'endDate': { 'type': 'datetime', 'required': True},
  'submittedBy': {'type': 'string', 'regex': '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', 'required': True},
}
```
