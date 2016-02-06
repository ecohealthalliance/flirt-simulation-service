## To run the passenger simulation from the command line

Install the python requirements. I recommend setting up a [virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/)
before doing this.

```
pip install -r requirements.pip
```

Run a simulation with the default parameters:

```
python AirportFlowCalculator.py
```

Parameters such as the number of passengers and starting airport can be passed
in as arguments. Use this command for information on all available arguments:

```
python AirportFlowCalculator.py --help
```

## To concurrently process all the airports 

Obtain the csv with all the flight data
then import it into mongo using the code here:
https://github.com/ecohealthalliance/grits-net-consume

As a shortcut, you can download the flights and airports collection
in the S3 bucket by omiting the --collection flag in the download
instructions below. This data may be out of date.

```
python queue_airports.py
# With the current configuration, each process will need about 2GB of RAM
celery worker -A tasks --loglevel=INFO --concurrency=2
```

## Accesing this project's S3 Bucket:

Install the AWS CLI and configure your credentials:

```
sudo apt-get install awscli
```

## To download the heatmap data:

```
aws s3 cp --recursive s3://flight-network-heat-map/ .
```

To load the data into mongo:

```
mongorestore dump --collection heatmap --drop
```

## To upload an updated database dump:

```
mongodump --db grits --collection heatmap
aws s3 cp --recursive dump s3://flight-network-heat-map/dump
```

## To run the tests:

```
python -m unittest discover tests
```

## License
Copyright 2016 EcoHealth Alliance

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
