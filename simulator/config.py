import os

if 'MONGO_URI' in os.environ:
        mongo_uri = os.environ['MONGO_URI']
else:
        mongo_uri="mongodb://localhost:27017"


if 'MONGO_DB' in os.environ:
        mongo_db_name = os.environ['MONGO_DB']
else:
        mongo_db_name="grits"

