import os

if 'MONGO_URI' in os.environ:
        mongo_uri = os.environ['MONGO_URI']
else:
        mongo_uri="mongodb://localhost:27017"

if 'MONGO_DB' in os.environ:
        mongo_db_name = os.environ['MONGO_DB']
else:
        mongo_db_name="grits-net-meteor"

if 'FLIRT_BASE' in os.environ:
        flirt_base = os.environ['FLIRT_BASE']
else:
        flirt_base="https://flirt.eha.io"

if 'SMTP' in os.environ:
        smtp = os.environ['SMTP']
else:
        smtp="email-smtp.us-east-1.amazonaws.com"

if 'SMTP_PORT' in os.environ:
        smtp_port = os.environ['SMTP_PORT']
else:
        smtp_port=465

# ************ATTENTION*************
# Make sure to remove the user/password before commit changes to github.  A safer move would be to just set the env variables.
if 'SMTP_USER' in os.environ:
        smtp_user = os.environ['SMTP_USER']
else:
        smtp_user="{Enter user here}"

if 'SMTP_PASSWORD' in os.environ:
        smtp_password = os.environ['SMTP_PASSWORD']
else:
        smtp_password="{Enter password here}"
