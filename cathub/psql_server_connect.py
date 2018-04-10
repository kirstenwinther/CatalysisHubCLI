import os
from sys import argv

user = argv[1]

os.system('psql --host=catalysishub.c8gwuc8jwb7l.us-west-2.rds.amazonaws.com --port=5432 --username={0} --dbname=catalysishub --password'.format(user))
