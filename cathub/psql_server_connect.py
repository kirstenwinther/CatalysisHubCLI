import os
from sys import argv

user = argv[1]

os.system('psql --host=catappdatabase.cjlis1fysyzx.us-west-1.rds.amazonaws.com --port=5432 --username={} --dbname=catappdatabase --password'.format(user))

