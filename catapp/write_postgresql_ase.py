import os
import subprocess
from sys import argv

password = os.environ['DB_PASSWORD']
user = argv[1]

subprocess.check_output(("""ase -T db winther/databases/atoms_{}.db --insert-into 'postgres://catappuser:{}@catappdatabase.cjlis1fysyzx.us-west-1.rds.amazonaws.com:5432/catappdatabase'""".format(user, password)).split())
