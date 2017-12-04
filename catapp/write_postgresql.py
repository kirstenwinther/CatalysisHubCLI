import psycopg2
import os
import sys

try:  #sherlock 1 or 2
    sherlock = os.environ['SHERLOCK']
    if sherlock == '1':
        catbase = '/home/winther/data_catapp/'
    elif sherlock == '2':
        catbase = '/home/users/winther/data_catapp/'
except:  # SUNCAT
    catbase = '/nfs/slac/g/suncatfs/data_catapp/'

data_base = catbase + 'winther/databases/'

from postgresql import CatappPostgreSQL
user = sys.argv[1]
db = CatappPostgreSQL()
db.transfer(data_base + 'catapp_{}.db'.format(user))



