import psycopg2
import sys
from tools import get_bases
from postgresql import CatappPostgreSQL

user = sys.argv[1]
catbase, data_base, user, user_base = get_bases(user)
db = CatappPostgreSQL()
db.transfer(data_base + 'catapp_{}.db'.format(user))



