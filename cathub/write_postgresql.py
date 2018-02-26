import psycopg2
import sys
from tools import get_bases
from postgresql import CatappPostgreSQL

db_file = sys.argv[1]

db = CatappPostgreSQL()
db.transfer(db_file)



