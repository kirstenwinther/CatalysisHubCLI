import psycopg2
import os

try:  #sherlock 1 or 2
    sherlock = os.environ['SHERLOCK']
    if sherlock == '1':
        catbase = '/home/winther/data_catapp/'
    elif sherlock == '2':
        catbase = '/home/users/winther/data_catapp/'
except:  # SUNCAT
    catbase = '/nfs/slac/g/suncatfs/data_catapp/'

data_base = catbase + 'winther/databases/'
print data_base

from postgresql import CatappPostgreSQL

db = CatappPostgreSQL()
db.transfer(data_base + 'catapp.db')

"""
conn = psycopg2.connect(host="catappdatabase.cjlis1fysyzx.us-west-1.rds.amazonaws.com", 
                                user='catappuser',
                                password='catappdb',
                                port=5432,
                                database='catappdatabase')

cur = conn.cursor()

cur.execute("DROP TABLE catapp;")
#cur.execute("CREATE TABLE catapp (id serial PRIMARY KEY);")

conn.commit()

cur.close()
conn.close()        
"""

