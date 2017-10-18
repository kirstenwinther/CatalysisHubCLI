import ase
from ase.db import *
from sys import argv
from catappsqlite import *

db = ase.db.connect('atoms.db')
n = db.count('id>0')
print 'ASE atoms:  ',  n 
    

catapp = CatappSQLite('catapp.db')
con = catapp._connect()
cur = con.cursor()

n = catapp.get_last_id(cur)

print 'Catapp:  ', n
