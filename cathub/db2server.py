from sys import argv
from postgresql import CathubPostgreSQL

def main(dbfile):
    db = CathubPostgreSQL()
    db.transfer(dbfile)

dbfile = argv[1]

if __name__ == '__main__':
    main(dbfile)



