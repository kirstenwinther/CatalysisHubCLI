from sys import argv
from postgresql import CatappPostgreSQL

def main(dbfile):
    db = CathubPostgreSQL()
    db.transfer(db_file)

dbfile = argv[1]

if __name__ == '__main__':
    main(dbfile)



