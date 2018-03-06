from sys import argv
from postgresql import CathubPostgreSQL

def main(dbfile, start_id=1, write_reaction=True, write_ase=True, 
         write_publication=True, write_reaction_system=True):

    db = CathubPostgreSQL()
    db.transfer(dbfile, start_id=start_id, write_reaction=write_reaction,
                write_ase=write_ase,
                write_publication=write_publication,
                write_reaction_system=write_reaction_system)

dbfile = argv[1]

if __name__ == '__main__':
    main(dbfile)



