from sys import argv
from postgresql import CathubPostgreSQL

def main(dbfile, start_id=1, write_reaction=True, write_ase=True, 
         write_publication=True, write_reaction_system=True,
         block_size=1000, start_block=0):

    db = CathubPostgreSQL()
    db.transfer(dbfile, start_id=start_id, write_reaction=write_reaction,
                write_ase=write_ase,
                write_publication=write_publication,
                write_reaction_system=write_reaction_system,
                block_size=block_size,
                start_block=start_block)

dbfile = argv[1]

if __name__ == '__main__':
    main(dbfile)



