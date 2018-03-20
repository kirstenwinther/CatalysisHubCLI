from sys import argv
from postgresql import CathubPostgreSQL

def main(user):
    db = CathubPostgreSQL(password='catappdb')  
    db.create_user(user)

if __name__ == '__main__':
    user = argv[1]
    main(user)
