import os

def main(user):
    os.system('psql --host=catalysishub.c8gwuc8jwb7l.us-west-2.rds.amazonaws.com --port=5432 --username={} --dbname=catalysishub --password'.format(user))

<<<<<<< HEAD
if __name__ == '__main__':
    from sys import argv
    user = argv[1]
    main(user)
=======
os.system('psql --host=catalysishub.c8gwuc8jwb7l.us-west-2.rds.amazonaws.com --port=5432 --username={0} --dbname=catalysishub --password'.format(user))
>>>>>>> f205f563094dd22839d4a0e8c3b02fd43b0f8fd7
