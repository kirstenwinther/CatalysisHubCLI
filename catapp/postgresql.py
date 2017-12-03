import psycopg2

init_command = \
                """CREATE TABLE catapp (
                id SERIAL PRIMARY KEY,
                chemical_composition text,
                surface_composition text,
                facet text,
                sites text,
                reactants jsonb,
                products jsonb,
                reaction_energy numeric,
                activation_energy numeric,
                dft_code text,
                dft_functional text,
                publication jsonb,
                doi text,
                year smallint,
                ase_ids jsonb
                );"""


class CatappPostgreSQL:
    def __init__(self):
        self.initialized = False
        self.connection = None
        self.id = None

    def _connect(self):
        import os
        password = os.environ['DB_PASSWORD']
        con = psycopg2.connect(host="catappdatabase.cjlis1fysyzx.us-west-1.rds.amazonaws.com", 
                               user='catappuser',
                               password=password,
                               port=5432,
                               database='catappdatabase')
        
        return con

    def __enter__(self):
        assert self.connection is None
        self.connection = self._connect()
        return self

    def __exit__(self, exc_type):#, exc_value, tb):
        if exc_type is None:
            self.connection.commit()
        else:
            self.connection.rollback()
        self.connection.close()
        self.connection = None

    def _initialize(self, con):
        if self.initialized:
            return
        cur = con.cursor()

        cur.execute("""SELECT to_regclass('catapp');""")
        if cur.fetchone()[0] == None:  # Catapp doesn't exist
             cur.execute(init_command)
             con.commit()
        self.initialized = True
        return self

    def status(self):
        con = self.connection or self._connect()
        self._initialize(con)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) from catapp;")
        print cur.fetchall()
        #cur.execute("""SELECT reactants, products FROM catapp where reference""")
        #print len(cur.fetchall())
 
    def write(self, values):
        con = self.connection or self._connect()
        self._initialize(con)
        cur = con.cursor()
        #id = self.get_last_id(cur)
        #if self.id is None:
        #    id = self.get_last_id(cur) + 1
        #else:
        #    id = self.id
        key_str, value_str = get_key_value_str(values)

        insert_command = 'INSERT INTO catapp ({}) VALUES ({}) RETURNING id;'.format(key_str, value_str)
        print insert_command
        cur.execute(insert_command)
        id = cur.fetchone()[0]
        if self.connection is None:
            con.commit()
            con.close()
        return id

        
    def update(self, id, values):
        con = self.connection or self._connect()
        self._initialize(con)
        cur = con.cursor()

        key_str, value_str = get_key_value_str(values)

        update_command = 'UPDATE catapp SET ({}) = ({}) WHERE id = {};'.format(key_str, value_str, id)

        print update_command
        cur.execute(update_command)
        #id = cur.fetchone()[0]
        if self.connection is None:
            con.commit()
            con.close()
        return id


    def transfer(self, filename_sqlite):
        from catappsqlite import CatappSQLite
        with CatappSQLite(filename_sqlite) as db:
            con_lite = db._connect()
            cur_lite = con_lite.cursor()
            n = db.get_last_id(cur_lite)
            for id_lite in range(1, n+1):
                row = db.read(id_lite)
                if len(row) == 0:
                    continue
                values = row[0]
                id = self.check(values[7])
                if id is not None:
                    print 'Allready in catapp db with row id = {}'.format(id)
                    id = self.update(id, values)
                else:
                    id = self.write(values)
                    print 'Written to catapp db row id = {}'.format(id)

    
    def check(self, reaction_energy):
        con = self.connection or self._connect()
        self._initialize(con)
        cur = con.cursor()
        statement = 'SELECT catapp.id FROM catapp WHERE catapp.reaction_energy={}'.format(reaction_energy)
        #argument = [reaction_energy]
        cur.execute(statement)
        rows = cur.fetchall()
        if len(rows) > 0:
            id = rows[0][0]
        else:
            id = None
        return id


def get_key_value_str(values):
    key_str = 'chemical_composition, surface_composition, facet, sites, reactants, products, reaction_energy, activation_energy, dft_code, dft_functional, publication, doi, year, ase_ids'
    value_str = "'{}'".format(values[1])
    for v in values[2:]:
        if isinstance(v, unicode):
            v = v.encode('ascii','ignore')
        if isinstance(v, str):
            value_str += ", '{}'".format(v)
        elif v is None or v == '':
            value_str += ", {}".format('NULL')
        else:
            value_str += ", {}".format(v)
        
    return key_str, value_str
