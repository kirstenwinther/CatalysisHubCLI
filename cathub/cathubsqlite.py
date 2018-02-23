import sqlite3
import json

import numpy as np

init_commands = [
    """ CREATE TABLE publications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pub_id text UNIQUE,
    title text,
    authors text,
    journal text,
    volume text,
    number text,
    pages text,
    year integer,
    publisher text,
    doi text,
    tags text
    );""",

    """CREATE TABLE publication_structures (
    ase_id text REFERENCES systems(unique_id),
    pub_id text REFERENCES publications(pub_id),
    PRIMARY KEY (pub_id, ase_id)
    );""",
    
    """CREATE TABLE catapp (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chemical_composition text,
    surface_composition text,
    facet text,
    sites text,
    reactants text,
    products text,
    reaction_energy real,
    activation_energy real,
    dft_code text,
    dft_functional text,
    username text,
    pub_id text, 
    FOREIGN KEY (pub_id) REFERENCES publications(pub_id)
    );""",

    """ CREATE TABLE catapp_structures (
    name text,
    ase_id text,
    id integer,
    FOREIGN KEY (ase_id) REFERENCES systems(unique_id),
    FOREIGN KEY (id) REFERENCES catapp(id)
    );"""
]

#index_statements = [
#    'CREATE INDEX idxpubid ON publications(pub_id)',
#    'CREATE INDEX idxrecen ON catapp(reaction_energy)',
#    'CREATE INDEX idxchemcomp ON catapp(chemical_composition)',
#    'CREATE INDEX idxuser ON catapp(user)',
#]


class CatappSQLite:
    def __init__(self, filename):
        self.filename = filename
        self.initialized = False
        self.default = 'NULL'  # used for autoincrement id
        self.connection = None
        self.id = None
        self.pid = None

    def _connect(self):
        return sqlite3.connect(self.filename, timeout=600)

    def __enter__(self):
        assert self.connection is None
        self.connection = self._connect()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is None:
            self.connection.commit()
        else:
            self.connection.rollback()
        self.connection.close()
        self.connection = None

    def _initialize(self, con):
        if self.initialized:
            return

        import ase
        from ase.db.sqlite import SQLite3Database
        SQLite3Database()._initialize(con)

        self._metadata = {}

        cur = con.execute(
            'SELECT COUNT(*) FROM sqlite_master WHERE name="catapp"')
        if cur.fetchone()[0] == 0:
            for init_command in init_commands:
                con.execute(init_command)
            self.id = 1
            self.pid = 1
            con.commit()

        self.initialized = True

    def read(self, id, table='catapp'):
        con = self.connection or self._connect()
        self._initialize(con)
        cur = con.cursor()
        cur.execute('SELECT * FROM \n {} \n WHERE \n {}.id={}'.format(table,
                                                                      table,
                                                                      id))
        row = cur.fetchall()
        return row

    
    def write_publication(self, values):
        con = self.connection or self._connect()
        self._initialize(con)
        cur = con.cursor()
        if self.pid is None:
            try:
                pid = self.get_last_pub_id(cur) + 1
            except:
                pid = 1
        else:
            pid = self.pid

        values = (pid,
                  values['pub_id'],
                  values['title'],
                  json.dumps(values['authors']),
                  values['journal'],
                  values['volume'],
                  values['number'],
                  values['pages'],
                  values['year'],
                  values['publisher'],
                  values['doi'],
                  json.dumps(values['tags'])
        )

        q = ', '.join('?' * len(values))
        cur.execute('INSERT OR IGNORE INTO publications VALUES ({})'.format(q),
                    values)
        return pid
    
    def write(self, values, data=None):
        con = self.connection or self._connect()
        self._initialize(con)
        cur = con.cursor()
        if self.id is None:
            try:
                id = self.get_last_id(cur) + 1
            except:
                id = 1
        else:
            id = self.id

        pub_id = values['pub_id']
        ase_ids = values['ase_ids']
        if ase_ids is not None:
            ase_values = ase_ids.values()
            assert len(set(ase_values)) == len(ase_values), 'Dublicate ASE ids!'

            reaction_species = set(values['reactants'].keys() +
                                   values['products'].keys())
                        
            n_split = 0
            for spec in ase_ids.keys():
                if '-' in spec:
                    n_split += 1
            
            assert len(reaction_species) <= len(ase_values) + n_split, 'ASE ids missing!'
        else:
            ase_ids = {}
        values = (id,
                  values['chemical_composition'],
                  values['surface_composition'],
                  values['facet'],
                  values['sites'],
                  json.dumps(values['reactants']),
                  json.dumps(values['products']),
                  values['reaction_energy'],
                  values['activation_energy'],
                  values['dft_code'],
                  values['dft_functional'],
                  values['user'],
                  values['pub_id']#,
                  #values['doi'],
                  #int(values['year']),
                  )

        q = ', '.join('?' * len(values))
        cur.execute('INSERT INTO catapp VALUES ({})'.format(q),
                    values)
        catapp_structure_values = []
        for name, ase_id in ase_ids.items():
            catapp_structure_values.append([name, ase_id, id])
            cur.execute('INSERT OR IGNORE INTO publication_structures(ase_id, pub_id) VALUES (?, ?)', [ase_id, pub_id])
        cur.executemany('INSERT INTO catapp_structures VALUES (?, ?, ?)',
                        catapp_structure_values)

        if self.connection is None:
            con.commit()
            con.close()

        return id

    def update(self, id, values, key_names='all'):
        con = self.connection or self._connect()
        self._initialize(con)
        cur = con.cursor()
        values['year'] = int(values['year'])

        key_list, value_list = get_key_value_list(key_names, values)
        N_keys = len(key_list)
        
        #cur.execute('SELECT * from catapp where id = {}'.format(id))
        #row = cur.fetchall()
        #update_index = []
        #for i, val in enumerate(row[0][1:]):
        #    if not val == value_list[i]:
        #        print i, val, value_list[i]
        #        update_index.append(i)
        
        value_strlist = get_value_strlist(value_list)
        execute_str = ', '.join('{}={}'.format(key_list[i], value_strlist[i])
                                for i in range(N_keys))
        

        #execute_str = ', '.join('{}={}'.format(key_list[i], value_strlist[i]) for i in update_index)
        
        update_command = 'UPDATE catapp SET {} WHERE id = {};'\
            .format(execute_str, id)

        cur.execute(update_command)
        if self.connection is None:
            con.commit()
            con.close()
        return id


    def get_last_id(self, cur):
        cur.execute('SELECT seq FROM sqlite_sequence WHERE name="catapp"')
        id = cur.fetchone()[0]
        return id

    def get_last_pub_id(self, cur):
        cur.execute('SELECT seq FROM sqlite_sequence WHERE name="publications"')
        id = cur.fetchone()[0]
        return id
    
    def check(self, chemical_composition, reaction_energy):
        con = self.connection or self._connect()
        self._initialize(con)
        cur = con.cursor()
        statement = 'SELECT catapp.id FROM \n catapp \n WHERE \n catapp.chemical_composition=? and catapp.reaction_energy=?'
        argument = [chemical_composition, reaction_energy]
        
        cur.execute(statement, argument)
        rows = cur.fetchall()
        if len(rows) > 0:
            id = rows[0][0]
        else:
            id = None
        return id
        
    def check_publication(self, pub_id):
        con = self.connection or self._connect()
        self._initialize(con)
        cur = con.cursor()
        statement = 'SELECT id FROM \n publications \n WHERE \n publications.pub_id=?'
        argument = [pub_id]
        
        cur.execute(statement, argument)
        rows = cur.fetchall()
        if len(rows) > 0:
            id = rows[0][0]
        else:
            id = None
        return id

    def check_publication_structure(self, pub_id, ase_id):
        con = self.connection or self._connect()
        self._initialize(con)
        cur = con.cursor()
        statement = 'SELECT id FROM \n publication_structures \n WHERE \n publications.pub_id=? and publications.ase_id=?'
        argument = [pub_id, ase_id]
        
        cur.execute(statement, argument)
        rows = cur.fetchall()
        if len(rows) > 0:
            id = rows[0][0]
        else:
            id = None
        return id

    

def get_key_value_str(values):
    key_str = 'chemical_composition, surface_composition, facet, sites, reactants, products, reaction_energy, activation_energy, dft_code, dft_functional, publication, doi, year, ase_ids, user'
    value_str = "'{}'".format(values[1])
    for v in values[2:]:
        if isinstance(v, unicode):
            v = v.encode('ascii','ignore')
        if isinstance(v, str) or isinstance(v, dict):
            value_str += ", '{}'".format(v)
        elif v is None or v == '':
            value_str += ", {}".format('NULL')
        else:
            value_str += ", {}".format(v)
        
    return key_str, value_str


def get_key_value_list(key_list, values):
    total_key_list = ['chemical_composition', 'surface_composition', 'facet', 
                    'sites', 'reactants', 'products', 'reaction_energy', 
                    'activation_energy', 'dft_code', 'dft_functional', 
                    'publication', 'doi', 'year', 'ase_ids', 'user']
    if key_list == 'all':
        key_list = total_key_list
    else:
        for key in key_list:
            assert key in total_key_list

    value_list = [values[key] for key in key_list]
    return key_list, value_list

def get_value_strlist(value_list):
    value_strlist = []
    for v in value_list:
        if isinstance(v, unicode):
            v = v.encode('ascii','ignore')
        if isinstance(v, dict):
            v = json.dumps(v)
            value_strlist.append("'{}'".format(v))
        elif isinstance(v, str):
            value_strlist.append("'{}'".format(v))
        elif v is None or v == '':
            value_strlist.append("{}".format('NULL'))
        else:
            value_strlist.append("{}".format(v))

    return value_strlist



