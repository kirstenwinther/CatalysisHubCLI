import sqlite3
import json

import numpy as np

init_command = \
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
                publication text,
                doi text,
                year int,
                ase_ids text,
                user text
                )"""


class CatappSQLite:
    def __init__(self, filename):
        self.filename = filename
        self.initialized = False
        self.default = 'NULL'  # used for autoincrement id
        self.connection = None
        self.id = None

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

        self._metadata = {}

        cur = con.execute(
            'SELECT COUNT(*) FROM sqlite_master WHERE name="catapp"')
        if cur.fetchone()[0] == 0:
            con.execute(init_command)
            self.id = 1
            con.commit()

        self.initialized = True

    def read(self, id):
        con = self.connection or self._connect()
        self._initialize(con)
        cur = con.cursor()
        cur.execute('SELECT * FROM \n catapp \n WHERE \n catapp.id={}'.format(id))
        row = cur.fetchall()
        return row
        
    def write(self, values, data=None):
        con = self.connection or self._connect()
        self._initialize(con)
        cur = con.cursor()
        if self.id is None:
            id = self.get_last_id(cur) + 1
        else:
            id = self.id
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
                  values['publication'],
                  values['doi'],
                  int(values['year']),
                  json.dumps(values['ase_ids']),
                  values['user']
                  )

        q = ', '.join('?' * len(values))
        cur.execute('INSERT INTO catapp VALUES ({})'.format(q),
                    values)

        if self.connection is None:
            con.commit()
            con.close()

        return id

    def update(self, id, values, key_names='all'):
        con = self.connection or self._connect()
        self._initialize(con)
        cur = con.cursor()

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
    
    def check(self, reaction_energy):
        con = self.connection or self._connect()
        self._initialize(con)
        cur = con.cursor()
        statement = 'SELECT catapp.id FROM \n catapp \n WHERE \n catapp.reaction_energy=?'
        argument = [reaction_energy]
        
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
        if isinstance(v, str):
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
        if isinstance(v, str):
            value_strlist.append("'{}'".format(v))
        elif v is None or v == '':
            value_strlist.append("{}".format('NULL'))
        else:
            value_strlist.append("{}".format(v))

    return value_strlist



