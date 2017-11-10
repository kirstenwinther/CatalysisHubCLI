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
                reference text,
                doi text,
                year int,
                ase_ids text
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
                  # str(values['gas_references']),
                  values['dft_code'],
                  values['dft_functional'],
                  values['reference'],
                  values['doi'],
                  int(values['year']),
                  json.dumps(values['ase_ids'])
                  )

        q = ', '.join('?' * len(values))
        cur.execute('INSERT INTO catapp VALUES ({})'.format(q),
                    values)

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
