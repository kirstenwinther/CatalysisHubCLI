import sqlite3
import json

import numpy as np

init_commands = \
                """CREATE TABLE catapp (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chemical_composition text,
                surface_composition text,
                atomic_numbers text,
                facet text,
                sites text,
                reactants text,
                products text,
                reaction_energy real,
                activation_energy real,
                DFT_code text,
                DFT_functional text,
                reference text,
                url text,
                year int,
                reactant_ids text,
                TS_id text, 
                product_ids text,
                reference_ids text
                )"""
#,
#    """CREATE TABLE species (
#    id INTEGER,
#    FOREIGN KEY (id) REFERENCES systems(id))
#    """
    #]
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
            #if self.create_indices:
                #for statement in index_statements:
                #    con.execute(statement)
            con.commit()

        self.initialized = True

    def write(self, values, data=None):
        con = self.connection or self._connect()
        self._initialize(con)
        cur = con.cursor()
        if self.id == None:
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
                  values['DFT_code'],
                  values['DFT_functional'],
                  values['reference'],
                  values['url'],
                  int(values['year']),
                  json.dumps(values['reactant_ids']),
                  json.dumps(values['TS_id']),
                  json.dumps(values['product_ids']),
                  json.dumps(values['reference_ids'])
        )
        
        #print values
        q = ', '.join('?' * len(values))
        cur.execute('INSERT INTO catapp VALUES ({})'.format(q),
                    values)
#        id = con.get_last_id()
#        cur.execute('INSERT INTO catapp VALUES ({})'.format(q),
#                    text_key_values)


#        if self.connection is None:
#            con.commit()
#            con.close()
        
        return id
   
    def get_last_id(self, cur):
        cur.execute('SELECT seq FROM sqlite_sequence WHERE name="catapp"')
        id = cur.fetchone()[0]
        return id



    def count_atoms()
        count = {}
        for symbol in self.symbols:
            count[symbol] = count.get(symbol, 0) + 1
        return count


    




        
