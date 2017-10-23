import os
import csv
import sqlite3
from sys import argv
from ase_tools import *
from catappsqlite import CatappSQLite
import glob
from ase.io.trajectory import convert
import ase
from ase import db

catbase = os.environ['data'] + 'winther/'
ase_db = catbase + 'atoms.db'

user = argv[1]
data_home = os.environ['data'] + '/'

base = data_home + user
base_level = len(base.split("/"))

maxdepth = 0
i = 0
up = 0

for root, dirs, files in os.walk(base):
    level = len(root.split("/")) - base_level
    if level == 1:
        # assert 'publication.txt' in files
        try:
            pub_data = json.load(open(root + '/publication.txt', 'r'))

            keys = ['journal', 'volume', 'number', 'pages', 'year']
            reference = json.dumps(pub_data)
            try:
                doi = pub_data['doi']
            except:
                doi = None
            year = pub_data['year']
        except:
            year = 2017
            doi = None
            reference = '{}({})'.format(user, year)
        publication_keys = {}
        for key, value in pub_data.iteritems():
            if isinstance(value, list):
                value = json.dumps(value)
            else:
                try:
                    value = int(value)
                except:
                    pass
            publication_keys.update({'publication_' + key: value})
    if level == 2:
        DFT_code = root.split('/')[-1]

    if level == 3:
        DFT_functional = root.split('/')[-1]

    if level == 4:
        folder_name = root.split('/')[-1]

        reaction = get_reaction_from_folder(folder_name)  # reaction dict

        reaction_atoms, prefactors, states = get_reaction_atoms(reaction)

        gas_i = {}
        for key, mollist in reaction.iteritems():
            gas_i[key] = [i for i in range(len(mollist)) if states[key][i] == 'gas']



        traj_files = {'reactants': ['' for n in range(len(reaction['reactants']))],
                      'products': ['' for n in range(len(reaction['products']))]}

        chemical_compositions = {'reactants': ['' for n in range(len(reaction['reactants']))],
                      'products': ['' for n in range(len(reaction['products']))]}
        traj_gas = [f for f in files if f.endswith('.traj')]


        ase_ids = {}
        reference_ase_ids = {}
        #reference_ids = {}
        
        for f in traj_gas:
            ase_id = None
            found = False
            traj = '{}/{}'.format(root, f)
            check_traj(traj)
            chemical_composition = ''.join(sorted(get_chemical_formula(traj, mode='all')))
            chemical_composition_hill = get_chemical_formula(traj, mode='hill')

            ase_id = check_in_ase(traj, ase_db)
            if ase_id is None:  # write to ASE db
                energy = get_energies([traj])
                key_value_pairs = publication_keys.copy()
                key_value_pairs.update({"name": chemical_composition_hill,
                                        'epot': energy})
                ase_id = write_ase(traj, ase_db, **key_value_pairs)

            for key, mollist in reaction_atoms.iteritems():
                for i, molecule in enumerate(mollist):
                    if molecule == chemical_composition:
                        assert found is False  # Should only be found once?
                        found = True
                        traj_files[key][i] = traj
                        chemical_compositions[key][i] = chemical_composition_hill
                        ase_ids.update({clear_prefactor(reaction[key][i]): ase_id})
                        #energy_references.update(get_reference(traj))

            if found is False:
                print '{} file is not part of reaction, include as reference'.format(f)
                ase_ids.update({chemical_composition_hill + 'gas': ase_id})
                
    if level == 5:
        up = 0

    if level == 5 + up:
        metal = root.split('/')[-1]
        if user == 'roling':
            if metal == reaction[0].replace('*', ''):
                up += 1

        if len(metal.split('_')) > 1:
            metal = metal.split('_')[0]
            facet = metal.split('_')[1]
            up -= 1
            
    if level == 6 + up:
        facet = root.split('/')[-1]
        if not 'x' in facet:
            facet = '{}x{}x{}'.format(facet[0], facet[1], facet[2])

        sites = ''
    if level > 6 + up:
         sites = '_'.join(info for info in root.split('/')[6 + up + base_level:])

    traj_slabs = [f for f in files if f.endswith('.traj') and 'gas' not in f]

    if len(traj_slabs) > 0 and level >= 6 + up:
        assert len(traj_slabs) > 1, 'Need at least two files!'
        n_atoms = np.array([])
        empty_i = None
        ts_i = None
        chemical_composition_slabs = []
        for i, f in enumerate(traj_slabs):
            if 'empty' in f:
                empty_i = i
            if 'TS' in f:
                ts_i = i

            traj = '{}/{}'.format(root, f)
            check_traj(traj)
            chemical_composition_slabs = np.append(chemical_composition_slabs, get_chemical_formula(traj, mode='all'))
            n_atoms = np.append(n_atoms,get_number_of_atoms(traj))
            
        # Empty slab has least atoms
        if empty_i is None:
            empty_i = np.argmin(n_atoms)
        traj_empty = root + '/' + traj_slabs[empty_i]


        # Identify TS
        if ts_i is not None:
            traj_TS = root + '/' + traj_slabs[ts_i]
            TS_id = {get_chemical_formula(traj_TS): ase_id}
            
        elif ts_i is None and len(traj_slabs) > len(reaction) + 1:
            raise AssertionError, 'which one is the transition state???'
        else:
            TS_id = None
            activation_energy = None

        for i, f in enumerate(traj_slabs):
            ase_id = None
            found = False
            res = chemical_composition_slabs[i]
            res = ''.join(sorted(res.replace(chemical_composition_slabs[empty_i], '', 1)))
            traj = '{}/{}'.format(root, f)
            chemical_composition_metal = get_chemical_formula(traj)

            ase_id = check_in_ase(traj, ase_db)
            if ase_id is None:
                key_value_pairs = publication_keys.copy()
                key_value_pairs.update({'name': get_chemical_formula(traj_empty),
                                        'species': '',
                                        'epot': get_energies([traj_empty]),
                                        'site': sites,
                                        'facet': facet,
                                        'layers': get_n_layers(traj_empty)})
                ase_id = write_ase(traj, ase_db, **key_value_pairs)

            
            if i == ts_i:
                found = True
                ase_ids.update({'TS': ase_id})
                continue
            elif i == empty_i:
                found = True
                ase_ids.update({'star': ase_id})

            for key, mollist in reaction_atoms.iteritems():
                for n, molecule in enumerate(mollist):
                    if res == molecule and states[key][n] == 'star':
                        found = True
                        traj_files[key][n] = traj
                        chemical_compositions[key][n] = chemical_composition_metal
                        ase_ids.update({clear_prefactor(reaction[key][n]): ase_id})

            if found is False:
                print '{} file is not part of reaction, include as reference'.format(f)
                ase_ids.update({chemical_composition_metal: ase_id})

                

        ## Transition state has higher energy
        #if len(np.unique(chemical_compositions)) > len(chemical_compositions):
        #    for chemical_composition in chemical_compositions:

        surface_composition = get_surface_composition(traj_empty)
        bulk_composition = get_bulk_composition(traj_empty)
        chemical_composition = get_chemical_formula(traj_empty)
        
        reaction_energy = get_reaction_energy(traj_files, prefactors)
        
        print reaction, reaction_energy

        key_value_pairs_catapp = {'chemical_composition': chemical_composition,
                                  'surface_composition': surface_composition,
                                  'facet': facet,
                                  'sites': sites,
                                  'reactants': reaction['reactants'],
                                  'products': reaction['products'],
                                  'reaction_energy': reaction_energy,
                                  'activation_energy': activation_energy,
                                  #'gas_references:': energy_references,
                                  'DFT_code': DFT_code,
                                  'DFT_functional': DFT_functional,
                                  'reference': reference,
                                  'doi': doi,
                                  'year': year,
                                  'ase_ids': ase_ids,
                              }
        

        with CatappSQLite(catbase + 'catapp.db') as db:
            id = db.write(key_value_pairs_catapp)
            print 'Writing to catapp db row id = {}'.format(id)

