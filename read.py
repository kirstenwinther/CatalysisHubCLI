import os
import csv
import copy
import sqlite3
from sys import argv
from ase_tools import *
from catappsqlite import CatappSQLite
import glob
from ase.io.trajectory import convert
import ase
from ase import db

try:  #sherlock 1 or 2
    sherlock = os.environ['SHERLOCK']
    if sherlock == '1':
        catbase = '/home/winther/data_catapp/'
    elif sherlock == '2':
        catbase = '/home/users/winther/data_catapp/'
except:  # SUNCAT
    catbase = '/nfs/slac/g/suncatfs/data_catapp/'

debug = False
strict = False

data_base = catbase + 'winther/databases/'
ase_db = data_base + 'atoms.db'

user = argv[1]
user_base = catbase + user
user_base_level = len(user_base.split("/"))

i = 0
up = 0

omit_folders = []

user_file = '{}winther/user_specific/{}.txt'.format(catbase, user) 
if os.path.isfile(user_file):
    user_spec = json.load(open(user_file, 'r'))
    locals().update(user_spec)
else:
    pub_level = 1
    DFT_level = 2
    XC_level = 3
    reaction_level = 4
    metal_level = 5
    facet_level = 6
    site_level = None
    final_level = 6


if site_level is None or site_level == "None":
    sites = ''

for root, dirs, files in os.walk(user_base):
    for omit_folder in omit_folders:
        if omit_folder in dirs:
            dirs.remove(omit_folder)
    level = len(root.split("/")) - user_base_level
    if level == pub_level:
        # assert 'publication.txt' in files
        publication_keys = {}
        try:
            pub_data = json.load(open(root + '/publication.txt', 'r'))

            keys = ['journal', 'volume', 'number', 'pages', 'year']
            reference = json.dumps(pub_data)
            try:
                doi = pub_data['doi']
            except:
                print 'ERROR: No doi'
                doi = None
            year = pub_data['year']
            for key, value in pub_data.iteritems():
                if isinstance(value, list):
                    value = json.dumps(value)
                else:
                    try:
                        value = int(value)
                    except:
                        pass
                publication_keys.update({'publication_' + key: value})

        except:
            print 'ERROR: insufficient publication info'
            year = 2017
            doi = None
            reference = '{}({})'.format(user, year)

    if level == DFT_level:
        DFT_code = root.split('/')[-1]

    if level == XC_level:
        DFT_functional = root.split('/')[-1]

    if level == reaction_level:        
        folder_name = root.split('/')[-1]
        try:
            reaction = get_reaction_from_folder(folder_name)  # reaction dict
        except:
            print 'ERROR: omitting directory {}'.format(root)
            dirs = []
            continue

        print '-------------- REACTION:  {} --> {} -----------------'.format('+'.join(reaction['reactants']), '+'.join(reaction['products']))

        reaction_atoms, prefactors, states = get_reaction_atoms(reaction)

        prefactors_TS = copy.deepcopy(prefactors)
        
        # empty slab balance
        
        n_star = {'reactants': 0,
                  'products': 0}

        for key, statelist in states.iteritems():
            for s in statelist:
                if s == 'star':
                    n_star[key] += 1

        n_r = n_star['reactants']
        n_p = n_star['products']

        diff = n_p - n_r

        if diff > 0:
            n_r += diff
            reaction['reactants'].append('star')
            prefactors['reactants'].append(diff)
            prefactors_TS['reactants'].append(1)
            states['reactants'].append('star')
            reaction_atoms['reactants'].append('')

        elif diff < 0:
            n_p += -diff
            reaction['products'].append('star')
            prefactors['products'].append(-diff)
            states['products'].append('star')
            reaction_atoms['products'].append('')

        if n_r > 1:
            if len([s for s in states['reactants'] if s =='star']) > 1:
                prefactors_TS['reactants'][-1] = 0
        
        r_empty = ['' for n in range(len(reaction['reactants']))]
        p_empty = ['' for n in range(len(reaction['products']))]
        traj_files = {'reactants': r_empty[:],
                      'products': p_empty[:]}

        chemical_compositions = {'reactants': r_empty[:], 
                                 'products': p_empty[:]}
        traj_gas = [f for f in files if f.endswith('.traj')]

        ase_ids = {}
        reference_ase_ids = {}
        #reference_ids = {}
        
        for f in traj_gas:
            ase_id = None
            found = False
            traj = '{}/{}'.format(root, f)
            if not check_traj(traj, strict, False):
                continue
            chemical_composition = \
                ''.join(sorted(get_chemical_formula(traj, mode='all')))
            chemical_composition_hill = get_chemical_formula(traj, mode='hill')

            ase_id = check_in_ase(traj, ase_db)
            if ase_id is None:  # write to ASE db
                energy = get_energies([traj])
                key_value_pairs = publication_keys.copy()
                key_value_pairs.update({"name": chemical_composition_hill,
                                        'epot': energy})
                ase_id = write_ase(traj, ase_db,**key_value_pairs)

            for key, mollist in reaction_atoms.iteritems():
                for i, molecule in enumerate(mollist):
                    if molecule == chemical_composition \
                            and states[key][i] == 'gas':
                        # Should only be found once?
                        assert found is False, root + ' ' + chemical_composition
                        found = True
                        traj_files[key][i] = traj
                        chemical_compositions[key][i] = \
                            chemical_composition_hill
                        ase_ids.update({clear_prefactor(reaction[key][i]): \
                                            ase_id})

            if found is False:
                print '{} file is not part of reaction, include as reference'.format(f)
                ase_ids.update({chemical_composition_hill + 'gas': ase_id})
                
    #if level == metal_level:
    #    up = 0


    if level == metal_level:
        metal = root.split('/')[-1]
        if metal_level == facet_level:
            if len(metal.split('_')) == 2:
                metal, facet = metal.split('_')
            else:
                facet = None
        print '--------------- METAL: {} ---------------'.format(metal) 
        #if user == 'roling':
        #    if metal == reaction[0].replace('*', ''):
        #        up += 1

    if level == facet_level: # + up:
        folder_name = root.split('/')[-1]
        if not metal_level == facet_level:
            if not facet_level == site_level:
                facet = folder_name
            else:
                split = folder_name.split('_')
                if len(split) == 1:
                    split = split[0].split('-')
                facet, site = split
        ase_facet = facet
        if not 'x' in facet and not '-' in facet:
            facetstr = 'x'.join('{}' for f in facet)
            ase_facet = facetstr.format(*facet)
        print '--------------- FACET: {} ---------------'.format(facet) 

    if level == site_level:
        if not facet_level == site_level:
            dirjoin = '_'.join(info for info in root.split('/'))
            sites = dirjoin[site_level + user_base_level:]

    if level == final_level:
        reaction_energy = None
        activation_energy = None
        
        traj_slabs = [f for f in files if f.endswith('.traj') \
                          and 'gas' not in f]
        if traj_slabs == []:
            continue
        assert len(traj_slabs) > 1, 'Need at least two files!'
        n_atoms = np.array([])
        empty_i = None
        ts_i = None
        chemical_composition_slabs = []
        breakloop = False
        for i, f in enumerate(traj_slabs):
            if 'empty' in f:
                empty_i = i
            if 'TS' in f:
                ts_i = i
            traj = '{}/{}'.format(root, f)
            if not check_traj(traj, strict, False):
                breakloop = True
                break
            chemical_composition_slabs = \
                np.append(chemical_composition_slabs, 
                          get_chemical_formula(traj, mode='all'))
            n_atoms = np.append(n_atoms,get_number_of_atoms(traj))

        if breakloop:
            continue

        # Empty slab has least atoms
        if empty_i is None:
            empty_i = np.argmin(n_atoms)
        traj_empty = root + '/' + traj_slabs[empty_i]

        # Identify TS
        if ts_i is not None:
            traj_TS = root + '/' + traj_slabs[ts_i]
            traj_files.update({'TS': [traj_TS]})
            prefactors.update({'TS': [1]})
            TS_id = {get_chemical_formula(traj_TS): ase_id}
            
        #elif ts_i is None and len(traj_slabs) > len(reaction) + 1:
            #raise AssertionError, 'which one is the transition state???'
        else:
            TS_id = None
            activation_energy = None

        #stop
        prefactor_scale = copy.deepcopy(prefactors)
        for key1, values in prefactor_scale.iteritems():
                    prefactor_scale[key1] = [1 for v in values]                
        for i, f in enumerate(traj_slabs):
            ase_id = None
            found = False
            res = chemical_composition_slabs[i]
            for char in chemical_composition_slabs[empty_i]:
                res = res.replace(char, '', 1)

            res = ''.join(sorted(res))

            traj = '{}/{}'.format(root, f)
            chemical_composition_metal = get_chemical_formula(traj)

            ase_id = check_in_ase(traj, ase_db)

            if ase_id is None:

                key_value_pairs = publication_keys.copy()
                key_value_pairs.update({'name': get_chemical_formula(traj_empty),
                                        'species': res,
                                        'epot': get_energies([traj]),
                                        'site': sites,
                                        'facet': ase_facet,
                                        'layers': get_n_layers(traj)})
                ase_id = write_ase(traj, ase_db, **key_value_pairs)

            
            if i == ts_i:
                found = True
                ase_ids.update({'TS': ase_id})
                continue
            elif i == empty_i:
                #found = True
                ase_ids.update({'star': ase_id})

            for key, mollist in reaction_atoms.iteritems():
                if found:
                    continue
                for n, molecule in enumerate(mollist):
                    if found:
                        continue
                    for k in range(1, 5):
                        if found:
                            continue
                        molecule = ''.join(sorted(molecule * k))
                        if res == molecule and states[key][n] == 'star':
                            found = True
                            n_ads = k
                            ads_i = n
                            ads_key = key
                            traj_files[key][n] = traj
                            chemical_compositions[key][n] = chemical_composition_metal
                            ase_ids.update({clear_prefactor(reaction[key][n]): ase_id})
      

            if n_ads > 1:
                for key1, values in prefactor_scale.iteritems():
                    for mol_i in range(len(values)):
                        if states[key1][mol_i] == 'gas':
                            prefactor_scale[key1][mol_i] = n_ads


            if found is False:
                print '{} file is not part of reaction, include as reference'.format(f)
                ase_ids.update({chemical_composition_metal: ase_id})        

        ## Transition state has higher energy
        #if len(np.unique(chemical_compositions)) > len(chemical_compositions):
        #    for chemical_composition in chemical_compositions:

        surface_composition = get_surface_composition(traj_empty)
        bulk_composition = get_bulk_composition(traj_empty)
        chemical_composition = get_chemical_formula(traj_empty)
        
        prefactors_final = copy.deepcopy(prefactors)
        for key in prefactors:
            for i, v in enumerate(prefactors[key]):
                prefactors_final[key][i] = prefactors[key][i] * prefactor_scale[key][i] 

        # try: 

        reaction_energy, activation_energy = \
            get_reaction_energy(traj_files, prefactors_final, prefactors_TS)    

        print reaction_energy
        #except:
        #    print 'ERROR: reaction energy failed: {}'.format(root)
        #    continue
        
        
        expr = -10 < reaction_energy < 10
        debug_assert(expr,
                     'reaction energy is wrong: {} eV: {}'.format(reaction_energy, root),
                     debug)
        expr = activation_energy is None or -10 < activation_energy < 10
        debug_assert(expr,
                     'activation energy is wrong: {} eV: {}'.format(activation_energy, root),
                     debug)

        
#       print chemical_composition, reaction_energy, activation_energy
        key_value_pairs_catapp = {'chemical_composition': chemical_composition,
                                  'surface_composition': surface_composition,
                                  'facet': facet,
                                  'sites': sites,
                                  'reactants': reaction['reactants'],
                                  'products': reaction['products'],
                                  'reaction_energy': reaction_energy,
                                  'activation_energy': activation_energy,
                                  'DFT_code': DFT_code,
                                  'DFT_functional': DFT_functional,
                                  'reference': reference,
                                  'doi': doi,
                                  'year': year,
                                  'ase_ids': ase_ids,
                                  }
        
        
        with CatappSQLite(data_base + 'catapp.db') as db:
            id = db.check(reaction_energy)
            if id is not None:
                print 'Allready in catapp db with row id = {}'.format(id)
            else:
                id = db.write(key_value_pairs_catapp)
                print 'Written to catapp db row id = {}'.format(id)


