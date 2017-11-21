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

if os.environ['USER'] == 'winther':
    data_base = catbase + 'winther/databases/'
    ase_db = data_base + 'atoms.db'
    user = argv[1]

else:
    user = os.environ['USER']
    data_base = catbase + user + '/'
    ase_db = data_base + 'atoms.db'

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
    for omit_folder in omit_folders:  # user specified omit_folder
        if omit_folder in dirs:
            dirs.remove(omit_folder)
    level = len(root.split("/")) - user_base_level  

    if level == pub_level:  # Read publication info
        # assert 'publication.txt' in files
        publication_keys = {}
        try:
            pub_data = json.load(open(root + '/publication.txt', 'r'))
            if 'url' in pub_data.keys():
                del pub_data['url']
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
            
            pub_data = {'title': root.split('/')[-1],
                        'authors': user,
                        'journal': None,
                        'volume': None,
                        'number': None,
                        'pages': None,
                        'year': year,
                        'publisher': None,
                        'doi': None,
                        }
            reference = json.dumps(pub_data)

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

        print '-------------- REACTION:  {} --> {} -----------------'\
            .format('+'.join(reaction['reactants']), '+'.join(reaction['products']))

        reaction_atoms, prefactors, prefactors_TS, states = get_reaction_atoms(reaction)
        
                
        # Create empty dictionaries
        r_empty = ['' for n in range(len(reaction['reactants']))]
        p_empty = ['' for n in range(len(reaction['products']))]
        traj_files = {'reactants': r_empty[:],
                      'products': p_empty[:]}

        chemical_compositions = {'reactants': r_empty[:], 
                                 'products': p_empty[:]}

        ase_ids = {}
        reference_ase_ids = {}

        traj_gas = [f for f in files if f.endswith('.traj')]
        
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
                        ase_ids.update({clear_prefactor(clear_state(reaction[key][i])): \
                                            ase_id})

            if found is False:
                print '{} file is not part of reaction, include as reference'.format(f)
                ase_ids.update({chemical_composition_hill + 'gas': ase_id})
                
    if level == metal_level:
        up = 0

    if level == metal_level + up:
        metal = root.split('/')[-1]
        if metal_level == facet_level:
            if len(metal.split('_')) == 2:
                metal, facet = metal.split('_')
            else:
                facet = None
        print '--------------- METAL: {} ---------------'.format(metal) 
        #if user == 'roling':
        #    if metal == reaction['reactants'][0].replace('star', ''):
        #        up = 1
        #        continue

    if level == facet_level + up:
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

    if site_level is not None and site_level != 'None':
        if level == int(site_level) + up:
            if not facet_level == site_level:
                dirjoin = '_'.join(info for info in root.split('/'))
                sites = dirjoin[site_level + user_base_level:]
            
    if level == final_level + up:  # this is where the fun happens!
        traj_slabs = [f for f in files if f.endswith('.traj') \
                          and 'gas' not in f]
        if traj_slabs == []:
            continue
        assert len(traj_slabs) > 1, 'Need at least two files in {}!'.format(root)

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
        empty_atn = get_atomic_numbers(traj_empty)

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

        prefactor_scale = copy.deepcopy(prefactors)
        for key1, values in prefactor_scale.iteritems():
            prefactor_scale[key1] = [1 for v in values]                

        for i, f in enumerate(traj_slabs):
            ase_id = None
            found = False
            traj = '{}/{}'.format(root, f)

            res_atn = get_atomic_numbers(traj)
            for atn in empty_atn:
                res_atn.remove(atn)
            res_atn = sorted(res_atn)  # residual atomic numbers 

            chemical_composition_metal = get_chemical_formula(traj)
            
            if i == ts_i:
                found = True
                ase_ids.update({'TS': ase_id})
                continue
            elif i == empty_i:
                #found = True
                ase_ids.update({'empty': ase_id})

            supercell_factor = 1
            for key, mollist in reaction_atoms.iteritems():
                if found:
                    continue
                for n, molecule in enumerate(mollist):
                    if found:
                        continue
                    molecule_atn = get_numbers_from_formula(molecule)
                    for k in range(1, 5):
                        if found:
                            continue
                        mol = ''.join(sorted(molecule * k))
                        mol_atn = sorted(molecule_atn * k)

                        if res_atn == mol_atn and states[key][n] == 'star':
                            found = True
                        elif len(res_atn) >= len(empty_atn):
                            res_slab_atn = res_atn[:]
                            for atn in mol_atn:
                                if atn in res_slab_atn:
                                    res_slab_atn.remove(atn)

                            try:
                                supercell_factor = int(len(res_slab_atn) / len(empty_atn))
                                for atn in empty_atn * supercell_factor:
                                    if atn in res_slab_atn:
                                        res_slab_atn.remove(atn)
                            except:
                                continue

                            if len(res_slab_atn) == 0:
                                found =True

                        if found:
                            n_ads = k
                            ads_i = n
                            ads_key = key
                            traj_files[key][n] = traj
                            chemical_compositions[key][n] = chemical_composition_metal
                            ase_id = check_in_ase(traj, ase_db)
                            species = clear_state(clear_prefactor(reaction[key][n]))
                            if ase_id is None:
                                key_value_pairs = publication_keys.copy()
                                key_value_pairs.update({'name': get_chemical_formula(traj_empty),
                                                        'species': species,
                                                        'epot': get_energies([traj]),
                                                        'site': sites,
                                                        'facet': ase_facet,
                                                        'layers': get_n_layers(traj)})
                                ase_id = write_ase(traj, ase_db, **key_value_pairs)

                            ase_ids.update({species: ase_id})

      

            if n_ads > 1:
                for key1, values in prefactor_scale.iteritems():
                    for mol_i in range(len(values)):
                        if states[key1][mol_i] == 'gas':
                            prefactor_scale[key1][mol_i] = n_ads

            if supercell_factor > 1:
                for key2, values in prefactor_scale.iteritems():
                    for mol_i in range(len(values)):
                        if reaction[key2][mol_i] =='star':
                            prefactor_scale[key2][mol_i] *= supercell_factor + 1

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


        reaction_energy = None
        activation_energy = None        
        # try: 
        reaction_energy, activation_energy = \
            get_reaction_energy(traj_files, prefactors_final, prefactors_TS)    

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

        reaction_info = {'reactants': {}, 
                         'products': {}}
        products = {}
        for key in ['reactants', 'products']:
            for i, r in enumerate(reaction[key]):
                r = clear_state(r)
                r = clear_prefactor(r)
                reaction_info[key].update({r: [states[key][i], prefactors_final[key][i]]})

#       print chemical_composition, reaction_energy, activation_energy
        key_value_pairs_catapp = {'chemical_composition': chemical_composition,
                                  'surface_composition': surface_composition,
                                  'facet': facet,
                                  'sites': sites,
                                  'reactants': reaction_info['reactants'],
                                  'products': reaction_info['products'], 
                                  'reaction_energy': reaction_energy,
                                  'activation_energy': activation_energy,
                                  'dft_code': DFT_code,
                                  'dft_functional': DFT_functional,
                                  'publication': reference,
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


