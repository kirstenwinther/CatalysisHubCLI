import os
import csv
import sqlite3
from sys import argv
from ase_tools import *
from catappsqlite import CatappSqlite
import glob
from ase.io.trajectory import convert
import ase
from ase import db

catbase = './'
ase_db = None
user = argv[1]
data_home = os.environ['data'] + 'winther/'
base = data_home + user
base_level =  len(base.split("/"))
publications = os.listdir(base)

maxdepth = 0
i = 0
up = 0


for root, dirs, files in os.walk(base):
    cur_level = len(root.split("/")) - base_level
    if cur_level == 1 + up:
        #assert 'publication.txt' in files
        try:
            pub_data = json.load(open(root + '/publication.txt', 'r'))

            keys = ['journal', 'volume', 'number', 'pages', 'year']
            ref_keys = [pub_data[key] for key in keys]
            reference = json.dumps(pub_data)
            #reference = '{} {}, {}, {} ({})'.format(*ref_keys)
            try:
                url = pub_data_url
            except:
                url = ''
            year = pub_data['year']
            print reference
        except:
            print 
            year = 2017
            url = ''
            reference = '{}({})'.format(user, year)
            
    if cur_level == 2 + up:
        DFT_code = root.split('/')[-1]
        
    if cur_level == 3 + up:
        DFT_functional = root.split('/')[-1]
    if cur_level == 4 + up:
        reaction = root.split('/')[-1]
        if '__' in reaction:  # Complicated reaction
            reactants = reaction.split('__')[0].split('_')
            products = reaction.split('__')[1].split('_')
            reaction = reactants.append(products)
            print reactants, products, reaction
        else:  # Standard format
            reaction = reaction.split('_')
            print reaction
            AB, A, B = reaction
            reactants = [AB]
            products = [A, B]

        adsorbed = ['star' in s and s != 'star' for s in reaction]
        gas = ['gas' in s and s != 'gas' for s in reaction]

        reactant_atoms = [clear_state(s) for s in reactants]
        product_atoms = [clear_state(s) for s in products]
        reactant_states = [get_state(s) for s in reactants]
        product_states = [get_state(s) for s in products]
        #reaction_atoms = [s.replace('star', '').replace('gas', '') for s in reaction]
        #reaction_states = [get_state(s) for s in reaction]
        #reaction = [s.replace('star', '*').replace('gas', '(g)') for s in reaction]
        #print reaction

        mol_ref = [f for f in files if f.endswith('.traj')]
        chemical_compositions_mol = np.array([])
        gasreactant_i = [i for i in range(len(reactants)) if reactant_states[i] == 'gas']
        gasproduct_i = [i for i in range(len(products)) if product_states[i] == 'gas']
                                              
        #energy_references = {}
        reference_ids = {}
        for f in mol_ref:
            traj = '{}/{}'.format(root, mol_ref[0])
            check_traj(traj)
            ase_id = check_in_ase(traj)
            chemical_composition = get_chemical_formula(traj, mode='hill')
            chemical_compositions_mol = \
            np.append(chemical_compositions_mol, 
                      chemical_composition)
            reference_ids.update({chemical_composition: ase_id})
            #energy_references.update(get_reference(traj))
            
             #_id = db_ase.write(read(traj))
            #ase_id_ref.append(_id)

        traj_reactants_mol = []
        traj_products_mol = []
        for n in gasreactant_i:
            m = [m for m in range(len(mol_ref)) if \
                 reactant_atoms[n] == chemical_compositions_mol[m]]
            print m
            assert len(m) == 1
            m = m[0]
            mol_ref_m = '{}/{}'.format(root, mol_ref[m])
            traj_reactants_mol.append(mol_ref_m)


        for n in gasproduct_i:
            print product_atoms[n]
            m = [m for m in range(len(mol_ref)) if \
                 product_atoms[n] == chemical_compositions_mol[m]]
            print m
            assert len(m) == 1

            m = m[0]
            mol_ref_m = '{}/{}'.format(root, mol_ref[m])
            traj_products_mol.append(mol_ref_m)

    if cur_level == 5:
        up = 0
    if cur_level == 5 + up:
        metal = root.split('/')[-1]
        if metal == reaction[0].replace('*', ''): # roling
            up += 1
        if len(metal.split('_')) > 1:
            metal = metal.split('_')[0]
            facet = metal.split('_')[1]
            up -= 1
            
    if cur_level == 6 + up:
        facet = root.split('/')[-1]
        
    if cur_level > 6 + up:
        info = '_'.join(info for info in root.split('/')[6 + up + base_level:])

    no_of_atoms = 0
    traj_files = [f for f in files if f.endswith('.traj') and 'gas' not in f]
    reactant_ids = {}
    product_ids = {}
    if len(traj_files) > 0 and cur_level >= 6 + up:
        
        assert len(traj_files) > 1, 'Need at least two files!'
        energies = np.array([])
        chemical_compositions = np.array([])
        n_atoms = np.array([])
        empty_i = None
        ts_i = None
        for i, f in enumerate(traj_files):
            if 'empty' in f:
                empty_i = i
            if 'TS' in f:
                ts_i = i
            traj = '{}/{}'.format(root, f)
            check_traj(traj)
            chemical_compositions = np.append(chemical_compositions, get_chemical_formula(traj, mode='all'))
            energies = np.append(energies, get_energy([traj]))
            n_atoms = np.append(n_atoms,get_number_of_atoms(traj))
            
        # Empty slab has least atoms
        if empty_i is None:
            empty_i = np.argmin(n_atoms)
        traj_empty = root + '/' + traj_files[empty_i]
        ase_id = check_in_ase(traj_empty)
        reference_ids.update({get_chemical_formula(traj_empty): ase_id})

        if ts_i is not None:
            traj_TS = root + '/' + traj_files[ts_i]
            ase_id = check_in_ase(traj_TS)
            TS_id = {get_chemical_formula(traj_TS): ase_id}
            
        elif ts_i is None and len(traj_files) > len(reaction) + 1:
            raise AssertionError, 'which one is the transition state???'
        else:
            TS_id = None
            activation_energy = None
            
        ## Transition state has higher energy
        #if len(np.unique(chemical_compositions)) > len(chemical_compositions):
        #    for chemical_composition in chemical_compositions:
        traj_reactants = traj_reactants_mol[:]
        traj_products = traj_products_mol[:]
        
        for n, res in enumerate(chemical_compositions):
            res = res.replace(chemical_compositions[empty_i], '', 1)
            traj = '{}/{}'.format(root, traj_files[n])
            for i, atoms in enumerate(reactant_atoms):
                if res == atoms and reactant_states[i] == 'star':
                    traj_reactants.append(traj)
                    ase_id = check_in_ase(traj)
                    reactant_ids.update({get_chemical_formula(traj): ase_id})
            for i, atoms in enumerate(product_atoms):
                if res == atoms and product_states[i] == 'star':
                    traj_products.append(traj)
                    ase_id = check_in_ase(traj)
                    product_ids.update({get_chemical_formula(traj): ase_id})

        surface_composition = get_surface_composition(traj_empty)
        bulk_composition = get_bulk_composition(traj_empty)
        chemical_composition = get_chemical_formula(traj_empty)

        print traj_reactants, traj_reactants_mol
        
        reaction_energy = get_reaction_energy(traj_reactants, traj_products)
        
        print reaction_energy
        sites = None

        key_value_pairs = {'chemical_composition': chemical_composition,
                           'surface_composition': surface_composition,
                           'facet': facet,
                           'sites': sites,
                           'reactants': reactants,
                           'products': products,
                           'reaction_energy': reaction_energy,
                           'activation_energy': activation_energy,
                           'DFT_code': DFT_code,
                           'DFT_functional': DFT_functional,
                           'reference':reference,
                           'url': url,
                           'year': year,
                           'reactant_ids': reactant_ids,
                           'TS_id': TS_id,
                           'product_ids': product_ids,
                           'reference_ids': reference_ids
        }
        

        with CatappSqlite(catbase + 'catapp.db') as db:
            id = db.write(key_value_pairs)
            print id


        


        

        #print chemical_composition, facet, AB, A, B, reaction_energy, DFT_code, DFT_functional, url, year
        #if AB.strip('*') in chemical_composition:
        #    print AB, chemical_composition
        #    traj_AB = traj
#       #     i += 1
        #else:
        #    traj_empty = traj

      #  i +=1
      #  print i, chemical_composition

        # assert metal in chemical_composition
        # for mol in reaction if mol != '':

        #, facet, AB, A, B, reaction_energy, energy_references, DFT_code, DFT_functional, reference, url, year


        #DFT_functional, DFT_code, reaction, metal, facet, info, chemical_composition

        
