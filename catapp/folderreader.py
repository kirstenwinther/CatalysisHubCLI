import os
import csv
import copy
import sqlite3
from sys import argv
from ase_tools import *
from catappsqlite import CatappSQLite
from tools import get_bases
import glob
from ase.io.trajectory import convert
import ase
from ase import db


class FolderReader:
    def __init__(self, user=None, debug=False, strict=False, verbose=False,
                 update=True):
        self.user = user
        self.debug = debug
        self.strict = strict
        self.verbose = verbose
        self.update = update
        
        self.catbase, self.data_base, self.user, self.user_base \
            = get_bases(user=user)
        self.user_base_level = len(self.user_base.split("/"))
        
        omit_folders = []

        self.pub_level = 1
        self.DFT_level = 2
        self.XC_level = 3
        self.reaction_level = 4
        self.metal_level = 5
        self.facet_level = 6
        self.site_level = None
        self.final_level = 6
        user_file = '{}winther/user_specific/{}.txt'.format(self.catbase, 
                                                            self.user) 
        self.omit_folders = []
        if os.path.isfile(user_file):
            user_spec = json.load(open(user_file, 'r'))
            self.__dict__.update(user_spec)

    
    def read(self, skip=None):
        if skip is not None:
            self.omit_folders.append(skip)
        up = 0
        for root, dirs, files in os.walk(self.user_base):
            for omit_folder in self.omit_folders:  # user specified omit_folder
                if omit_folder in dirs:
                    dirs.remove(omit_folder)
            level = len(root.split("/")) - self.user_base_level  

            if level == self.pub_level:
                self.read_pub(root)

            if level == self.DFT_level:
                self.DFT_code = self.read_name_from_folder(root)

            if level == self.XC_level:
                self.DFT_functional = self.read_name_from_folder(root)

            if level == self.reaction_level:
                self.read_reaction(root, files)

            if level == self.metal_level:
                up = 0

            if level == self.metal_level + up:
                self.read_metal(root)
                if self.user == 'roling':
                    if self.metal == self.reaction['reactants'][0].replace('star', ''):
                        up = 1
                        continue

            if level == self.facet_level + up \
               and not level == self.metal_level + up:
                self.read_facet(root)

            if self.site_level is not None and self.site_level != 'None':
                if level == int(self.site_level) + up\
                   and not level == self.facet_level + up:
                    self.read_site(root)
                    print self.sites
                    try:
                        self.sites = int(self.site)
                    except:
                        pass

            else:
                self.sites = ''

            
            if level == self.final_level + up:
                self.read_final(root, files)
                if self.key_value_pairs_catapp is not None:
                    yield self.key_value_pairs_catapp
            
    def write(self, skip=None):
        for key_values in self.read(skip=skip):
            with CatappSQLite(self.catapp_db) as db:
                id = db.check(key_values['reaction_energy'])
                #print 'Allready in catapp db with row id = {}'.format(id)
                if id is None:
                    id = db.write(key_values)
                    print 'Written to catapp db row id = {}'.format(id)
                else:
                    print 'Allready in catapp db with row id = {}'.format(id)
                #elif self.update:
                #    db.update(id, key_value_pairs_catapp)
            
    def update(key_names='all'):
        with CatappSQLite(self.catapp_db) as db:
            for key_values in self.read():
                id = db.check(key_values['reaction_energy'])
                #print 'Allready in catapp db with row id = {}'.format(id)
                if id is not None:
                    db.update(id, key_value_pairs_catapp, key_names=key_names)
    

    def read_name_from_folder(self, root):
        folder_name = root.split('/')[-1]
        return folder_name
        
    def read_pub(self, root):
        pub_folder = root.split('/')[-1]
        self.ase_db = '{}atoms_{}.db'.format(self.data_base, pub_folder)
        self.catapp_db = '{}catapp_{}.db'.format(self.data_base, pub_folder)
        # assert 'publication.txt' in files
        publication_keys = {}
        try:
            pub_data = json.load(open(root + '/publication.txt', 'r'))
            if 'url' in pub_data.keys():
                del pub_data['url']
            self.reference = json.dumps(pub_data)
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
                        'authors': self.user,
                        'journal': None,
                        'volume': None,
                        'number': None,
                        'pages': None,
                        'year': year,
                        'publisher': None,
                        'doi': None,
                        }
            self.reference = pub_data
        self.doi = doi
        self.year = year
        self.publication_keys = publication_keys


    def read_reaction(self, root, files):
        folder_name = root.split('/')[-1]
        try:
            self.reaction = get_reaction_from_folder(folder_name)  # reaction dict
        except:
            print 'ERROR: omitting directory {}'.format(root)
            dirs = []
            return

        print '-------------- REACTION:  {} --> {} -----------------'\
            .format('+'.join(self.reaction['reactants']), 
                    '+'.join(self.reaction['products']))

        self.reaction_atoms, self.prefactors, self.prefactors_TS, \
            self.states = get_reaction_atoms(self.reaction)
                
        # Create empty dictionaries
        r_empty = ['' for n in range(len(self.reaction['reactants']))]
        p_empty = ['' for n in range(len(self.reaction['products']))]
        self.traj_files = {'reactants': r_empty[:],
                           'products': p_empty[:]}


        self.ase_ids = {}

        traj_gas = [f for f in files if f.endswith('.traj')]
        
        key_value_pairs = copy.deepcopy(self.publication_keys)
        for f in traj_gas:
            ase_id = None
            found = False
            traj = '{}/{}'.format(root, f)
            if not check_traj(traj, self.strict, False):
                return
            chemical_composition = \
                ''.join(sorted(get_chemical_formula(traj, mode='all')))
            chemical_composition_hill = get_chemical_formula(traj, mode='hill')
            
            energy = get_energies([traj])
            key_value_pairs.update({"name": chemical_composition_hill,
                                    'state': 'gas',
                                    'epot': energy})

            id, ase_id = check_in_ase(traj, self.ase_db)

            for key, mollist in self.reaction_atoms.iteritems():
                for i, molecule in enumerate(mollist):
                    if molecule == chemical_composition \
                            and self.states[key][i] == 'gas':
                        # Should only be found once?
                        assert found is False, \
                            root + ' ' + chemical_composition
                        found = True
                        self.traj_files[key][i] = traj

                        species = clear_prefactor(self.reaction[key][i])
                        key_value_pairs.update({'species': 
                                                clear_state(species)})
                        if ase_id is None:
                            ase_id = write_ase(traj, self.ase_db, 
                                               **key_value_pairs)
                        elif self.update:
                            update_ase(self.ase_db, id, **key_value_pairs)
                        self.ase_ids.update({species: ase_id})

            if found is False:
                print '{} file is not part of reaction, include as reference'\
                    .format(f)
                self.ase_ids.update({chemical_composition_hill + 'gas': ase_id})

    def read_metal(self, root):
        self.metal = root.split('/')[-1]
        if self.metal_level == self.facet_level:
            if len(self.metal.split('_')) == 2:
                self.metal, self.facet = metal.split('_')
            else:
                self.facet = None
                self.ase_facet = None
        print '--------------- METAL: {} ---------------'.format(self.metal) 

    def read_facet(self, root):
        folder_name = root.split('/')[-1]
        
        if not self.facet_level == self.site_level:
            self.facet = folder_name
        else:
            split = folder_name.split('_')
            if len(split) == 1:
                split = split[0].split('-')
            self.facet, self.sites = split
        self.ase_facet = self.facet
        if not 'x' in self.facet and not '-' in self.facet:
            facetstr = 'x'.join('{}' for f in self.facet)
            self.ase_facet = facetstr.format(*self.facet)
        print '--------------- FACET: {} ---------------'.format(self.facet) 

    def read_site(self, root):
        dirjoin = '_'.join(info for info in root.split('/')\
                           [self.site_level + self.user_base_level - 1:])
        self.sites = dirjoin


    def read_final(self, root, files):
        key_value_pairs_catapp = None
        traj_slabs = [f for f in files if f.endswith('.traj') \
                          and 'gas' not in f]
        #if traj_slabs == []:
        #    return
        assert len(traj_slabs) > 1, \
            'Need at least two files in {}!'.format(root)

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
            if not check_traj(traj, self.strict, False):
                breakloop = True
                break
            chemical_composition_slabs = \
                np.append(chemical_composition_slabs, 
                          get_chemical_formula(traj, mode='all'))
            n_atoms = np.append(n_atoms,get_number_of_atoms(traj))

        if breakloop:
            return

        # Empty slab has least atoms
        if empty_i is None:
            empty_i = np.argmin(n_atoms)
        traj_empty = root + '/' + traj_slabs[empty_i]

        empty_atn = get_atomic_numbers(traj_empty)

        # Identify TS
        if ts_i is not None:
            traj_TS = root + '/' + traj_slabs[ts_i]
            self.traj_files.update({'TS': [traj_TS]})
            self.prefactors.update({'TS': [1]})
            TS_id = {get_chemical_formula(traj_TS): ase_id}
            
        #elif ts_i is None and len(traj_slabs) > len(reaction) + 1:
            #raise AssertionError, 'which one is the transition state???'
        else:
            TS_id = None
            activation_energy = None

        prefactor_scale = copy.deepcopy(self.prefactors)
        for key1, values in prefactor_scale.iteritems():
            prefactor_scale[key1] = [1 for v in values]                
            
        #prefactor_scale_ads = copy.deepcopy(prefactor_scale)

        key_value_pairs = copy.deepcopy(self.publication_keys)
        key_value_pairs.update({'name': get_chemical_formula(traj_empty),
                                'site': self.sites,
                                'facet': self.ase_facet,
                                'layers': get_n_layers(traj_empty),
                                'state': 'star'})

        for i, f in enumerate(traj_slabs):
            ase_id = None
            id, ase_id = check_in_ase(traj, self.ase_db)
            found = False
            traj = '{}/{}'.format(root, f)

            key_value_pairs.update({'epot': get_energies([traj])})
            chemical_composition_metal = get_chemical_formula(traj)

            if i == ts_i:                
                found = True
                key_value_pairs.update({'species': 'TS'})
                if ase_id is None:
                    ase_id = write_ase(traj, self.ase_db, **key_value_pairs)
                elif self.update:
                    update_ase(self.ase_db, id,  **key_value_pairs)
                self.ase_ids.update({'TSstar': ase_id})
                continue

            elif i == empty_i:
                found = True
                for key, mollist in self.reaction_atoms.iteritems():
                    if '' in mollist:
                        n = mollist.index('')
                        self.traj_files[key][n] = traj
                key_value_pairs.update({'species': ''})
                if ase_id is None:
                    ase_id = write_ase(traj, self.ase_db, **key_value_pairs)
                elif self.update:
                    update_ase(self.ase_db, id, **key_value_pairs)                    
                self.ase_ids.update({'star': ase_id})
                continue
            
            res_atn = get_atomic_numbers(traj)

            for atn in empty_atn:

                res_atn.remove(atn)
            res_atn = sorted(res_atn)  # residual atomic numbers 

            supercell_factor = 1
            n_ads = 1
            for key, mollist in self.reaction_atoms.iteritems():
                if found:
                    continue
                for n, molecule in enumerate(mollist):
                    if found:
                        continue
                    molecule_atn = get_numbers_from_formula(molecule)
                    for k in range(1, 5):
                        if found:
                            continue
                        mol_atn = sorted(molecule_atn * k)
                        if res_atn == mol_atn and self.states[key][n] == 'star':
                            found = True
                        elif len(res_atn) >= len(empty_atn):
                            res_slab_atn = res_atn[:]
                            for atn in mol_atn:
                                if atn in res_slab_atn:
                                    res_slab_atn.remove(atn)
                            try:
                                supercell_factor = int(len(res_slab_atn) / len(empty_atn))
                                if sorted(empty_atn * supercell_factor) \
                                        == sorted(res_slab_atn):
                                    found = True
                            except:
                                continue

                        if found:
                            n_ads = k
                            ads_i = n
                            ads_key = key
                            self.traj_files[key][n] = traj

                            species = clear_prefactor(self.reaction[key][n])

                            id, ase_id = check_in_ase(traj, self.ase_db)
                            key_value_pairs.update({'species': 
                                                    clear_state(species),
                                                    'n': n_ads})
                            if ase_id is None:
                                ase_id = write_ase(traj, self.ase_db, **key_value_pairs)
                            elif self.update:
                                update_ase(self.ase_db, id, **key_value_pairs)
                            self.ase_ids.update({species: ase_id})

      

            if n_ads > 1:
                for key1, values in prefactor_scale.iteritems():
                    for mol_i in range(len(values)):
                        #prefactor_scale_ads[key1][mol_i] = n_ads
                        if self.states[key1][mol_i] == 'gas':
                            prefactor_scale[key1][mol_i] = n_ads

            if supercell_factor > 1:
                for key2, values in prefactor_scale.iteritems():
                    for mol_i in range(len(values)):
                        if self.reaction[key2][mol_i] =='star':
                            prefactor_scale[key2][mol_i] *= supercell_factor + 1
            
            

        ## Transition state has higher energy
        #if len(np.unique(chemical_compositions)) > len(chemical_compositions):
        #    for chemical_composition in chemical_compositions:

        surface_composition = get_surface_composition(traj_empty)
        bulk_composition = get_bulk_composition(traj_empty)
        chemical_composition = get_chemical_formula(traj_empty)
        
        prefactors_final = copy.deepcopy(self.prefactors)
        for key in self.prefactors:
            for i, v in enumerate(self.prefactors[key]):
                prefactors_final[key][i] = self.prefactors[key][i] * \
                                           prefactor_scale[key][i] 

        reaction_energy = None
        activation_energy = None        

        reaction_energy, activation_energy = \
            get_reaction_energy(self.traj_files, prefactors_final, 
                                self.prefactors_TS)    

        #except:
        #    print 'ERROR: reaction energy failed: {}'.format(root)
        #    continue
                
        expr = -10 < reaction_energy < 10
        debug_assert(expr,
                     'reaction energy is wrong: {} eV: {}'\
                     .format(reaction_energy, root),
                     self.debug)
        expr = activation_energy is None or -10 < activation_energy < 10
        debug_assert(expr,
                     'activation energy is wrong: {} eV: {}'\
                     .format(activation_energy, root),
                     self.debug)

        reaction_info = {'reactants': {}, 
                         'products': {}}
        products = {}
        for key in ['reactants', 'products']:
            for i, r in enumerate(self.reaction[key]):
                r = clear_prefactor(r)
                reaction_info[key].update({r: self.prefactors[key][i]})

#       print chemical_composition, reaction_energy, activation_energy
        self.key_value_pairs_catapp = {'chemical_composition': chemical_composition,
                                       'surface_composition': surface_composition,
                                       'facet': self.facet,
                                       'sites': self.sites,
                                       'reactants': reaction_info['reactants'],
                                       'products': reaction_info['products'], 
                                       'reaction_energy': reaction_energy,
                                       'activation_energy': activation_energy,
                                       'dft_code': self.DFT_code,
                                       'dft_functional': self.DFT_functional,
                                       'publication': self.reference,
                                       'doi': self.doi,
                                       'year': int(self.year),
                                       'ase_ids': self.ase_ids,
                                       'user': self.user
                                   }

