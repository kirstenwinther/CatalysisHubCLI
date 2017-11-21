#!/usr/bin/python
import numpy as np
import ase
from ase import *
from sys import argv
from ase.io import read, write
from ase.db.row import AtomsRow
from ase.io.jsonio import encode
from ase.io.trajectory import convert
from ase.visualize import view
import json
import csv
import six


def read_ase(filename):
    if isinstance(filename, six.string_types):
        atoms = read(filename)
    else:
        atoms = filename
    return atoms


def check_traj(filename, strict=True, verbose=True):
    try:
        atoms = read_ase(filename)
        if verbose:
            print 'traj file ok!'
    except:
        try:
            convert(filename)
            if verbose:
                print 'Converting to new ase format!'
            atoms = read_ase(filename)
        except:
            print 'Could not read .traj file'
            return False

    try:
        atoms.get_potential_energy()
    except:
        if strict:
            raise RuntimeError, 'No energy for .traj file: {}'.format(filename)
        else:
            return False
    return True


def get_reference(filename):
    atoms = read_ase(filename)
    energy = atoms.get_potential_energy()
    name = atoms.get_chemical_formula()
    return {name: str(energy)}


def get_traj_str(filename):
    atoms = read_ase(filename)
    row = AtomsRow(atoms)
    dct = {}
    for key in row.__dict__:
        if key[0] == '_' or key in row._keys or key == 'id':
            continue
        dct[key] = row[key]
    constraints = row.get('constraints')
    if constraints:
        dct['constraints'] = constraints

    txt = ','.join('"{0}": {1}'.format(key, encode(dct[key]))
                   for key in sorted(dct.keys()))

    atoms_txt = '{{{0}}}'.format(txt)
    return atoms_txt


def get_chemical_formula(filename, mode='metal'):
    atoms = read_ase(filename)
    return atoms.get_chemical_formula(mode=mode)


def get_number_of_atoms(filename):
    atoms = read_ase(filename)
    return atoms.get_number_of_atoms()


def get_energy_diff(filename, filename_ref):
    atoms = read_ase(filename)
    reference = read_ase(filename_ref)
    return atoms.get_potential_energy() - reference.get_potential_energy()


def get_energies(filenames):
    if len(filenames) == 1:
        atoms = read_ase(filenames[0])
        return atoms.get_potential_energy()
    elif len(filenames) > 1:
        energies = []
        for filename in filenames:
            atoms = read_ase(filename)
            energies.append(atoms.get_potential_energy())
        return energies


def get_energy(filename):
    atoms = read_ase(filename)
    return atoms.get_potential_energy()


def get_atomic_numbers(filename):
    atoms = read_ase(filename)
    return list(atoms.get_atomic_numbers())


def get_formula_from_numbers(numbers):
    formula = Atoms(numbers).get_chemical_formula(mode='all')
    return formula


def get_numbers_from_formula(formula):
    atoms = Atoms(formula)
    return get_atomic_numbers(atoms)
    

def clear_state(name):
    name = name.replace('*', '').replace('(g)', '')
    name = name.replace('star', '').replace('gas', '')
    return name


def clear_prefactor(molecule):
    if molecule == '':
        return molecule
    if not molecule[0].isalpha():
        i = 0
        while not molecule[i].isalpha():
            i += 1
        molecule = molecule[i:]
    return molecule


def get_atoms(molecule):
    molecule = clear_state(molecule)
    if molecule == '':
        prefactor = 1
        return molecule, prefactor
    try:
        return '', float(molecule)
    except:
        pass
    if not molecule[0].isalpha():
        i = 0
        while not molecule[i].isalpha():
            i += 1
        prefactor = molecule[:i]
        if prefactor == '-':
            prefactor = -1
        prefactor = float(prefactor)
        molecule = molecule[i:]
    else:
        prefactor = 1

    temp = ''
    for k in range(len(molecule)):
        if molecule[k].isdigit():
            for j in range(int(molecule[k]) - 1):
                temp += molecule[k - 1]
        else:
            temp += molecule[k]

    molecule = ''.join(sorted(temp))

    return molecule, prefactor


def get_state(name):
    if '*' in name or 'star' in name:
        state = 'star'
    elif 'gas' in name:
        state = 'gas'
    else:
        state = 'star'
    return state


def get_reaction_energy(traj_files, prefactors, prefactors_TS):
    energies = {}
    for key in traj_files.keys():
        energies.update({key: ['' for n in range(len(traj_files[key]))]})
    for key, trajlist in traj_files.iteritems():
        for i, traj in enumerate(trajlist):
            energies[key][i] = prefactors[key][i] * get_energy(traj)

    energy_reactants = np.sum(energies['reactants'])
    energy_products = np.sum(energies['products'])

    reaction_energy = energy_products - energy_reactants

    if 'TS' in traj_files.keys():
        for i, traj in enumerate(traj_files['reactants']):
            energies['reactants'][i] = prefactors_TS[
                'reactants'][i] * get_energy(traj)
        energy_reactants = np.sum(energies['reactants'])
        energy_TS = energies['TS'][0]
        activation_energy = energy_TS - energy_reactants
    else:
        activation_energy = None
    return reaction_energy, activation_energy


def tag_atoms(atoms, types=None):
    non_metals = ['H', 'He', 'B', 'C', 'N', 'O', 'F', 'Ne',
                  'Si', 'P', 'S', 'Cl', 'Ar',
                  'Ge', 'As', 'Se', 'Br', 'Kr',
                  'Sb', 'Te', 'I', 'Xe',
                  'Po', 'At', 'Rn']

    layer_i = get_layers(atoms)
    top_layer_i = np.max(layer_i)
    i = 0

    for i in range(0, top_layer_i + 1):
        atoms_i = np.where(layer_i == top_layer_i - i)[0]
        if len(np.where(layer_i == top_layer_i - i)[0]) == 1 and i < 4:
            atom = atoms[atoms_i[0]]
            if types is not None:
                if atom.symbol in types:
                    atom.tag = 0
            elif types is None:
                if atom.symbol in non_metals:
                    atom.tag = 0
        else:
            for l in atoms_i:
                atoms[l].tag = i + 1

    return atoms


def get_layers(atoms):
    tolerance = 0.2
    d = atoms.positions[:, 2]
    keys = np.argsort(d)
    ikeys = np.argsort(keys)
    mask = np.concatenate(([True], np.diff(d[keys]) > tolerance))
    layer_i = np.cumsum(mask)[ikeys]

    if layer_i.min() == 1:
        layer_i -= 1
    return layer_i


def get_surface_composition(filename):
    atoms = read_ase(filename)

    if len(np.unique(atoms.get_atomic_numbers())) == 1:
        return atoms.get_chemical_symbols()[0]

    layer_i = get_layers(atoms)
    top_layer_i = np.max(layer_i)
    atom_i = np.where(layer_i >= top_layer_i - 1)[0]

    layer_atoms = atoms[atom_i]

    surface_composition = layer_atoms.get_chemical_formula(mode='metal')

    return surface_composition


def tag_atoms(atoms, types=None):
    non_metals = ['H', 'He', 'B', 'C', 'N', 'O', 'F', 'Ne',
                  'Si', 'P', 'S', 'Cl', 'Ar',
                  'Ge', 'As', 'Se', 'Br', 'Kr',
                  'Sb', 'Te', 'I', 'Xe',
                  'Po', 'At', 'Rn']

    layer_i = get_layers(atoms)
    top_layer_i = np.max(layer_i)
    i = 0

    for i in range(0, top_layer_i + 1):
        atoms_i = np.where(layer_i == top_layer_i - i)[0]
        if len(np.where(layer_i == top_layer_i - i)[0]) == 1 and i < 4:
            atom = atoms[atoms_i[0]]
            if types is not None:
                if atom.symbol in types:
                    atom.tag = 0
            elif types is None:
                if atom.symbol in non_metals:
                    atom.tag = 0
        else:
            for l in atoms_i:
                atoms[l].tag = i + 1
    return atoms


def get_n_layers(filename):
    atoms = read_ase(filename)
    layer_i = get_layers(atoms)
    n = np.max(layer_i)
    return n


def get_layers(atoms):
    tolerance = 0.01
    d = atoms.positions[:, 2]
    keys = np.argsort(d)
    ikeys = np.argsort(keys)
    mask = np.concatenate(([True], np.diff(d[keys]) > tolerance))
    layer_i = np.cumsum(mask)[ikeys]

    if layer_i.min() == 1:
        layer_i -= 1
    return layer_i


def get_bulk_composition(filename):
    atoms = read_ase(filename)

    if len(np.unique(atoms.get_atomic_numbers())) == 1:
        return atoms.get_chemical_symbols()[0]

    layer_i = get_layers(atoms)
    top_layer_i = np.max(layer_i)
    compositions = []
    for i in range(0, top_layer_i + 1):
        atom_i = np.where(layer_i == top_layer_i - i)[0]
        atoms_layer = atoms[atom_i]
        if len(np.unique(atoms_layer.get_atomic_numbers())) == 1:
            c = atoms_layer.get_chemical_symbols()[0]
            compositions.append(c)
        else:
            c = atoms[atom_i].get_chemical_formula(mode='metal')
            compositions.append(c)

    compositions = np.array(compositions)
    same_next_layer = compositions[1:] == compositions[:-1]
    bulk_compositions = compositions[:-1][same_next_layer]

    if len(bulk_compositions) > 0 and all(c == bulk_compositions[0] for c in bulk_compositions):
        bulk_composition = bulk_compositions[0]
    else:
        bulk_composition = None
    return bulk_composition


def check_in_ase(filename, ase_db, energy=None):
    """ Check if entry is allready in ASE db
    """

    db_ase = ase.db.connect(ase_db)
    atoms = read_ase(filename)
    if energy is None:
        energy = atoms.get_potential_energy()
    formula = atoms.get_chemical_formula(mode='metal')
    rows = db_ase.select(energy=energy)
    n = 0
    ids = []
    for row in rows:
        if formula == row.formula:
            n += 1
            ids.append(row.id)
    if n > 0:
        print '{} already in ASE database'.format(formula)
        id = ids[0]
        unique_id = db_ase.get(id)['unique_id']
        return unique_id
    else:
        return None


def write_ase(filename, db_file, **key_value_pairs):
    """ Connect to ASE db"""
    atoms = read_ase(filename)

    atoms = tag_atoms(atoms)
    db_ase = ase.db.connect(db_file)
    id = db_ase.write(atoms, **key_value_pairs)
    print 'writing atoms to ASE db row id = {}'.format(id)
    unique_id = db_ase.get(id)['unique_id']
    return unique_id


def get_reaction_from_folder(folder_name):
    reaction = {}
    if '__' in folder_name:  # Complicated reaction
        if '-' in folder_name and '_-' not in folder_name:
            # intermediate syntax
            a, b = folder_name.split('-')
            folder_name = a + '_-' + b

        reaction.update({'reactants': folder_name.split('__')[0].split('_'),
                         'products': folder_name.split('__')[1].split('_')})

    elif '_' in folder_name:  # Standard format
        AB, A, B = folder_name.split('_')
        if '-' in A:
            A = A.split('-')
            A[1] = '-' + A[1]
            products = [A[0], A[1], B]
        else:
            products = [A, B]
        reaction.update({'reactants': [AB],
                         'products': products})
    else:
        raise AssertionError, 'problem with folder {}'.format(foldername)
    return reaction


def get_reaction_atoms(reaction):
    reaction_atoms = {'reactants': [],
                      'products': []}

    prefactors = {'reactants': [],
                  'products': []}

    states = {'reactants': [],
              'products': []}

    for key, mollist in reaction.iteritems():
        for molecule in mollist:
            atoms, prefactor = get_atoms(molecule)
            reaction_atoms[key].append(atoms)
            prefactors[key].append(prefactor)
            state = get_state(molecule)
            states[key].append(state)

    import copy
    prefactors_TS = copy.deepcopy(prefactors)
    # Empty slab balance

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


    return reaction_atoms, prefactors, prefactors_TS, states


def debug_assert(expression, message, debug=False):
    if debug:
        try:
            assert expression, message
        except AssertionError as e:
            print(e)
    else:
        assert expression, message

# def handle_gas_species():
