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

def check_traj(filename):
    try:
        atoms = read(filename)
    except:
        convert(filename)
    return

def get_reference(filename):
    atoms = read(filename)
    view(atoms)
    print atoms
    energy = atoms.get_potential_energy()
    name =  atoms.get_chemical_formula()
    return {name: energy}
    
def get_traj_str(filename):
    if isinstance(filename, str):
        atoms = read(filename)
    else:
        atoms = filename
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
    atoms = read(filename)
    return atoms.get_chemical_formula(mode=mode)

def get_number_of_atoms(filename):
    atoms = read(filename)
    return atoms.get_number_of_atoms()

def get_energy_diff(filename, filename_ref):
    atoms = read(filename)
    reference = read(filename_ref)
    return atoms.get_potential_energy() - reference.get_potential_energy()

def get_energy(filenames):
    if len(filenames) == 1:
        atoms = read(filenames[0])
        return atoms.get_potential_energy()    
    elif len(filenames) > 1:
        energies = []
        for filename in filenames:
            atoms = read(filename)
            energies.append(atoms.get_potential_energy())
        return energies
    
    
def clear_state(name):
    return name.replace('*','').replace('(g)', '').replace('star','').replace('gas','')

def get_state(name):
    if '*' in name or 'star' in name:
        state = 'star'
    elif 'gas' in name:
        state = 'gas'
    else:
        state = 'star'
    return state


def get_reaction_energy(traj_reactants, traj_products):
    energy_reactants = np.sum(get_energy(traj_reactants))
    energy_products = np.sum(get_energy(traj_products))
    reaction_energy = energy_products - energy_reactants

    return reaction_energy

def tag_atoms(atoms, types = None):
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
    atoms = read(filename)
    
    if len(np.unique(atoms.get_atomic_numbers())) == 1:
        return atoms.get_chemical_symbols()[0]

    layer_i = get_layers(atoms)
    top_layer_i = np.max(layer_i)
    atom_i = np.where(layer_i >= top_layer_i - 1)[0]

    layer_atoms = atoms[atom_i]

    surface_composition = layer_atoms.get_chemical_formula(mode='metal')

    return surface_composition

def tag_atoms(atoms, types = None):
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
    atoms = read(filename)
    
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
            c = atoms[atom_i].get_chemical_formula(mode ='metal')
            compositions.append(c)

    compositions = np.array(compositions)
    same_next_layer = compositions[1:] == compositions[:-1]
    bulk_compositions = compositions[:-1][same_next_layer]

    if all(c == bulk_compositions[0] for c in bulk_compositions):
        bulk_composition = bulk_compositions[0]
    else:
        bulk_composition = None 
    return bulk_composition


def check_in_ase(filename):
    """ Connect to ASE db"""
    db_ase = ase.db.connect('atoms.db')
    atoms = read(filename)
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
        print 'Already in ASE database'
        id = ids[0]
        unique_id = db_ase.get(id)['unique_id']
        return unique_id
    else:
        print 'write to ase'
        id = write_ase(filename)
        unique_id = db_ase.get(id)['unique_id']
        return unique_id

def write_ase(filename):
    """ Connect to ASE db"""
    atoms = read(filename)
    atoms = tag_atoms(atoms)
    db_ase = ase.db.connect('atoms.db')
    return db_ase.write(atoms)
    

def get_xyz(filename):
#    from ase.io.xyz import *
    atoms = read(filename, index=-1)
    atoms.write('test.xyz')
#    simple_write_xyz(atoms, 'test.xyz')


"""
def simple_write_xyz(fileobj, images, comment=''):
    symbols = images[0].get_chemical_symbols()
    natoms = len(symbols)
    for atoms in images:
        fileobj.write('%d\n%s\n' % (natoms, comment))
        for s, (x, y, z) in zip(symbols, atoms.positions):
            fileobj.write('%-2s %22.15f %22.15f %22.15f\n' % (s, x, y, z))

"""
