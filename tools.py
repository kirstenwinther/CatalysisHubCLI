#!/usr/bin/python


def extract_atoms(molecule):
    # return a string with all atoms in molecule
    if molecule == '':
        return molecule
    atoms = ''
    if not molecule[0].isalpha():
        i = 0
        while not molecule[i].isalpha():
            i += 1
        prefactor = float(molecule[:i])
        if prefactor < 0:
            prefactor = abs(prefactor)
            sign = '-'
        else:
            sign = ''
        molecule = molecule[i:]
    else:
        prefactor = 1
        sign = ''
    for k in range(len(molecule)):
        if molecule[k].isdigit():
            for j in range(int(molecule[k]) - 1):
                atoms += molecule[k - 1]
        else:
            atoms += molecule[k]
    if prefactor % 1 == 0:
        atoms *= int(prefactor)
    elif prefactor % 1 == 0.5:
        atoms_sort = sorted(atoms)
        N = len(atoms)
        atoms = ''
        for n in range(N):
            for m in range(int(prefactor - 0.5)):
                atoms += atoms_sort[n]
            if n % 2 == 0:
                atoms += atoms_sort[n]

    return sign + ''.join(sorted(atoms))

def add_atoms(atoms_list):
    add = ''
    sub = ''
    for atoms in atoms_list:
        if len(atoms)>0 and atoms[0] == '-':
            sub += atoms[1:]
        else:
            add += atoms
    return add.replace(sub, '', 1)

def check_reaction(reactants, products):
    """
    check the stoichiometry and format of chemical reaction used for 
    folder structure.
    list of reactants -> list of products
    """
    
    reactants = [reactant.strip('star').strip('gas') for reactant in reactants]
    products = [product.strip('star').strip('gas') for product in products]

    reactants = [extract_atoms(reactant) for reactant in reactants]
    products = [extract_atoms(product) for product in products]

    reactants = add_atoms(reactants)
    products = add_atoms(products)

    assert ''.join(sorted(reactants)) == ''.join(sorted(products))

