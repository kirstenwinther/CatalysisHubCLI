#!/usr/bin/python

import os
import sys
import json

try:  # sherlock 1 or 2
    sherlock = os.environ['SHERLOCK']
    if sherlock == '1':
        catbase = '/home/winther/data_catapp'
    elif sherlock == '2':
        catbase = '/home/users/winther/data_catapp'
except:  # SUNCAT
    catbase = '/nfs/slac/g/suncatfs/data_catapp'

sys.path.append(catbase)
from tools import extract_atoms, check_reaction


username = os.environ['USER']

# ---------publication info------------


def main(
    title='Fancy title',  # work title if not yet published
    authors=['Doe, John', 'Einstein, Albert'],  # name required
    journal='',
    volume='',
    number='',
    pages='',
    year='2017',  # year required
    publisher='',
    doi='',
    DFT_code='',  # for example 'Quantum ESPRESSO'
    DFT_functional='',  # For example 'BEEF-vdW'

    #  ---------molecules info-----------

    reactions=[
        {'reactants': ['OOHstar@ontop'], 'products': [
            '2.0H2Ogas', '-1.5H2gas', 'star']},
        #{'reactants': ['CCH3'], 'products': ['C', 'CH3']},
        #{'reactants': ['CH3star'], 'products': ['CH3gas', 'star']}
    ],
    surfaces=['Pt'],
    facets=['111'],

    custom_base=None,
):

    """
    Dear all

    Use this script to make the right structure for your folders.
    Folders will be created automatically when you run the script with python.
    Start by copying the script to a folder in your username,
    and assign the right information to the variables below.

    You can change the parameters and run the script several times if you,
    for example, are using different functionals or are doing different reactions
    on different surfaces.


    Include the phase if necessary:

    'star' for empty site or adsorbed phase. Only necessary to put 'star' if
    gas phase species are also involved.
    'gas' if in gas phase

    Include the adsorption site if relevant:
    In case of adsorbed species, include the site after 'star' as f.ex
    star@top, star@bridge.

    Remember to include the adsorption energy of reaction intermediates, taking
    gas phase molecules as references (preferably H20, H2, CH4, CO, NH3).
    For example, we can write the desorption of CH2 as:
    CH2* -> CH4(g) - H2(g) + *
    Here you would have to write 'CH4gas-H2gas' as "products_A" entry.

    See examples:

    reactions = [
        {'reactants': ['CH2star@bridge'], 'products': ['CH4gas', '-H2gas', 'star']},
        {'reactants': ['CH3star@top'], 'products': ['CH4gas', '-0.5H2gas', 'star']}
        ]

    Reaction info is now a list of dictionaries. 
    A new dictionary is required for each reaction, and should include two lists,
    'reactants' and 'products'. Remember to include a minus sign in the name when
    relevant.

    # ---------------surface info---------------------

    facets # If complicated structure: use term you would use in publication
    """

    #  ----------- You're done!------------------------

    # Check reactions
    #assert len(reactants) == len(products_A) == len(products_B)

    for reaction in reactions:
        check_reaction(reaction['reactants'], reaction['products'])

    # Set up directories
    base = '%s/%s/' % (catbase, username)
    if custom_base is not None:
        base = custom_base + '/'

    if not os.path.exists(base):
        os.mkdir(base)

    publication_shortname = '%s_%s_%s' % (authors[0].split(',')[0].lower(),
                                          title.split()[0].lower(), year)

    publication_base = base + publication_shortname + '/'

    if not os.path.exists(publication_base):
        os.mkdir(publication_base)

# save publication info to publications.txt
    publication_dict = {'title': title,
                        'authors': authors,
                        'journal': journal,
                        'volume': volume,
                        'number': number,
                        'pages': pages,
                        'year': year,
                        'publisher': publisher,
                        'doi': doi
                        }

    pub_txt = publication_base + 'publication.txt'
    json.dump(publication_dict, open(pub_txt, 'wb'))

    create = []  # list of directories to be made
    create.append(publication_base + DFT_code + '/')
    create.append(create[-1] + DFT_functional + '/')

    base_reactions = create[-1]

    for i in range(len(reactions)):
        rname = '_'.join(reactions[i]['reactants'])
        pname = '_'.join(reactions[i]['products'])
        reaction_name = '__'.join([rname, pname])
        create.append(base_reactions + reaction_name + '/')
        base_surfaces = create[-1]
        for surface in surfaces:
            create.append(base_surfaces + surface + '/')
            base_facets = create[-1]
            for facet in facets:
                create.append(base_facets + facet + '/')
                base_sites = create[-1]
                #for site in sites:
                #    create.append(base_sites + site + '/')

    for path in create:
        if not os.path.exists(path):
            os.mkdir(path)

if __name__ == "__main__":
    main()
