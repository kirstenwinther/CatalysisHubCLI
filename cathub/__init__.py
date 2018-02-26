import click
import six
import pprint


@click.group()
def cli():
    pass


@cli.group()
def ase_tools():
    """Some useful functions for working w/ ASE .traj file"""
    pass


@ase_tools.command()
@click.argument('filename')
def read_ase(filename):
    import ase_tools
    click.echo(ase_tools.read_ase(filename))


@ase_tools.command()
@click.argument('filename')
def check_traj(filename):
    """Read an ase .traj file, e.g. to check if it is readable."""
    import ase_tools
    click.echo(ase_tools.check_traj(filename))


@ase_tools.command()
@click.argument('filename')
def get_reference(filename):
    """Print chemical formula and energy of a .traj file"""
    import ase_tools
    click.echo(ase_tools.get_reference(filename))


@ase_tools.command()
@click.argument('filename')
def get_traj_str(filename):
    import ase_tools
    click.echo(ase_tools.get_traj_str(filename))


@ase_tools.command()
@click.argument('filename')
@click.option('--mode', help='Mode for chemical formula', default='metal')
def get_chemical_formula(filename, mode):
    """Print the chemical formula of inside a .traj file"""
    import ase_tools
    click.echo(ase_tools.get_chemical_formula(filename, mode=mode))


@ase_tools.command()
@click.argument('filename')
def get_number_of_atoms(filename):
    import ase_tools
    click.echo(ase_tools.get_number_of_atoms(filename))


@ase_tools.command()
@click.argument('filename')
@click.argument('filename_ref')
def get_energy_diff(filename, filename_ref):
    """Return the energy difference between two .traj files."""
    import ase_tools
    click.echo(ase_tools.get_energy_diff(filename, filename_ref))


@ase_tools.command()
@click.argument('filenames', nargs=-1)
def get_energies(filenames):
    """Return the energies of one or more .traj files."""
    import ase_tools
    click.echo(ase_tools.get_energies(filenames))


@ase_tools.command()
@click.argument('filename')
def get_energy(filename):
    """Return the energy of a .traj file."""
    import ase_tools
    click.echo(ase_tools.get_energy(filename))


@ase_tools.command()
@click.argument('name')
def clear_state(name):
    import ase_tools
    click.echo(ase_tools.clear_state(name))


@ase_tools.command()
@click.argument('molecule')
def clear_prefactor(molecule):
    import ase_tools
    click.echo(ase_tools.clear_prefactor(molecule))


@ase_tools.command()
@click.argument('molecule')
def get_atoms(molecule):
    import ase_tools
    click.echo(ase_tools.get_atoms(molecule))


@ase_tools.command()
@click.argument('name')
def get_state(name):
    import ase_tools
    click.echo(ase_tools.get_state(name))


@ase_tools.command()
@click.argument('traj_files', nargs=-1)
@click.argument('prefactors')
@click.argument('prefactors_TS')
def get_reaction_energy(traj_file, prefactors, prefactors_TS):
    import ase_tools
    prefactors = eval(prefactors)
    prefactors_TS = eval(prefactors_TS)
    click.echo(ase_tools.get_reaction_energy(
        traj_file, prefactors, prefactors_TS))

# skip tag_atoms
# skip  get_layers


@ase_tools.command()
@click.argument('filename')
def get_surface_composition(filename):
    import ase_tools
    click.echo(ase_tools.get_surface_composition(filename))


@ase_tools.command()
@click.argument('filename')
def get_n_layers(filename):
    import ase_tools
    click.echo(ase_tools.get_layers(filename))


@ase_tools.command()
@click.argument('filename')
def get_bulk_composition(filename):
    import ase_tools
    click.echo(ase_tools.get_bulk_composition(filename))


@ase_tools.command()
@click.argument('filenames')
@click.argument('ase-db')
@click.option('--energy', default=None, type=float)
def check_in_ase(filename, ase_db):
    import ase_tools
    click.echo(ase_tools.check_in_ase(filename, ase_db, energy=energy))


@ase_tools.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.pass_context
@click.argument('filename')
@click.argument('db_file')
def write_ase(ctx, filename, db_file):
    import ase_tools
    ase_tools.write_ase(filename, db_file, **ctx)


@ase_tools.command()
@click.argument('folder_name')
def get_reaction_from_folder(folder_name):
    import ase_tools
    click.echo(ase_tools.get_reaction_from_folder(folder_name))


# skip get_reaction_atoms

@cli.group()
def cathubsqlite():
    """Command line tools for setting up and inspecting a
    cathub sqlite database."""
    pass


@cathubsqlite.command()
@click.argument('filename')
def init(filename):
    """Initialize a Cathub database."""
    import cathubsqlite
    db = cathubsqlite.CathubSQLite(filename)
    con = db._connect()
    db._initialize(con)


@cathubsqlite.command()
@click.argument('filename')
@click.argument('row_id', default=0)
def read(filename, row_id):
    import cathubsqlite
    db = cathubsqlite.CathubSQLite(filename)
    click.echo(db.read(row_id))

# skip write


@cathubsqlite.command()
@click.argument('reaction_energy', type=float)
def check(reaction_energy):
    """Check if reaction energy <energy> is already present in cathub db."""
    import cathubsqlite
    db = cathubsqlite.CathubSQLite(filename)
    click.echo(cathubsqlite.check(reaction_energy))


@cli.command()
@click.option('--base', default='.')
def convert_traj(base):
    """Convert .traj file from ASE <=3.9 to ASE >=3.10 format."""
    import convert_traj
    convert_traj.main(base)


@cli.command()
def db_status():
    """Check status of SQLITE DB."""
    import db_status


@cli.command()
@click.argument('template')
@click.option('--create-template', is_flag=True, help="Create an empty template file.")
@click.option('--custom-base', )
def make_folders_template(create_template, template, custom_base, ):
    """A create a basic folder tree to put in DFT calculcations.

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

    Remember to include the adsorption energy of reaction intermediates, taking
    gas phase molecules as references (preferably H20, H2, CH4, CO, NH3).
    For example, we can write the desorption of CH2 as:
    CH2* -> CH4(g) - H2(g) + *
    Here you would have to write 'CH4gas-H2gas' as "products_A" entry.

    See examples:

    reactions = [
        {'reactants': ['CH2star'], 'products': ['CH4gas', '-H2gas', 'star']},
        {'reactants': ['CH3star'], 'products': ['CH4gas', '-0.5H2gas', 'star']}
        ]



    Reaction info is now a list of dictionaries. 
    A new dictionary is required for each reaction, and should include two lists,
    'reactants' and 'products'. Remember to include a minus sign in the name when
    relevant.

# ---------------surface info---------------------

    facets # If complicated structure: use term you would use in publication
    sites # put sites or additional info is necessary. Use '_' in the case of different adsorbates.
    """
    import make_folders_template
    import json
    import os

    if custom_base is None:
        custom_base = os.path.abspath(os.path.curdir)

    template_data = {
        'title': 'Fancy title',
        'authors': ['Doe, John', 'Einstein, Albert'],
        'journal': 'JACS',
        'volume': '1',
        'number': '1',
        'pages': '23-42',
        'year': '2017',
        'publisher': 'ACS',
        'doi': '10.NNNN/....',
        'DFT_code': 'Quantum Espresso',
        'DFT_functional': 'BEEF-vdW',
        'reactions': [
                {'reactants': ['OOHstar'], 'products': [
                    '2.0H2Ogas', '-1.5H2gas', 'star']},
                {'reactants': ['CCH3'], 'products': ['C', 'CH3']},
                {'reactants': ['CH3star'], 'products': ['CH3gas', 'star']}
        ],
        'surfaces': ['Pt'],
        'facets': ['111'],
        'sites': ['top', 'hcp', 'fcc_hcp'],
    }
    if template is not None:
        if create_template:
            if os.path.exists(template):
                raise UserWarning(
                    "File {template} already exists. Refusing to overwrite".format(**locals()))
            with open(template, 'w') as outfile:
                outfile.write(json.dumps(template_data, indent=4,
                                         separators=(',', ': '), sort_keys=True) + '\n')
                return
        else:
            with open(template) as infile:
                template_data = json.load(infile)
                title = template_data['title']
                authors = template_data['authors']
                journal = template_data['journal']
                volume = template_data['volume']
                number = template_data['number']
                pages = template_data['pages']
                year = template_data['year']
                publisher = template_data['publisher']
                doi = template_data['doi']
                dft_code = template_data['DFT_code']
                dft_functional = template_data['DFT_functional']
                reactions = template_data['reactions']
                surfaces = template_data['surfaces']
                facets = template_data['facets']
                sites = template_data['sites']

    make_folders_template.main(
        title=title,
        authors=eval(authors) if isinstance(
            authors, six.string_types) else authors,
        journal=journal,
        volume=volume,
        number=number,
        pages=pages,
        year=year,
        publisher=publisher,
        doi=doi,
        DFT_code=dft_code,
        DFT_functional=dft_functional,
        reactions=eval(reactions) if isinstance(
            reactions, six.string_types) else reactions,
        custom_base=custom_base,
        surfaces=surfaces,
        facets=facets,
        sites=sites,
    )


@cli.command()
def postgresql():
    """Create a basic PostgreSQL database with Cathub schema."""


@cli.command()
def psql_server_connect():
    """Test connection to PostreSQL server."""
    import psql_server_connect


@cli.command()
def read():
    """Read a directory of .traj file into a database."""
    pass


@cli.command()
def tools():
    """Various tools."""
    pass


@cli.command()
def write_postgresql():
    """Write a local database to cloud hosted PostgreSQL options."""
    pass


@cli.command()
@click.argument('user')
@click.argument('pub_level')
@click.argument('DFT_level')
@click.argument('XC_level')
@click.argument('reaction_level')
@click.argument('metal_level')
@click.argument('facet_level')
@click.argument('site_level')
@click.argument('final_level')
def write_user_spec():
    """Write JSON specfile for single DFT calculation."""
    import write_user_spec
