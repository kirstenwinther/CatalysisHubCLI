import click

@click.group()
def cli():
    pass


@cli.command()
def ase_tools():
    pass

@cli.command()
def catappsqlite():
    pass

@cli.command()
def convert_traj():
    pass

@cli.command()
def db_status():
    pass

@cli.command()
def make_folders_template():
    pass

@cli.command()
def postgresql():
    pass

@cli.command()
def psql_server_connect():
    pass

@cli.command()
def read():
    pass

@cli.command()
def tools():
    pass

@cli.command()
def write_postgresql():
    pass

@click.command()
def write_user_spec():
    pass

