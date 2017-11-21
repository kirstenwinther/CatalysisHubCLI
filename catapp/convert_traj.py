import os
from sys import path, argv
from distutils.version import StrictVersion
import ase

assert StrictVersion(ase.__version__) >= StrictVersion('3.10.0')
try:  # sherlock 1 or 2
    sherlock = os.environ['SHERLOCK']
    if sherlock == '1':
        catbase = '/home/winther/data_catapp/winther/scripts'
    elif sherlock == '2':
        catbase = '/home/users/winther/data_catapp/winther/scripts'
except:  # SUNCAT
    catbase = '/nfs/slac/g/suncatfs/data_catapp/winther/scripts'

path.append(catbase)
from ase_tools import check_traj


def main(base):
    for roots, dirs, files in os.walk(base):
        traj_files = [f for f in files if f.endswith('traj')]
        for f in traj_files:
            check_traj('{}/{}'.format(roots, f))

try:
    base = argv[1]
except:
    base = '.'

if __name__ == '__main__':
    main(base)
