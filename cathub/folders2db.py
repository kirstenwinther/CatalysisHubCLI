import os
from sys import argv
from folderreader import FolderReader

def main(user, debug=False):
    FR = FolderReader(user=user, debug=debug)
    FR.write(skip=None, goto=None

try:
    user = argv[1]
except:
    user = os.environ['USER']

if __name__ == '__main__':
    main(user)
