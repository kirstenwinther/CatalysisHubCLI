import os
from sys import argv
from folderreader import FolderReader

def main(folder_name, debug=False, skip=[]):
    FR = FolderReader(folder_name=folder_name, debug=debug)
    FR.write(skip=skip)

folder_name = argv[1]

if __name__ == '__main__':
    main(folder_name)
