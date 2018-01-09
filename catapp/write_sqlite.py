import psycopg2
import os
import sys
from folderreader import FolderReader

user = sys.argv[1]
FR = FolderReader(user=user)

FR.write()


