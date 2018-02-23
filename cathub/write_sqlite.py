import psycopg2
import os
import sys
from folderreader import FolderReader

user = sys.argv[1]
if len(sys.argv) > 2:
    goto_reaction = sys.argv[2]
else:
    goto_reaction = None

FR = FolderReader(user=user, debug=True)


FR.write(skip=None, goto=None)


