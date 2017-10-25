import json
from sys import argv

try:  #sherlock 1 or 2
    sherlock = os.environ['SHERLOCK']
    if sherlock == '1':
        catbase = '/home/winther/data_catapp'
    elif sherlock == '2':
        catbase = '/home/users/winther/data_catapp'
except:  # SUNCAT
    catbase = '/nfs/slac/g/suncatfs/data_catapp'


user, pub, XC, reaction, metal, facet, site = argv[1:]

user_dict = {'user_level': user,
             'pub_level': int(pub),
             'DFT_level': int(DFT),
             'XC_level': int(XC),
             'reaction_level': int(reaction),
             'metal_level': int(metal),
             'facet_level':int(facet),
             'site_level': int(site)
             }

user_file = '{}user_specific/{}.txt'.format(catbase, user) 
json.dump(user_dict, open(user_file, 'wb'))



