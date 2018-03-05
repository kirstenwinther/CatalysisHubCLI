#!/usr/bin/env python

import requests
import pprint

def main(query):
    root = 'http://catappdatabase2.herokuapp.com/graphql'
    print(query)
    data = requests.post(root, {'query': query}).json()
    pprint.pprint(data)
    return data


def graphql_query(table='reactions',
                  columns=['chemical_composition',
                           'reactants',
                           'products'],
                  n_results=10,
                  queries=None):


    statement = '{'
    statement += '{}(first: {}'.format(table, n_results)
    for key, value in queries.iteritems():
        if isinstance(value, str):
            statement += ', {}: "{}"'.format(key, value)
        else:
            statement += ', {}: {}'.format(key, value)
    statement += ') { \n'
    statement += 'totalCount \n  edges { \n    node { \n'
    for column in columns:
        statement += '      {}\n'.format(column)

    statement += '    }\n'
    statement += '  }\n'
    statement += '}}'
    
    return  statement


if __name__ == '__main__':
    query = graphql_query(table='reactions',
                          columns=['chemicalComposition',
                                   'reactants',
                                   'products'],
                          n_results=10,
                          queries={'chemicalComposition': "~Pt"})
    
    main(query)
