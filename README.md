## Installation

Install with pip using

    pip install --user --upgrade git+https://github.com/mhoffman/CatApp-database.git


## Usage

Run `catapp`, like so

    catapp --help

or with any of its sub-commands, like so

    catapp make_folders_template --help

## Examples


To create an .json input file

    catapp make_folders_template project1.json --create-template

To create a folder structures from a .json input file

    catapp make_folders_template project1.json
