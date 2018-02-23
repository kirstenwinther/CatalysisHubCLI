## Installation

Install with pip using

    pip install --user --upgrade git+https://github.com/kirstenwinther/CatalysisHubCLI.git


## Usage

Run `cathub`, like so

    cathub --help

or with any of its sub-commands, like so

    cathub make_folders_template --help

## Examples


To create an .json input file

    cathub make_folders_template project1.json --create-template

To create a folder structures from a .json input file

    cathub make_folders_template project1.json
