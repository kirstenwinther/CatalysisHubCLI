from setuptools import setup

setup(name='cathub',
      version='0.0.2',
      packages=['cathub',
                'cathub.ase_tools'],
      install_requires=[
          'Click',
          'six',
      ],
      entry_points='''
            [console_scripts]
            cathub=cathub:cli
        ''',
      long_description=open('README.md').read(),
    )
