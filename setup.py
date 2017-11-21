from setuptools import setup

setup(name='catapp',
      version='0.0.1',
      packages=['catapp'],
      install_requires=[
          'Click',
          'six',
      ],
      entry_points='''
            [console_scripts]
            catapp=catapp:cli
        ''',
      long_description=file('README.md').read(),
    )
