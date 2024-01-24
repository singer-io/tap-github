#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-github',
      version='3.1.0',
      description='Singer.io tap for extracting data from the GitHub API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_github'],
      install_requires=[
          'singer-python==6.0.0',
          'requests==2.31.0',
          'backoff==2.2.1'
      ],
      extras_require={
          'dev': [
              'pylint==3.0.3',
              'ipdb',
              'nose2',
              'requests-mock==1.9.3'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-github=tap_github:main
      ''',
      packages=['tap_github'],
      package_data = {
          'tap_github': ['tap_github/schemas/*.json']
      },
      include_package_data=True
)
