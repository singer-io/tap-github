#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-github',
      version='1.9.2',
      description='Singer.io tap for extracting data from the GitHub API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_github'],
      install_requires=[
          'singer-python==5.3.3',
          'requests==2.20.0'
      ],
      extras_require={
          'dev': [
              'pylint',
              'ipdb',
              'nose',
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
