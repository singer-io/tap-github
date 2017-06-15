#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-github',
      version='0.2.2',
      description='Singer.io tap for extracting data from the GitHub API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_github'],
      install_requires=['singer-python==1.6.0',
                        'requests==2.13.0'],
      entry_points='''
          [console_scripts]
          tap-github=tap_github:main
      ''',
      packages=['tap_github'],
      package_data = {
          'tap_github': [
              'commits.json',
              'issues.json'
              ]
          }
)
