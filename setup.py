#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="tap-rest-api",
    packages=["tap_rest_api"],
    version="0.9.0",
    description="Singer.io tap for extracting data from any generic REST API",
    author="github.com/aaronsteers",
    url="https://github.com/aaronsteers/tap-rest-api",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    # py_modules=["tap_rest_api"],
    install_requires=["singer-python==5.3.3", "requests==2.20.0"],
    entry_points="""
          [console_scripts]
          tap-rest-api=tap_rest_api.cli:main
      """,
    package_data={"": ["tap_rest_api/catalog/*.json"]},
    include_package_data=True,
)
