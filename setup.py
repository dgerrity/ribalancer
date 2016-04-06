#!/usr/bin/env python
import sys

from setuptools import setup, find_packages

import ribalance


requires = ['boto3>=1.2.3']

if sys.version_info[:2] == (2, 6):
    # For python2.6 we have to require argparse since it
    # was not in stdlib until 2.7.
    requires.append('argparse>=1.1')


setup_options = dict(
    name='ribalance',
    version=ribalance.__version__,
    description='Optimize AWS Reserved Instance Allocation',
    long_description=open('README.md').read(),
    author='Valentino Volonghi (AdRoll)',
    url='http://github.com/AdRoll/ribalance',
    scripts=['bin/ribalance'],
    packages=find_packages(exclude=['tests*']),
    install_requires=requires,
    extras_require={
        ':python_version=="2.6"': [
            'argparse>=1.1',
        ]
    },
    license="MIT",
    classifiers=(
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ),
)

setup(**setup_options)
