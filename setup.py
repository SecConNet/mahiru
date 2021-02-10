#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))

# To update the package version number, edit proof_of_concept/__version__.py
version = {}
with open(os.path.join(here, 'proof_of_concept', '__version__.py')) as f:
    exec(f.read(), version)

with open('README.rst') as readme_file:
    readme = readme_file.read()

setup(
    name='proof_of_concept',
    version=version['__version__'],
    description="A proof of concept for a federated digital data marketplace",
    long_description=readme + '\n\n',
    author="Lourens Veen",
    author_email='l.veen@esciencecenter.nl',
    url='https://github.com/SecConNet/proof_of_concept',
    packages=find_packages(include=['proof_of_concept', 'proof_of_concept.*']),
    include_package_data=True,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='proof_of_concept',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    test_suite='tests',
    install_requires=[
        'cryptography',
        'docker',
        'falcon==3.0.0a3',
        'openapi-schema-validator',
        'python-dateutil',
        'requests',
        'retrying',
        'ruamel.yaml<=0.16.10',
        'yatiml'
    ],
    setup_requires=[
        # dependency for `python setup.py test`
        'pytest-runner',
        'pytest-pycodestyle',
        'pytest-pydocstyle',
        'pytest-mypy',
        # dependencies for `python setup.py build_sphinx`
        'sphinx',
        'sphinx_rtd_theme',
        'recommonmark'
    ],
    tests_require=[
        'pytest',
        'pytest-cov',
    ],
    extras_require={
        'dev':  ['prospector[with_pyroma]', 'yapf', 'isort'],
    }
)
