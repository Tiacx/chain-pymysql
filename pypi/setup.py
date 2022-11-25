#!/usr/bin/env python
from distutils.core import setup

version = '1.0.4'

with open('./README.rst', encoding='utf-8') as f:
    readme = f.read()

setup(
    name='chain-pymysql',
    version=version,
    url='https://github.com/Tiacx/chain-pymysql',
    project_urls={
        'Documentation': 'https://github.com/Tiacx/chain-pymysql',
    },
    description='Easy to use PyMySQL.',
    long_description=readme,
    author='Taic',
    packages=['chain_pymysql'],
    install_requires=['pymysql'],
    classifiers=[
        # Chose either '3 - Alpha', '4 - Beta' or '5 - Production/Stable' as the current state of your package
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database',
    ],
    keywords='ChainPyMySql',
)
