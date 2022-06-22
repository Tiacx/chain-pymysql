#!/usr/bin/env python
from distutils.core import setup

setup(
    name='chain-pymysql',
    version='1.0.1',
    url='https://github.com/Tiacx/chain-pymysql',
    project_urls={
        'Documentation': 'https://github.com/Tiacx/chain-pymysql/README.md',
    },
    description='Easy to use pymysql.',
    author='Taic',
    packages=['chain_pymysql'],
    install_requires=['pymysql'],
    classifiers=[
        # Chose either '3 - Alpha', '4 - Beta' or '5 - Production/Stable' as the current state of your package
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database',
    ],
    keywords='EasyPyMySql',
)
