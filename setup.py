# -*- coding: utf-8 -*-
"""
This module contains the tool of collective.recipe.backup
"""
import os
from setuptools import setup, find_packages


def read(*rnames):
    return open(os.path.join(os.path.dirname(__file__), *rnames)).read()

version = '0.1 dev'

long_description = (
    read('README.txt')
    + '\n' +
    'Detailed Documentation\n'
    '**********************\n'
    + '\n' +
    read('collective', 'recipe', 'backup', 'README.txt')
    + '\n' +
    'Contributors\n'
    '************\n'
    + '\n' +
    read('CONTRIBUTORS.txt')
    + '\n' +
    'Change history\n'
    '**************\n'
    + '\n' +
    read('CHANGES.txt')
    + '\n' +
   'Download\n'
    '********\n')
entry_point = 'collective.recipe.backup:Recipe'
entry_points = {
    'zc.buildout': ["default = %s" % entry_point],
    #'console_scripts':[
    #        'backup = collective.recipe.backup.repozorunner:main'
    #        ]
    }

tests_require=['zope.testing', 'zc.buildout', 'zc.recipe.egg']

setup(name='collective.recipe.backup',
      version=version,
      description="bin/backup script: sensible defaults around bin/repozo",
      long_description=long_description,
      classifiers=[
        'Framework :: Buildout',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        ],
      keywords='buildout backup repozo zope',
      author='Reinout van Rees',
      author_email='reinout@vanrees.org',
      url='',
      license='GPL',
      packages=find_packages(exclude=['ez_setup']),
      namespace_packages=['collective', 'collective.recipe'],
      include_package_data=True,
      zip_safe=False,
      install_requires=['zc.buildout',
                        'setuptools',
                        # -*- Extra requirements: -*-
                        'zc.recipe.egg',
                        ],
      tests_require=tests_require,
      extras_require=dict(tests=tests_require),
      test_suite = 'collective.recipe.backup.tests.test_docs.test_suite',
      entry_points=entry_points,
      )