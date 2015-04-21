#!/usr/bin/env python
#
# Copyright 2013 Trey Morris
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
import sys

from setuptools import setup, find_packages


install_requires = ['redis']
print find_packages()

setup(
    name='twiceredis',
    version='1.0.0',
    author='Trey Morris',
    author_email='trey@treymorris.com',
    description='sentinel pool backed read and write redis client',
    long_description=open('README.rst').read(),
    install_requires=install_requires,
    classifiers=['Development Status :: 5 - Production/Stable',
                 'License :: OSI Approved :: Apache Software License'],
    keywords='redis',
    packages=find_packages(),
    license='Apache Software License',
    url='https://github.com/tr3buchet/twiceredis')
