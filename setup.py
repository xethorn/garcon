from setuptools import find_packages
from setuptools import setup
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))
# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()


setup(
    name='Garcon',
    version='0.4.0',
    url='https://github.com/xethorn/garcon/',
    author='Michael Ortali',
    author_email='hello@xethorn.net',
    description=(
        'Lightweight library for AWS SWF.'),
    long_description=long_description,
    license='MIT',
    packages=find_packages(),
    include_package_data=True,
    data_files=[('graph', ['graph/graph.js', 'graph/graph.json'])],
    install_requires=[
        'boto',
        'backoff',
        'networkx',
        'flask'],
    zip_safe=False,
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
    ],)
