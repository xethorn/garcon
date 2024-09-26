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
    version='1.1.1',
    url='https://github.com/xethorn/garcon/',
    author='Michael Ortali',
    author_email='hello@xethorn.net',
    description=('Lightweight library for AWS SWF.'),
    long_description=long_description,
    license='MIT',
    packages=find_packages(),
    include_package_data=True,
    install_requires=['boto3', 'backoff'],
    zip_safe=False,
    classifiers=[
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],)
