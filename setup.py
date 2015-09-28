from setuptools import find_packages
from setuptools import setup


setup(
    name='Garcon',
    version='0.2.1',
    url='https://github.com/xethorn/garcon/',
    author='Michael Ortali',
    author_email='hello@xethorn.net',
    description=(
        'Lightweight library for AWS SWF.'),
    license='MIT',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        'Development Status :: Alpha',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
    ],)
