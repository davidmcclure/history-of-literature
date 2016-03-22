

from setuptools import setup, find_packages


setup(

    name='htrc-dev',
    version='0.1.0',
    description='Experimental HTRC bindings.',
    url='https://github.com/davidmcclure/htrc-dev',
    license='MIT',
    author='David McClure',
    author_email='dclure@stanford.edu',
    packages=find_packages(),
    scripts=['bin/htrc'],

)
