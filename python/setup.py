from setuptools import setup

setup(
   name='sparker',
   version='1.0',
   description='SparkER: An Entity Resolution framework for Apache Spark.',
   author='Luca Gagliardelli',
   author_email='luca.gagliardelli@unimore.it',
   url='https://github.com/Gaglia88/sparker',
   packages=['sparker'],
   install_requires=['numpy', 'networkx'],
)