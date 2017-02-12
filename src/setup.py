from setuptools import setup
from setuptools import find_packages

setup(
    name='Cupboard',
    version='0.1.0',
    description='Abstracted interface for a variety of key value storage systems.',
    author='Luke de Oliveira',
    author_email='lukedeo@vaitech.io',
    url='https://github.com/vaitech/cupboard',
    install_requires=['numpy'],
    packages=find_packages()
)
