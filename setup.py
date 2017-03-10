from setuptools import setup, find_packages

# modules = {_.project_name for _ in __import__('pkg_resources').working_set}

# if not any(m in modules for m in ['redis', 'plyvel', 'lmdb']):
#     raise ImportError('need one of redis, plyvel, or lmdb to install Cupboard')

setup(
    name='Cupboard',
    version='0.2.0',
    description=('Abstracted interface for a many key value storage systems.'),
    author='Luke de Oliveira',
    author_email='lukedeo@ldo.io',
    url='https://github.com/vaitech/cupboard',
    license='Apache 2.0',
    install_requires=['future'],
    long_description=('Abstracted interface for a variety of key value storage '
                      'systems. Developers get tired of having to refactor '
                      'around different APIs for different storage systems. '
                      'Cupboard can serve as a drop-in replacement for a '
                      'dictionary under most usages and can be backed by a '
                      'variety of KV storage systems.'),
    packages=find_packages(),
    keywords=' '.join(('NoSQL', 'persistent', 'storage', 'key-value',
                       'store', 'redis', 'leveldb', 'lmdb', 'database')),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
    extras_require={
        'all': ['redis', 'lmdb', 'plyvel'],
        'tests': ['numpy', 'pytest', 'pytest-cov', 'pytest-pep8',
                  'pytest-xdist', 'python-coveralls', 'coverage==3.7.1']
    }
)
