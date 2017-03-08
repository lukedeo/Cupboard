# Cupboard

A dictionary-like abstraction for Redis, LevelDB, and LMDB in Python.

`Cupboard` can be used as a drop-in replacement for a dictionary for most cases. With `Cupboard`, you can develop with a dictionary, and deploy with reliably backed NoSQL storage system such as Redis, LevelDB, or LMDB. 

## Installation

You will need one (or more) of the Python [LevelDB](https://plyvel.readthedocs.io), [LMDB](https://lmdb.readthedocs.io/en/release/), or [Redis](https://redis-py.readthedocs.io/en/latest/) clients.

Cupboard is installable via `pip`, with `pip install cupboard`.