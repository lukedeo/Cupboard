# Cupboard

[![Build Status](https://travis-ci.org/lukedeo/Cupboard.svg?branch=master)](https://travis-ci.org/lukedeo/Cupboard)
[![Coverage Status](https://coveralls.io/repos/github/lukedeo/Cupboard/badge.svg?branch=master)](https://coveralls.io/github/lukedeo/Cupboard?branch=master)

A dictionary-like abstraction for Redis, LevelDB, and LMDB in Python.

`Cupboard` can be used as a drop-in replacement for a dictionary for most cases. With `Cupboard`, you can develop with a dictionary, and deploy with reliably backed NoSQL storage system such as Redis, LevelDB, or LMDB. 

## Installation

You will need one (or more) of the Python [LevelDB](https://plyvel.readthedocs.io), [LMDB](https://lmdb.readthedocs.io/en/release/), or [Redis](https://redis-py.readthedocs.io/en/latest/) clients.

Cupboard is installable via `pip`, with `pip install Cupboard`.