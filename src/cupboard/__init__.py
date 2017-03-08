import logging

# supress "No handlers could be found for logger XXXXX" error
logging.getLogger('cupboard').addHandler(logging.NullHandler())

from ._cache import Cupboard
from ._backend import MB, GB, TB

__version__ = '0.2.0'
